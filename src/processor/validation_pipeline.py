import json
from datetime import datetime, timezone, timedelta
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSource
from bytewax.connectors.stdio import StdOutSink
from pydantic import ValidationError
import clickhouse_connect
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from src.processor.models.telemetry_schema import IotTelemetryRecord

# --- CẤU HÌNH ---
REDPANDA_BROKERS = ["localhost:9092"]
SOURCE_TOPIC = "iot_raw_data"

# --- CLICKHOUSE SINK ---
class ClickhouseSinkPartition(StatelessSinkPartition):
    def __init__(self, table_name):
        self.client = clickhouse_connect.get_client(
            host='localhost', 
            port=8123, 
            username='iot_admin', 
            password='iot_password',
            database='iot_telemetry'
        )
        self.table_name = table_name

    def write_batch(self, items):
        if not items:
            return
            
        flat_items = []
        for batch in items:
            flat_items.extend(batch)

        if not flat_items:
            return

        column_names = list(flat_items[0].keys())
        data_rows = [[item.get(col) for col in column_names] for item in flat_items]
        
        self.client.insert(self.table_name, data_rows, column_names=column_names)

class ClickhouseSink(DynamicSink):
    def __init__(self, table_name):
        self.table_name = table_name

    def build(self, step_id, worker_index, worker_count):
        return ClickhouseSinkPartition(self.table_name)

# --- XỬ LÝ LOGIC ---
def parse_raw_message(kafka_message) -> dict:
    raw_bytes = kafka_message.value
    return json.loads(raw_bytes.decode('utf-8'))

def validate_schema_and_logic(raw_payload: dict) -> tuple:
    try:
        valid_record = IotTelemetryRecord(**raw_payload)
        return (True, valid_record.model_dump())
    except ValidationError as validation_error:
        error_details = generate_error_tag(raw_payload, str(validation_error))
        return (False, error_details)

def generate_error_tag(failed_payload: dict, error_message: str) -> dict:
    return {
        "device_id": failed_payload.get("device_id", "UNKNOWN"),
        "raw_payload_content": json.dumps(failed_payload),
        "error_reason_description": error_message,
        "event_timestamp": datetime.now(timezone.utc)
    }

# BƯỚC MỚI: Phiên dịch tên cột cho ClickHouse
def format_clean_record_for_db(payload: dict) -> dict:
    return {
        "device_id": payload.get("device_id"),
        "event_timestamp": payload.get("timestamp"),         
        "temperature_value": payload.get("temperature"),     
        "humidity_value": payload.get("humidity"),           
        "is_valid_record": 1                                 
    }

def add_key(data: dict) -> tuple:
    return ("fixed_key", data)

def remove_key(keyed_batch: tuple) -> list:
    return keyed_batch[1]

# --- PHÂN LUỒNG ---
def is_clean_record(validation_result: tuple) -> bool:
    return validation_result[0]

def is_error_record(validation_result: tuple) -> bool:
    return not validation_result[0]

def extract_payload(validation_result: tuple) -> dict:
    return validation_result[1]

# --- KHỞI TẠO LUỒNG ---
pipeline = Dataflow("iot_quality_pipeline")

stream = op.input("read_from_broker", pipeline, KafkaSource(REDPANDA_BROKERS, [SOURCE_TOPIC]))

parsed_stream = op.map("parse_json", stream, parse_raw_message)
validated_stream = op.map("apply_pydantic_rules", parsed_stream, validate_schema_and_logic)

# Xử lý nhánh Sạch (Clean)
clean_branch = op.filter("filter_clean", validated_stream, is_clean_record)
clean_data_raw = op.map("extract_clean_payload", clean_branch, extract_payload)

clean_data_mapped = op.map("format_clean_data", clean_data_raw, format_clean_record_for_db)

# Xử lý nhánh Lỗi (Error)
error_branch = op.filter("filter_error", validated_stream, is_error_record)
error_data_mapped = op.map("extract_error_payload", error_branch, extract_payload)

keyed_clean = op.map("key_clean", clean_data_mapped, add_key)
keyed_error = op.map("key_error", error_data_mapped, add_key)

# Gom lô (Batching)
batched_clean_keyed = op.collect("batch_clean", keyed_clean, timeout=timedelta(seconds=5), max_size=10)
batched_error_keyed = op.collect("batch_error", keyed_error, timeout=timedelta(seconds=5), max_size=10)

final_clean_batch = op.map("remove_key_clean", batched_clean_keyed, remove_key)
final_error_batch = op.map("remove_key_error", batched_error_keyed, remove_key)

# Ghi vào ClickHouse
op.output("write_to_ch_clean", final_clean_batch, ClickhouseSink("iot_telemetry_clean"))
op.output("write_to_ch_error", final_error_batch, ClickhouseSink("iot_telemetry_error"))

if __name__ == "__main__":
    from bytewax.testing import run_main
    print(f"--- [HỆ THỐNG] Processor đang chạy... ---")
    try:
        run_main(pipeline)
    except KeyboardInterrupt:
        print("--- [HỆ THỐNG] Dừng Processor ---")