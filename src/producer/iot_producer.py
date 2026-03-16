import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

REDPANDA_BROKER_ADDRESS = 'localhost:9092'
TARGET_TOPIC_NAME = 'iot_raw_data'
DEVICE_ID_POOL = [f"SENSOR_AQUA_{i:03d}" for i in range(1, 6)]

def generate_clean_telemetry(device_id: str) -> dict:
    return {
        "device_id": device_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(40.0, 80.0), 2)
    }

def inject_data_errors(clean_record: dict) -> dict:
    error_type = random.choice(["missing_field", "invalid_type", "logical_error"])
    corrputed_record = clean_record.copy()

    print(f"🚀 Bắt đầu giả lập thiết bị IoT. Gửi dữ liệu tới topic: {TARGET_TOPIC_NAME}...")

    if error_type == "missing_field":
        corrputed_record.pop("device_id", None)
    elif error_type == "invalid_type":
        corrputed_record["temperature"] = "SENSOR_FAULT"
    elif error_type == "logical_error":
        corrputed_record["temperature"] = 500.00

    return corrputed_record

def delivery_callback(error_context, message_context):
    if error_context is not None:
        print(f"[THẤT BẠI] Không thể gửi tin nhắn: {error_context}")

def start_iot_simulation() -> None:
    producer_configuration = {'bootstrap.servers': REDPANDA_BROKER_ADDRESS}
    kafka_producer = Producer(producer_configuration)

    print(f"🚀 Bắt đầu giả lập thiết bị IoT. Gửi dữ liệu tới topic: {TARGET_TOPIC_NAME}...")

    try:
        while True:
            current_device_id = random.choice(DEVICE_ID_POOL)
            telemetry_data = generate_clean_telemetry(current_device_id)

            should_inject_error = random.random() < 0.15

            if should_inject_error:
                telemetry_data = inject_data_errors(telemetry_data)
                print(f"[CẢNH BÁO] Đã tạo dữ liệu LỖI: {telemetry_data}")
            else:
                print(f"[THÀNH CÔNG] Đã tạo dữ liệu SẠCH: {telemetry_data}")

            encoded_payload = json.dumps(telemetry_data).encode('utf-8')
            kafka_producer.produce(
                topic=TARGET_TOPIC_NAME,
                value=encoded_payload,
                callback=delivery_callback
            )

            kafka_producer.poll(0)

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n🛑 Nhận lệnh dừng từ người dùng. Đang đóng kết nối...")
    finally:
        kafka_producer.flush()

if __name__ == "__main__":
    start_iot_simulation()