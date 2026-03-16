SELECT
    device_id,
    event_timestamp,
    temperature_value
FROM {{ source('clickhouse_raw', 'iot_telemetry_clean') }}
WHERE temperature_value < -50 OR temperature_value > 100