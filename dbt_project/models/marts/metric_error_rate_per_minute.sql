{{ config(
    materialized='view'
) }}

WITH clean_events_aggregated AS (
    SELECT
        toStartOfMinute(event_timestamp) AS metric_minute,
        COUNT(device_id) AS total_clean_count,
        0 AS total_error_count
    FROM {{ source('clickhouse_raw', 'iot_telemetry_clean') }}
    GROUP BY metric_minute
),

error_events_aggregated AS (
    SELECT
        toStartOfMinute(event_timestamp) AS metric_minute,
        0 AS total_clean_count,
        COUNT(device_id) AS total_error_count
    FROM {{ source('clickhouse_raw', 'iot_telemetry_error') }}
    GROUP BY metric_minute
),

-- Gộp chung 2 tập dữ liệu
combined_metrics AS (
    SELECT * FROM clean_events_aggregated
    UNION ALL
    SELECT * FROM error_events_aggregated
)

SELECT
    metric_minute,
    SUM(total_clean_count) AS final_clean_count,
    SUM(total_error_count) AS final_error_count,
    (SUM(total_clean_count) + SUM(total_error_count)) AS total_events_processed,
    
    IF(
        total_events_processed > 0, 
        (SUM(total_error_count) / total_events_processed) * 100, 
        0
    ) AS error_rate_percentage

FROM combined_metrics
GROUP BY metric_minute
ORDER BY metric_minute DESC