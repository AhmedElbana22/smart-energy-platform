CREATE TABLE meter_readings (
    meter_id STRING,
    reading_ts_utc STRING,
    voltage_v_baseline DOUBLE,
    voltage_v DOUBLE,
    transformer_id STRING,
    loading_ratio DOUBLE,
    anomaly_family STRING,
    anomaly_label INT,
    quality_status STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'meter-readings',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-grid-alerts',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE grid_alerts (
    alert_id STRING,
    meter_id STRING,
    reading_ts_utc STRING,
    transformer_id STRING,
    alert_type STRING,
    voltage_v DOUBLE,
    voltage_v_baseline DOUBLE,
    loading_ratio DOUBLE,
    message STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'grid-alerts',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

INSERT INTO grid_alerts
SELECT
    CONCAT('low_voltage_', meter_id, '_', reading_ts_utc),
    meter_id,
    reading_ts_utc,
    transformer_id,
    'LOW_VOLTAGE',
    voltage_v,
    voltage_v_baseline,
    loading_ratio,
    CONCAT(
        'Voltage below 95% of baseline: ',
        CAST(voltage_v AS STRING),
        ' V; baseline ',
        CAST(voltage_v_baseline AS STRING),
        ' V'
    )
FROM meter_readings
WHERE voltage_v < voltage_v_baseline * 0.95

UNION ALL

SELECT
    CONCAT('high_loading_', meter_id, '_', reading_ts_utc),
    meter_id,
    reading_ts_utc,
    transformer_id,
    'HIGH_TRANSFORMER_LOADING',
    voltage_v,
    voltage_v_baseline,
    loading_ratio,
    CONCAT(
        'Transformer loading ratio reached ',
        CAST(loading_ratio AS STRING)
    )
FROM meter_readings
WHERE loading_ratio >= 0.80;
