-- Smart Energy Platform: Flink 1.20 SQL streaming detectors.
-- Input is one meter reading per meter_id + reading_ts_utc, with 15-minute cadence.
-- This development replay uses a zero-delay watermark because producer.py sends
-- complete timestamp groups in event-time order. For live production data, allow
-- a bounded lateness interval appropriate to the meter/network latency.
--
SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';

-- Ground-truth anomaly_label/anomaly_family are intentionally excluded. The source
-- schema keeps optional enrichment fields, but no detector depends on value_status.

CREATE TABLE meter_readings (
    meter_id STRING,
    source_system STRING,
    source_meter_id STRING,
    reading_ts_utc TIMESTAMP_LTZ(3),
    interval_minutes INT,
    active_energy_kwh_import DOUBLE,
    active_power_kw DOUBLE,
    reactive_energy_kvarh_import DOUBLE,
    reactive_power_kvar DOUBLE,
    voltage_v_baseline DOUBLE,
    voltage_v DOUBLE,
    apparent_power_kva DOUBLE,
    transformer_id STRING,
    feeder_id STRING,
    substation_id STRING,
    transformer_rating_kva DOUBLE,
    loading_ratio DOUBLE,
    value_status STRING,
    quality_status STRING,
    WATERMARK FOR reading_ts_utc AS reading_ts_utc
) WITH (
    'connector' = 'kafka',
    'topic' = 'meter-readings',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'smart-energy-flink-alerts-v2',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- Preserve the original alert fields used by the initial project contract and
-- append severity/diagnostic fields that support investigations and dashboards.
CREATE TABLE grid_alerts (
    alert_id STRING,
    meter_id STRING,
    reading_ts_utc TIMESTAMP_LTZ(3),
    transformer_id STRING,
    alert_type STRING,
    voltage_v DOUBLE,
    voltage_v_baseline DOUBLE,
    loading_ratio DOUBLE,
    message STRING,
    severity STRING,
    feeder_id STRING,
    source_system STRING,
    observed_value DOUBLE,
    details STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'grid-alerts',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- One INSERT / UNION ALL means one submitted Flink job consumes the source and
-- emits each category of alert. This avoids submitting four competing source jobs.
INSERT INTO grid_alerts

-- A1. Transformer loading. Prefer recomputing load from apparent power / capacity
-- when every reading in the transformer window has both fields and the capacity is
-- consistent. Otherwise use the existing loading_ratio field from the source contract.
-- Emit at >100%; severity HIGH at >=110%.
SELECT
    CONCAT('transformer_load_', transformer_id, '_', CAST(window_end AS STRING)) AS alert_id,
    CAST(NULL AS STRING) AS meter_id,
    window_end AS reading_ts_utc,
    transformer_id,
    'HIGH_TRANSFORMER_LOADING' AS alert_type,
    CAST(NULL AS DOUBLE) AS voltage_v,
    CAST(NULL AS DOUBLE) AS voltage_v_baseline,
    load_ratio AS loading_ratio,
    CONCAT('Transformer loading reached ', CAST(load_ratio * 100.0 AS STRING), '% of rating') AS message,
    CASE WHEN load_ratio >= 1.10 THEN 'HIGH' ELSE 'MEDIUM' END AS severity,
    CASE WHEN min_feeder_id = max_feeder_id THEN max_feeder_id ELSE CAST(NULL AS STRING) END AS feeder_id,
    'flink_transformer_window' AS source_system,
    load_ratio AS observed_value,
    CONCAT(
        'rule=transformer_capacity_15m; calculation=', calculation_method,
        '; total_apparent_power_kva=', COALESCE(CAST(apparent_load_kva AS STRING), 'unavailable'),
        '; rating_kva=', COALESCE(CAST(rating_kva AS STRING), 'unavailable'),
        '; loading_ratio=', CAST(load_ratio AS STRING),
        '; window_start=', CAST(window_start AS STRING),
        '; window_end=', CAST(window_end AS STRING)
    ) AS details
FROM (
    SELECT
        window_start,
        window_end,
        transformer_id,
        SUM(apparent_power_kva) AS apparent_load_kva,
        MAX(transformer_rating_kva) AS rating_kva,
        CASE
            WHEN COUNT(apparent_power_kva) = COUNT(*)
             AND COUNT(transformer_rating_kva) = COUNT(*)
             AND MIN(transformer_rating_kva) = MAX(transformer_rating_kva)
            THEN SUM(apparent_power_kva) / MAX(transformer_rating_kva)
            ELSE MAX(loading_ratio)
        END AS load_ratio,
        CASE
            WHEN COUNT(apparent_power_kva) = COUNT(*)
             AND COUNT(transformer_rating_kva) = COUNT(*)
             AND MIN(transformer_rating_kva) = MAX(transformer_rating_kva)
            THEN 'apparent_power_over_rating'
            ELSE 'source_loading_ratio_fallback'
        END AS calculation_method,
        MIN(feeder_id) AS min_feeder_id,
        MAX(feeder_id) AS max_feeder_id
    FROM TABLE(
        TUMBLE(
            TABLE meter_readings,
            DESCRIPTOR(reading_ts_utc),
            INTERVAL '15' MINUTES
        )
    )
    WHERE transformer_id IS NOT NULL
      AND meter_id IS NOT NULL
      AND (
            loading_ratio IS NOT NULL
            OR (apparent_power_kva IS NOT NULL AND transformer_rating_kva > 0)
          )
      AND LOWER(TRIM(COALESCE(quality_status, ''))) IN ('good', 'valid', 'ok')
    GROUP BY window_start, window_end, transformer_id
) AS transformer_windows
WHERE load_ratio > 1.00

UNION ALL

-- A2. Persistent voltage drop: >=15 V below baseline in at least 3 readings
-- for the same meter within a 45-minute hopping window (hop size 15 minutes).
SELECT
    CONCAT('persistent_voltage_drop_', meter_id, '_', CAST(window_end AS STRING)) AS alert_id,
    meter_id,
    window_end AS reading_ts_utc,
    CASE WHEN min_transformer_id = max_transformer_id THEN max_transformer_id ELSE CAST(NULL AS STRING) END AS transformer_id,
    'PERSISTENT_VOLTAGE_DROP' AS alert_type,
    min_voltage_v AS voltage_v,
    max_baseline_v AS voltage_v_baseline,
    CAST(NULL AS DOUBLE) AS loading_ratio,
    CONCAT('Persistent voltage drop: ', CAST(max_drop_v AS STRING), ' V maximum drop across qualifying readings') AS message,
    CASE WHEN max_drop_v >= 18.0 THEN 'HIGH' ELSE 'MEDIUM' END AS severity,
    CASE WHEN min_feeder_id = max_feeder_id THEN max_feeder_id ELSE CAST(NULL AS STRING) END AS feeder_id,
    source_system,
    max_drop_v AS observed_value,
    CONCAT(
        'rule=voltage_drop_45m; threshold_v=15.0; qualifying_readings=', CAST(qualifying_readings AS STRING),
        '; max_drop_v=', CAST(max_drop_v AS STRING),
        '; window_start=', CAST(window_start AS STRING),
        '; window_end=', CAST(window_end AS STRING)
    ) AS details
FROM (
    SELECT
        window_start,
        window_end,
        meter_id,
        MAX(source_system) AS source_system,
        MIN(transformer_id) AS min_transformer_id,
        MAX(transformer_id) AS max_transformer_id,
        MIN(feeder_id) AS min_feeder_id,
        MAX(feeder_id) AS max_feeder_id,
        MIN(voltage_v) AS min_voltage_v,
        MAX(voltage_v_baseline) AS max_baseline_v,
        MAX(voltage_v_baseline - voltage_v) AS max_drop_v,
        SUM(CASE WHEN voltage_v_baseline - voltage_v >= 15.0 THEN 1 ELSE 0 END) AS qualifying_readings
    FROM TABLE(
        HOP(
            TABLE meter_readings,
            DESCRIPTOR(reading_ts_utc),
            INTERVAL '15' MINUTES,
            INTERVAL '45' MINUTES
        )
    )
    WHERE meter_id IS NOT NULL
      AND voltage_v IS NOT NULL
      AND voltage_v_baseline IS NOT NULL
      AND voltage_v_baseline > 0
      AND LOWER(TRIM(COALESCE(quality_status, ''))) IN ('good', 'valid', 'ok')
    GROUP BY window_start, window_end, meter_id
    HAVING SUM(CASE WHEN voltage_v_baseline - voltage_v >= 15.0 THEN 1 ELSE 0 END) >= 3
) AS voltage_windows

UNION ALL

-- A3. Suspicious consumption: current active_power_kw > prior 24-reading mean
-- + 3 population standard deviations. Current reading is excluded from baseline;
-- require at least 12 valid prior readings. This is a suspicious pattern, not proof
-- of theft. A cold-start replay needs at least 13 timestamp groups per meter.
SELECT
    CONCAT('consumption_spike_', meter_id, '_', CAST(reading_ts_utc AS STRING)) AS alert_id,
    meter_id,
    reading_ts_utc,
    transformer_id,
    'SUSPICIOUS_CONSUMPTION_SPIKE' AS alert_type,
    voltage_v,
    voltage_v_baseline,
    loading_ratio,
    CONCAT('Suspicious consumption deviation: current active power=', CAST(active_power_kw AS STRING), ' kW') AS message,
    'MEDIUM' AS severity,
    feeder_id,
    source_system,
    active_power_kw AS observed_value,
    CONCAT(
        'rule=meter_rolling_baseline_24; current_kw=', CAST(active_power_kw AS STRING),
        '; prior_mean_kw=', CAST(prior_mean_kw AS STRING),
        '; prior_stddev_kw=', CAST(prior_stddev_kw AS STRING),
        '; prior_valid_readings=', CAST(prior_count AS STRING)
    ) AS details
FROM (
    SELECT
        meter_id,
        source_system,
        transformer_id,
        feeder_id,
        reading_ts_utc,
        active_power_kw,
        voltage_v,
        voltage_v_baseline,
        loading_ratio,
        prior_count,
        prior_sum_kw / CAST(prior_count AS DOUBLE) AS prior_mean_kw,
        SQRT(
            CASE
                WHEN prior_sum_sq_kw / CAST(prior_count AS DOUBLE)
                     - POWER(prior_sum_kw / CAST(prior_count AS DOUBLE), 2) < 0.0
                THEN 0.0
                ELSE prior_sum_sq_kw / CAST(prior_count AS DOUBLE)
                     - POWER(prior_sum_kw / CAST(prior_count AS DOUBLE), 2)
            END
        ) AS prior_stddev_kw
    FROM (
        SELECT
            meter_id,
            source_system,
            transformer_id,
            feeder_id,
            reading_ts_utc,
            active_power_kw,
            voltage_v,
            voltage_v_baseline,
            loading_ratio,
            window_count - 1 AS prior_count,
            COALESCE(window_sum_kw, 0.0) - COALESCE(active_power_kw, 0.0) AS prior_sum_kw,
            COALESCE(window_sum_sq_kw, 0.0) - COALESCE(active_power_kw * active_power_kw, 0.0) AS prior_sum_sq_kw
        FROM (
            SELECT
                meter_id,
                source_system,
                transformer_id,
                feeder_id,
                reading_ts_utc,
                active_power_kw,
                voltage_v,
                voltage_v_baseline,
                loading_ratio,
                COUNT(active_power_kw) OVER meter_history AS window_count,
                SUM(active_power_kw) OVER meter_history AS window_sum_kw,
                SUM(active_power_kw * active_power_kw) OVER meter_history AS window_sum_sq_kw
            FROM meter_readings
            WHERE meter_id IS NOT NULL
              AND active_power_kw IS NOT NULL
              AND LOWER(TRIM(COALESCE(quality_status, ''))) IN ('good', 'valid', 'ok')
                    WINDOW meter_history AS (
                PARTITION BY meter_id
                ORDER BY reading_ts_utc
                ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
            )
        ) AS meter_window_stats
    ) AS prior_stats
) AS consumption_baseline
WHERE active_power_kw IS NOT NULL
  AND prior_count >= 12
  AND prior_mean_kw > 0
  AND active_power_kw > prior_mean_kw + 3.0 * prior_stddev_kw

UNION ALL

-- DQ. Quality checks are separate from anomaly detection. Optional metadata such as
-- feeder_id, value_status, source_meter_id and substation_id is not treated as required.
-- Flag missing fields needed by the rules, invalid physical ranges, and unrecognized quality status.
SELECT
    CONCAT(
        'data_quality_', COALESCE(meter_id, 'unknown_meter'), '_',
        COALESCE(CAST(reading_ts_utc AS STRING), 'missing_time')
    ) AS alert_id,
    meter_id,
    reading_ts_utc,
    transformer_id,
    'POOR_DATA_QUALITY' AS alert_type,
    voltage_v,
    voltage_v_baseline,
    loading_ratio,
    'Meter reading failed one or more data-quality checks' AS message,
    'LOW' AS severity,
    feeder_id,
    source_system,
    CAST(NULL AS DOUBLE) AS observed_value,
    CONCAT(
        'quality_status=', COALESCE(quality_status, 'missing'),
        '; value_status=', COALESCE(value_status, 'missing'),
        '; interval_minutes=', COALESCE(CAST(interval_minutes AS STRING), 'missing'),
        '; active_power_kw=', COALESCE(CAST(active_power_kw AS STRING), 'missing'),
        '; apparent_power_kva=', COALESCE(CAST(apparent_power_kva AS STRING), 'missing'),
        '; loading_ratio=', COALESCE(CAST(loading_ratio AS STRING), 'missing'),
        '; voltage_v=', COALESCE(CAST(voltage_v AS STRING), 'missing'),
        '; voltage_v_baseline=', COALESCE(CAST(voltage_v_baseline AS STRING), 'missing'),
        '; transformer_rating_kva=', COALESCE(CAST(transformer_rating_kva AS STRING), 'missing')
    ) AS details
FROM meter_readings
WHERE meter_id IS NULL
   OR reading_ts_utc IS NULL
   OR active_power_kw IS NULL
   OR active_power_kw < 0
   OR (interval_minutes IS NOT NULL AND interval_minutes <> 15)
   OR (apparent_power_kva IS NOT NULL AND apparent_power_kva < 0)
   OR (loading_ratio IS NULL AND (apparent_power_kva IS NULL OR transformer_rating_kva IS NULL OR transformer_rating_kva <= 0))
   OR (loading_ratio IS NOT NULL AND loading_ratio < 0)
   OR transformer_id IS NULL
   OR voltage_v IS NULL
   OR voltage_v < 0
   OR voltage_v > 300
   OR voltage_v_baseline IS NULL
   OR voltage_v_baseline <= 0
   OR voltage_v_baseline > 300
   OR quality_status IS NULL
   OR LOWER(TRIM(quality_status)) NOT IN ('good', 'valid', 'ok');
