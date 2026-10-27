{{ config(materialized='view') }}

select
    nullif(trim(METER_ID), '') as meter_id,
    cast(READING_TS_UTC as timestamp_ntz) as reading_ts_utc,
    lower(nullif(trim(SOURCE_SYSTEM), '')) as source_system,
    nullif(trim(SOURCE_METER_ID), '') as source_meter_id,
    cast(INTERVAL_MINUTES as integer) as interval_minutes,
    cast(ACTIVE_ENERGY_KWH_IMPORT as float) as active_energy_kwh_import,
    cast(ACTIVE_POWER_KW as float) as active_power_kw,
    cast(REACTIVE_ENERGY_KVARH_IMPORT as float) as reactive_energy_kvarh_import,
    cast(REACTIVE_POWER_KVAR as float) as reactive_power_kvar,
    cast(VOLTAGE_V_BASELINE as float) as voltage_v_baseline,
    cast(VOLTAGE_V as float) as voltage_v,
    cast(APPARENT_POWER_KVA as float) as apparent_power_kva,
    nullif(trim(TRANSFORMER_ID), '') as transformer_id,
    nullif(trim(FEEDER_ID), '') as feeder_id,
    nullif(trim(SUBSTATION_ID), '') as substation_id,
    cast(TRANSFORMER_RATING_KVA as float) as transformer_rating_kva,
    cast(LOADING_RATIO as float) as loading_ratio,
    lower(nullif(trim(ANOMALY_FAMILY), '')) as anomaly_family,
    cast(ANOMALY_LABEL as boolean) as anomaly_label,
    lower(nullif(trim(VALUE_STATUS), '')) as value_status,
    lower(nullif(trim(QUALITY_STATUS), '')) as quality_status,
    cast(GENERATOR_SEED as number(38, 0)) as generator_seed
from {{ source('energy_landing', 'silver_meter_readings') }}
