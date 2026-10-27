{{
  config(
    materialized='incremental',
    unique_key='reading_fact_key',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

with readings as (
    select *
    from {{ ref('int_meter_readings_enriched') }}
    {% if is_incremental() %}
    where reading_ts_utc >= dateadd(
        day,
        -{{ var('incremental_lookback_days', 3) }},
        (select coalesce(max(reading_ts_utc), '1900-01-01'::timestamp_ntz) from {{ this }})
    )
    {% endif %}
)
select
    r.reading_fact_key,
    r.date_key,
    r.time_key,
    m.meter_key,
    g.grid_asset_key,
    r.reading_ts_utc,
    r.interval_minutes,
    r.active_energy_kwh_import,
    r.active_power_kw,
    r.reactive_energy_kvarh_import,
    r.reactive_power_kvar,
    r.voltage_v_baseline,
    r.voltage_v,
    r.apparent_power_kva,
    r.loading_ratio,
    r.anomaly_label,
    r.anomaly_family,
    r.value_status,
    r.quality_status,
    r.source_system,
    r.source_meter_id,
    r.transformer_id,
    r.feeder_id,
    r.substation_id,
    r.transformer_rating_kva,
    r.generator_seed
from readings r
left join {{ ref('dim_meter') }} m
    on r.meter_key = m.meter_key
left join {{ ref('dim_grid_asset') }} g
    on r.grid_asset_key = g.grid_asset_key
