-- depends_on: {{ ref('fact_meter_readings') }}
{{
  config(
    materialized='incremental',
    unique_key='transformer_daily_key',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

with affected_dates as (
    {% if is_incremental() %}
    select distinct date_key
    from {{ ref('fact_meter_readings') }}
    where reading_ts_utc >= dateadd(
        day,
        -{{ var('incremental_lookback_days', 3) }},
        (select coalesce(max(reading_ts_utc), '1900-01-01'::timestamp_ntz)
         from {{ ref('fact_meter_readings') }})
    )
    {% else %}
    select distinct date_key
    from {{ ref('int_transformer_daily_aggregates') }}
    {% endif %}
)
select
    to_number(
        substr(
            sha2(
                concat_ws(
                    '|',
                    to_varchar(d.date_key),
                    to_varchar(d.grid_asset_key)
                ),
                256
            ),
            1,
            15
        ),
        'XXXXXXXXXXXXXXX'
    )::number(19,0) as transformer_daily_key,
    d.date_key,
    d.grid_asset_key,
    d.reading_count,
    d.total_active_energy_kwh,
    d.avg_voltage_v,
    d.min_voltage_v,
    d.max_voltage_v,
    d.avg_active_power_kw,
    d.max_active_power_kw,
    d.max_apparent_power_kva,
    d.avg_loading_ratio,
    d.max_loading_ratio,
    d.anomaly_count,
    current_timestamp()::timestamp_ntz as calculated_at_utc
from {{ ref('int_transformer_daily_aggregates') }} d
inner join affected_dates a on d.date_key = a.date_key
