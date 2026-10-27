{{
  config(
    materialized='incremental',
    unique_key='anomaly_event_key',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

select
    anomaly_event_key,
    reading_fact_key,
    date_key,
    time_key,
    meter_key,
    grid_asset_key,
    anomaly_family,
    anomaly_label,
    event_ts_utc,
    source_system
from {{ ref('int_anomaly_events') }}

{% if is_incremental() %}
where event_ts_utc >= dateadd(
    day,
    -{{ var('incremental_lookback_days', 3) }},
    (
        select coalesce(
            max(event_ts_utc),
            '1900-01-01'::timestamp_ntz
        )
        from {{ this }}
    )
)
{% endif %}