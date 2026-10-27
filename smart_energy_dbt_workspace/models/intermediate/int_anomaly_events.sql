{{ config(materialized='view') }}

select
    to_number(
        substr(
            sha2(
                concat_ws(
                    '|',
                    'GROUND_TRUTH_ANOMALY',
                    to_varchar(reading_fact_key)
                ),
                256
            ),
            1,
            15
        ),
        'XXXXXXXXXXXXXXX'
    )::number(19,0) as anomaly_event_key,

    reading_fact_key,
    date_key,
    time_key,
    meter_key,
    grid_asset_key,

    anomaly_family,
    anomaly_label,

    reading_ts_utc as event_ts_utc,
    source_system

from {{ ref('int_meter_readings_enriched') }}

where anomaly_label = true