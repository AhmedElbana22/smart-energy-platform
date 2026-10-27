{{ config(materialized='view') }}

with source_rows as (
    select * from {{ ref('stg_silver_meter_readings') }}
), keyed as (
    select
        source_rows.*,
        to_date(reading_ts_utc) as full_date,
        to_number(to_char(to_date(reading_ts_utc), 'YYYYMMDD'))::number(8,0) as date_key,
        (
            date_part(hour, reading_ts_utc) * 4
            + floor(date_part(minute, reading_ts_utc) / 15)
        )::integer as time_key,
        to_number(substr(sha2(concat_ws('|',
            coalesce(meter_id, '<NULL>'),
            coalesce(source_system, '<NULL>'),
            coalesce(source_meter_id, '<NULL>'),
            coalesce(to_varchar(reading_ts_utc, 'YYYY-MM-DD HH24:MI:SS.FF9'), '<NULL>')
        ), 256), 1, 15), 'XXXXXXXXXXXXXXX')::number(19,0) as reading_fact_key,
        to_number(substr(sha2(concat_ws('|',
            coalesce(meter_id, '<NULL>'),
            coalesce(source_system, '<NULL>'),
            coalesce(source_meter_id, '<NULL>')
        ), 256), 1, 15), 'XXXXXXXXXXXXXXX')::number(19,0) as meter_key,
        to_number(substr(sha2(concat_ws('|',
            coalesce(transformer_id, '<NULL>'),
            coalesce(feeder_id, '<NULL>'),
            coalesce(substation_id, '<NULL>')
        ), 256), 1, 15), 'XXXXXXXXXXXXXXX')::number(19,0) as grid_asset_key
    from source_rows
)
select * from keyed
