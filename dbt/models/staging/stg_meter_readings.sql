{{ config(materialized='view') }}

-- Starter model based only on the current real fields: id, timestamp, value_kwh.
-- Confirm whether id is a meter id or a source-row id before naming it meter_id.
select
    id as source_id,
    try_to_timestamp_ntz(timestamp) as event_timestamp,
    try_to_decimal(value_kwh, 18, 6) as value_kwh
from {{ source('raw', 'meter_readings') }}
