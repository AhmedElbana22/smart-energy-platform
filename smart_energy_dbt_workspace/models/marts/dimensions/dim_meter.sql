{{ config(materialized='table') }}

select
    to_number(substr(sha2(concat_ws('|',
        coalesce(meter_id, '<NULL>'),
        coalesce(source_system, '<NULL>'),
        coalesce(source_meter_id, '<NULL>')
    ), 256), 1, 15), 'XXXXXXXXXXXXXXX')::number(19,0) as meter_key,
    meter_id,
    source_system,
    source_meter_id
from {{ ref('stg_silver_meter_readings') }}
group by meter_id, source_system, source_meter_id
