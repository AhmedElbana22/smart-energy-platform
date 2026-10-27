-- This test returns duplicate natural keys. It should return zero rows.
select
    meter_id,
    reading_ts_utc,
    source_system,
    source_meter_id,
    count(*) as duplicate_count
from {{ ref('stg_silver_meter_readings') }}
group by 1, 2, 3, 4
having count(*) > 1
