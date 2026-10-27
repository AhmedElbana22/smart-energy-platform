-- Readings are expected to align to quarter-hour boundaries.
select meter_id, reading_ts_utc
from {{ ref('stg_silver_meter_readings') }}
where reading_ts_utc is not null
  and (
      date_part(minute, reading_ts_utc) not in (0, 15, 30, 45)
      or date_part(second, reading_ts_utc) <> 0
  )
