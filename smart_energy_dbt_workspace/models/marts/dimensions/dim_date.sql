{{ config(materialized='table') }}

with bounds as (
    select
        min(to_date(reading_ts_utc)) as min_date,
        max(to_date(reading_ts_utc)) as max_date
    from {{ ref('stg_silver_meter_readings') }}
), offsets as (
    select seq4() as day_offset
    from table(generator(rowcount => 10000))
), dates as (
    select dateadd(day, offsets.day_offset, bounds.min_date)::date as full_date
    from bounds
    cross join offsets
    where bounds.min_date is not null
      and dateadd(day, offsets.day_offset, bounds.min_date)::date <= bounds.max_date
)
select
    to_number(to_char(full_date, 'YYYYMMDD'))::number(8,0) as date_key,
    full_date,
    day(full_date)::integer as day,
    month(full_date)::integer as month,
    monthname(full_date) as month_name,
    quarter(full_date)::integer as quarter,
    year(full_date)::integer as year,
    dayofweekiso(full_date)::integer as day_of_week,
    dayname(full_date) as day_name,
    (dayofweekiso(full_date) in (6, 7)) as is_weekend
from dates
