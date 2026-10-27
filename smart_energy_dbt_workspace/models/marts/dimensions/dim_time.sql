{{ config(materialized='table') }}

with slots as (
    select seq4()::integer as time_key
    from table(generator(rowcount => 96))
), parts as (
    select
        time_key,
        floor(time_key / 4)::integer as hour,
        (mod(time_key, 4) * 15)::integer as minute
    from slots
)
select
    time_key,
    hour,
    minute,
    lpad(to_varchar(hour), 2, '0') || ':' || lpad(to_varchar(minute), 2, '0') as time_label,
    time_key as quarter_hour_number
from parts
