{{ config(materialized='view') }}

with readings as (

    select *
    from {{ ref('int_meter_readings_enriched') }}
    where transformer_id is not null
      and reading_ts_utc is not null

),

daily_reading_stats as (

    select
        date_key,
        full_date,
        grid_asset_key,
        transformer_id,

        count(*)::number(38,0) as reading_count,

        sum(active_energy_kwh_import)::float
            as total_active_energy_kwh,

        avg(voltage_v)::float as avg_voltage_v,
        min(voltage_v)::float as min_voltage_v,
        max(voltage_v)::float as max_voltage_v,

        sum(iff(anomaly_label = true, 1, 0))::number(38,0)
            as anomaly_count

    from readings
    group by
        date_key,
        full_date,
        grid_asset_key,
        transformer_id

),

transformer_intervals as (

    select
        date_key,
        full_date,
        grid_asset_key,
        transformer_id,
        reading_ts_utc,

        sum(active_power_kw)::float as active_load_kw,
        sum(apparent_power_kva)::float as apparent_load_kva,
        max(transformer_rating_kva)::float as transformer_rating_kva

    from readings
    group by
        date_key,
        full_date,
        grid_asset_key,
        transformer_id,
        reading_ts_utc

),

interval_stats as (

    select
        date_key,
        full_date,
        grid_asset_key,
        transformer_id,

        avg(active_load_kw)::float as avg_active_power_kw,
        max(active_load_kw)::float as max_active_power_kw,

        max(apparent_load_kva)::float
            as max_apparent_power_kva,

        avg(
            iff(
                transformer_rating_kva > 0,
                apparent_load_kva / transformer_rating_kva,
                null
            )
        )::float as avg_loading_ratio,

        max(
            iff(
                transformer_rating_kva > 0,
                apparent_load_kva / transformer_rating_kva,
                null
            )
        )::float as max_loading_ratio

    from transformer_intervals
    group by
        date_key,
        full_date,
        grid_asset_key,
        transformer_id

)

select
    d.date_key,
    d.full_date,
    d.grid_asset_key,
    d.transformer_id,

    d.reading_count,
    d.total_active_energy_kwh,

    i.avg_active_power_kw,
    i.max_active_power_kw,

    d.avg_voltage_v,
    d.min_voltage_v,
    d.max_voltage_v,

    i.max_apparent_power_kva,
    i.avg_loading_ratio,
    i.max_loading_ratio,

    d.anomaly_count

from daily_reading_stats d

left join interval_stats i
    on d.date_key = i.date_key
   and d.grid_asset_key = i.grid_asset_key
   and d.transformer_id = i.transformer_id