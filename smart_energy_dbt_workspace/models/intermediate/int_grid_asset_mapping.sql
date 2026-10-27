{{ config(materialized='view') }}

select
    grid_asset_key,
    transformer_id,
    feeder_id,
    substation_id,
    max(transformer_rating_kva)::float as transformer_rating_kva
from {{ ref('int_meter_readings_enriched') }}
group by
    grid_asset_key,
    transformer_id,
    feeder_id,
    substation_id