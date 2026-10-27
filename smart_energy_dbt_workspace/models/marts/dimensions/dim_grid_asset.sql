{{ config(materialized='table') }}

select
    grid_asset_key,
    transformer_id,
    feeder_id,
    substation_id,
    transformer_rating_kva
from {{ ref('int_grid_asset_mapping') }}