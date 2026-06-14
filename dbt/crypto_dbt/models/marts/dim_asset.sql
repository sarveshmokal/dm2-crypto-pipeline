-- Gold DIMENSION table: descriptive attributes per asset.
-- fct_market_window.asset_id joins to dim_asset.asset_id (the star-schema join).
-- Starts simple; will be enriched with DefiLlama/CoinGecko batch data (market cap,
-- supply) as a slowly-changing dimension later.
{{ config(materialized='table') }}

with assets as (
    select distinct asset_id from {{ ref('fct_market_window') }}
)
select
    asset_id,                                   -- PK
    asset_id                       as symbol,
    case asset_id
        when 'BTCUSDT' then 'Bitcoin'
        when 'ETHUSDT' then 'Ethereum'
        when 'SOLUSDT' then 'Solana'
        else 'Unknown'
    end                            as asset_name,
    case asset_id
        when 'BTCUSDT' then 'large-cap'
        when 'ETHUSDT' then 'large-cap'
        when 'SOLUSDT' then 'mid-cap'
        else 'unknown'
    end                            as market_cap_tier,
    current_timestamp()            as loaded_at
from assets
