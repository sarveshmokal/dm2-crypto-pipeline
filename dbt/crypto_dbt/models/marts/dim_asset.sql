-- gold dimension: descriptive attributes per asset
-- now fed by the defillama batch source (bronze.asset_metadata) instead of hardcoded values
{{ config(materialized='table') }}

with metadata as (
    -- defillama gives symbol + reference price; take the latest row per symbol
    select
        symbol,
        price as reference_price,
        confidence,
        timestamp as price_timestamp
    from `dm2-crypto-microstructure.bronze.asset_metadata`
    qualify row_number() over (partition by symbol order by timestamp desc) = 1
),

-- the assets actually traded in our fact table (binance uses e.g. BTCUSDT)
assets as (
    select distinct asset_id from {{ ref('fct_market_window') }}
)

select
    a.asset_id,                                      -- PK, e.g. BTCUSDT
    -- strip the USDT quote to get the base symbol (BTCUSDT -> BTC) to join metadata
    replace(a.asset_id, 'USDT', '')      as base_symbol,
    case replace(a.asset_id, 'USDT', '')
        when 'BTC' then 'Bitcoin'
        when 'ETH' then 'Ethereum'
        when 'SOL' then 'Solana'
        when 'BNB' then 'BNB'
        when 'XRP' then 'XRP'
        when 'ADA' then 'Cardano'
        when 'DOGE' then 'Dogecoin'
        when 'AVAX' then 'Avalanche'
        else 'Unknown'
    end                                  as asset_name,
    -- market-cap tier classification (by well-known relative size of these 8 assets).
    -- lets us analyse whether realized volatility differs by tier.
    case replace(a.asset_id, 'USDT', '')
        when 'BTC' then 'Large Cap'
        when 'ETH' then 'Large Cap'
        when 'BNB' then 'Large Cap'
        when 'SOL' then 'Mid Cap'
        when 'XRP' then 'Mid Cap'
        when 'DOGE' then 'Mid Cap'
        when 'ADA' then 'Mid Cap'
        when 'AVAX' then 'Small Cap'
        else 'Unknown'
    end                                  as market_cap_tier,
    m.reference_price,                               -- from defillama
    m.confidence                         as price_confidence,
    m.price_timestamp,
    current_timestamp()                  as loaded_at
from assets a
left join metadata m
    on replace(a.asset_id, 'USDT', '') = m.symbol
