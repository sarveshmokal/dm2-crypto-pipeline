-- Gold FACT table: one row per (asset, minute) window.
-- Grain: symbol x window_start. This is the star-schema fact.
{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_trades_minute') }}
)
select
    to_hex(md5(concat(symbol, '|', cast(window_start as string)))) as market_window_id,
    symbol            as asset_id,        -- FK to dim_asset
    window_start,
    open_price, high_price, low_price, close_price,
    vwap, volume, trade_count,
    realized_vol,
    buy_volume, sell_volume, signed_volume
from stg
