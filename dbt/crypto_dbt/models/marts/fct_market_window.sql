-- gold FACT table: one row per (asset, minute), now with the deepened metrics
{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_trades_minute') }}
)
select
    to_hex(md5(concat(symbol, '|', cast(window_start as string)))) as market_window_id,
    symbol            as asset_id,
    cast(format_timestamp('%Y%m%d%H%M', window_start) as int64) as time_id,
    window_start,
    open_price, high_price, low_price, close_price,
    vwap, volume, trade_count,
    realized_variance,
    realized_vol,
    bipower_variation,
    jump_component,
    buy_volume, sell_volume, signed_volume,
    ofi,
    -- short-horizon return over the window (close vs open), in log terms
    case when open_price > 0 then ln(close_price / open_price) else 0 end as window_return
from stg
