-- gold FACT table (speed layer): one row per (asset, minute) from the live stream.
-- this is the streaming counterpart to fct_market_window. kept separate because
-- the streaming path computes a lighter, lower-latency metric set than the batch
-- serving layer (Lambda architecture: batch layer + speed layer served separately).
{{ config(materialized='table') }}
with stg as (
    select * from {{ ref('stg_trades_minute_stream') }}
)
select
    to_hex(md5(concat(symbol, '|', cast(window_start as string)))) as market_window_id,
    symbol            as asset_id,
    cast(format_timestamp('%Y%m%d%H%M', window_start) as int64) as time_id,
    window_start,
    window_end,
    open_price, high_price, low_price, close_price,
    vwap, volume, trade_count,
    realized_variance,
    realized_vol,
    buy_volume, sell_volume, signed_volume,
    ofi,
    -- short-horizon return over the window (close vs open), in log terms
    case when open_price > 0 then ln(close_price / open_price) else 0 end as window_return
from stg
