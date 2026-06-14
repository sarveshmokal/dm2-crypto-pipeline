-- Staging: read the silver per-minute table, standardize names/types.
-- Silver is produced by PySpark (bronze -> silver).
with source as (
    select * from `dm2-crypto-microstructure.silver.trades_minute`
)
select
    symbol,
    minute                                   as window_start,
    cast(open  as float64)                   as open_price,
    cast(high  as float64)                   as high_price,
    cast(low   as float64)                   as low_price,
    cast(close as float64)                   as close_price,
    cast(vwap  as float64)                   as vwap,
    cast(volume as float64)                  as volume,
    cast(trade_count as int64)               as trade_count,
    cast(realized_vol as float64)            as realized_vol,
    cast(buy_volume as float64)              as buy_volume,
    cast(sell_volume as float64)             as sell_volume,
    cast(signed_volume as float64)           as signed_volume
from source
