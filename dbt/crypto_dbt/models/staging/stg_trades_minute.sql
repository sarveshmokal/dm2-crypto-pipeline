-- staging: read the deepened silver table, pass through the new metrics
with source as (
    select * from `dm2-crypto-microstructure.silver.trades_minute`
)
select
    symbol,
    minute                              as window_start,
    cast(open  as float64)              as open_price,
    cast(high  as float64)              as high_price,
    cast(low   as float64)              as low_price,
    cast(close as float64)              as close_price,
    cast(vwap  as float64)              as vwap,
    cast(volume as float64)             as volume,
    cast(trade_count as int64)          as trade_count,
    cast(realized_variance as float64)  as realized_variance,
    cast(realized_vol as float64)       as realized_vol,
    cast(bipower_variation as float64)  as bipower_variation,
    cast(jump_component as float64)     as jump_component,
    cast(buy_volume as float64)         as buy_volume,
    cast(sell_volume as float64)        as sell_volume,
    cast(signed_volume as float64)      as signed_volume,
    cast(ofi as float64)                as ofi
from source
