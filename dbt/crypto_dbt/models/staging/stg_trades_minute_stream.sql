-- staging: read the streaming silver table (speed layer)
-- note: streaming computes a lighter metric set than batch -
-- price_variance is a proxy, not full realized variance / bipower
with source as (
    select * from `dm2-crypto-microstructure.silver.trades_minute_stream`
)
select
    symbol,
    window_start,
    window_end,
    cast(open  as float64)            as open_price,
    cast(high  as float64)            as high_price,
    cast(low   as float64)            as low_price,
    cast(close as float64)            as close_price,
    cast(vwap  as float64)            as vwap,
    cast(volume as float64)           as volume,
    cast(trade_count as int64)        as trade_count,
    cast(price_variance as float64)   as price_variance,
    cast(buy_volume as float64)       as buy_volume,
    cast(sell_volume as float64)      as sell_volume,
    cast(signed_volume as float64)    as signed_volume,
    cast(ofi as float64)              as ofi
from source
