-- gold ANALYSIS mart: the centerpiece that answers the research question.
-- classifies each minute into a volatility regime RELATIVE TO THE RECENT PAST
-- using a rolling window, then exposes the OFI / return / volatility relationship
-- per regime. rolling (not global) thresholds make the regime adaptive: a minute
-- is "turbulent" relative to what volatility has looked like recently, which is
-- the correct framing for a live stream where the baseline drifts over time.
{{ config(materialized='table') }}

with base as (
    select * from {{ ref('fct_market_window') }}
),

-- rolling baseline: for each minute, look back over the trailing window of recent
-- minutes (here up to 30 preceding rows) and compute the average and stddev of
-- realized_vol over that trailing window. these are WINDOW FUNCTIONS (OVER ...).
rolling as (
    select
        b.*,
        avg(realized_vol) over (
            partition by asset_id
            order by window_start
            rows between 30 preceding and 1 preceding
        ) as roll_mean_vol,
        stddev(realized_vol) over (
            partition by asset_id
            order by window_start
            rows between 30 preceding and 1 preceding
        ) as roll_std_vol,
        count(*) over (
            partition by asset_id
            order by window_start
            rows between 30 preceding and 1 preceding
        ) as roll_n
    from base b
),

classified as (
    select
        r.*,
        -- z-score of this minute's vol vs the trailing window
        case
            when roll_std_vol is not null and roll_std_vol > 0
            then (realized_vol - roll_mean_vol) / roll_std_vol
            else null
        end as vol_zscore,
        case
            -- not enough history yet to judge a regime
            when roll_n is null or roll_n < 5 then 'warmup'
            when roll_std_vol is null or roll_std_vol = 0 then 'normal'
            -- below the recent average => calm; well above => turbulent
            when (realized_vol - roll_mean_vol) / roll_std_vol <= -0.5 then 'calm'
            when (realized_vol - roll_mean_vol) / roll_std_vol >=  1.0 then 'turbulent'
            else 'normal'
        end as volatility_regime,
        case
            when realized_variance > 0
                 and (jump_component / realized_variance) > 0.5
            then true else false
        end as is_jump_minute
    from rolling r
)

select
    market_window_id,
    asset_id,
    window_start,
    volatility_regime,
    vol_zscore,
    realized_vol,
    realized_variance,
    bipower_variation,
    jump_component,
    is_jump_minute,
    ofi,
    signed_volume,
    window_return,
    volume,
    trade_count,
    roll_mean_vol,
    roll_std_vol
from classified
