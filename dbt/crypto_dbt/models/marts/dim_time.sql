-- gold DIMENSION: time dimension at minute grain (his data-cube "time dimension", slide 37).
-- one row per distinct minute window observed across both batch and streaming facts.
-- supports OLAP roll-up / drill-down by hour, day, weekday, etc.
{{ config(materialized='table') }}

with batch_windows as (
    select window_start from {{ ref('stg_trades_minute') }}
),
stream_windows as (
    select window_start from {{ ref('stg_trades_minute_stream') }}
),
all_windows as (
    select window_start from batch_windows
    union distinct
    select window_start from stream_windows
)
select
    -- smart key YYYYMMDDHHMM, e.g. 202606150532
    cast(format_timestamp('%Y%m%d%H%M', window_start) as int64) as time_id,
    window_start                                          as window_start,
    date(window_start)                                    as window_date,
    extract(year   from window_start)                     as year,
    extract(month  from window_start)                     as month,
    extract(day    from window_start)                     as day,
    extract(hour   from window_start)                     as hour,
    extract(minute from window_start)                     as minute,
    format_timestamp('%A', window_start)                  as day_name,
    extract(dayofweek from window_start)                  as day_of_week,
    -- weekend flag (1=Sun..7=Sat in BigQuery)
    case when extract(dayofweek from window_start) in (1,7) then true else false end as is_weekend
from all_windows
