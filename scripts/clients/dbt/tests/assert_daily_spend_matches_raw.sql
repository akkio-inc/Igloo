-- Every unit of platform spend lands in fct_campaign_daily exactly once,
-- including late rows merged into days already reported.
with raw_total as (
    select sum(spend) as spend from {{ ref('stg_ad_platform_daily') }}
),

fct_total as (
    select sum(spend) as spend from {{ ref('fct_campaign_daily') }}
)

select r.spend as raw_spend, f.spend as fct_spend
from raw_total as r
cross join fct_total as f
where r.spend <> f.spend
