select
    platform,
    cast(campaign_id as integer) as campaign_id,
    cast(event_date as date) as event_date,
    impressions,
    clicks,
    cast(spend as numeric(14, 2)) as spend,
    _loaded_at as loaded_at
from {{ source('raw', 'ad_platform_daily') }}
where impressions >= 0
