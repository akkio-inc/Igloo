{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['campaign_id', 'event_date'],
) }}

with delivery as (
    select
        campaign_id,
        event_date,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(spend) as spend,
        max(loaded_at) as loaded_at
    from {{ ref('stg_ad_platform_daily') }}
    {% if is_incremental() %}
    -- Rebuild every day that received rows since the last run, including
    -- late rows for days already reported.
    where event_date in (
        select distinct event_date
        from {{ ref('stg_ad_platform_daily') }}
        where loaded_at > (select max(loaded_at) from {{ this }})
    )
    {% endif %}
    group by campaign_id, event_date
),

conversions as (
    select
        campaign_id,
        conversion_date,
        count(*) as conversions,
        sum(revenue) as revenue
    from {{ ref('stg_conversions') }}
    group by campaign_id, conversion_date
)

select
    d.campaign_id,
    d.event_date,
    d.impressions,
    d.clicks,
    d.spend,
    coalesce(c.conversions, 0) as conversions,
    coalesce(c.revenue, 0) as revenue,
    case when d.impressions > 0 then cast(d.clicks as double precision) / d.impressions end as ctr,
    d.loaded_at
from delivery as d
left join conversions as c
    on c.campaign_id = d.campaign_id and c.conversion_date = d.event_date
