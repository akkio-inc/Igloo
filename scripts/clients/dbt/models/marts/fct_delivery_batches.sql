{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_date',
    batch_size='day',
    begin='2026-09-01',
    unique_key=['platform', 'campaign_id', 'event_date'],
) }}

-- Delivery per platform, campaign and day, built a day at a time: a run
-- builds only the days it is given (--event-time-start and
-- --event-time-end), reading only those days of the staging model.
select
    platform,
    campaign_id,
    event_date,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(spend) as spend
from {{ ref('stg_ad_platform_daily') }}
group by platform, campaign_id, event_date
