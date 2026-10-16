select
    c.campaign_id,
    c.client_name,
    c.channel,
    c.channel_group,
    c.campaign_name,
    c.start_date,
    c.daily_budget,
    s.bid_strategy,
    s.status
from {{ ref('stg_campaigns') }} as c
left join {{ ref('campaign_settings_snapshot') }} as s
    on s.campaign_id = c.campaign_id and s.dbt_valid_to is null
