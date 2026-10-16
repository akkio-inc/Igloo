select
    c.campaign_id,
    c.client_name,
    lower(c.channel) as channel,
    ch.channel_group,
    c.campaign_name,
    c.start_date,
    c.daily_budget
from {{ ref('campaigns') }} as c
left join {{ ref('channels') }} as ch on ch.channel = c.channel
