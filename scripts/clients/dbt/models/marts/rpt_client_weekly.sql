with weekly as (
    select
        c.client_name,
        c.channel_group,
        cast(date_trunc('week', f.event_date) as date) as week_start,
        sum(f.spend) as spend,
        sum(f.impressions) as impressions,
        sum(f.clicks) as clicks,
        sum(f.conversions) as conversions,
        sum(f.revenue) as revenue
    from {{ ref('fct_campaign_daily') }} as f
    inner join {{ ref('dim_campaigns') }} as c on c.campaign_id = f.campaign_id
    group by c.client_name, c.channel_group, cast(date_trunc('week', f.event_date) as date)
),

benchmarks as (
    select
        channel_group,
        percentile_cont(0.5) within group (order by spend) as median_weekly_spend
    from weekly
    group by channel_group
)

select
    w.client_name,
    w.channel_group,
    w.week_start,
    w.spend,
    w.impressions,
    w.clicks,
    w.conversions,
    w.revenue,
    case when w.spend > 0 then w.revenue / w.spend end as roas,
    case when w.conversions > 0 then w.spend / w.conversions end as cost_per_conversion,
    case
        when w.spend > 2 * b.median_weekly_spend then 'heavy'
        when w.spend < 0.5 * b.median_weekly_spend then 'light'
        else 'typical'
    end as spend_band,
    sum(w.spend) over (
        partition by w.client_name order by w.week_start, w.channel_group
        rows between unbounded preceding and current row
    ) as cumulative_client_spend,
    rank() over (partition by w.week_start order by w.spend desc) as spend_rank_in_week
from weekly as w
inner join benchmarks as b on b.channel_group = w.channel_group
