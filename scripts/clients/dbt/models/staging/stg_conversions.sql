-- Platforms restate conversions (late attribution, refunds); keep the latest
-- version of each.
select
    conversion_id,
    campaign_id,
    converted_at,
    cast(converted_at as date) as conversion_date,
    coalesce(revenue, 0) as revenue,
    _loaded_at as loaded_at
from (
    select
        *,
        row_number() over (partition by conversion_id order by _loaded_at desc) as version_rank
    from {{ source('raw', 'conversions') }}
) as versions
where version_rank = 1
