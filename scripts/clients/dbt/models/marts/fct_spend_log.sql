{{ config(
    materialized='incremental',
    incremental_strategy='append',
) }}

-- Every delivery row as loaded, for reconciliation with platform invoices.
select
    platform,
    campaign_id,
    event_date,
    spend,
    loaded_at
from {{ ref('stg_ad_platform_daily') }}
{% if is_incremental() %}
where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
