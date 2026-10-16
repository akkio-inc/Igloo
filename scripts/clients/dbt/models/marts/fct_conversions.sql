{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='conversion_id',
) }}

select
    conversion_id,
    campaign_id,
    converted_at,
    conversion_date,
    revenue,
    loaded_at
from {{ ref('stg_conversions') }}
{% if is_incremental() %}
where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
