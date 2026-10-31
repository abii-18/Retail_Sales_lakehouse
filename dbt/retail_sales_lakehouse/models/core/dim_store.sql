{{ config(
    materialized = 'table'
) }}

with latest_store as (

    select *
    from {{ ref('stg_stores') }}

    qualify row_number() over (
        partition by store_id
        order by created_at desc
    ) = 1

)

select
    md5('retail:store:' || cast(store_id as varchar)) as store_key,
    store_id,
    store_name,
    store_type,
    store_size,
    region,
    country,
    opened_date,
    created_at,
    batch_date
from latest_store