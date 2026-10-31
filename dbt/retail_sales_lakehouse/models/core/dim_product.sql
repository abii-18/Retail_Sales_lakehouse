{{ config(
    materialized = 'table'
) }}

with latest_product as (

    select *
    from {{ ref('stg_products') }}

    qualify row_number() over (
        partition by product_id
        order by updated_at desc
    ) = 1

)

select
    md5('retail:product:' || cast(product_id as varchar)) as product_key,
    product_id,
    product_name,
    category,
    sub_category,
    unit_price,
    unit_cost,
    is_active,
    created_at,
    updated_at,
    batch_date
from latest_product