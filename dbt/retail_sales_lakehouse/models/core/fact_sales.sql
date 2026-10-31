{{ config(
    materialized = 'table'
) }}

with latest_orders as (

    select *
    from {{ ref('stg_orders') }}

    qualify row_number() over (
        partition by order_id
        order by batch_date desc
    ) = 1

),

latest_order_items as (

    select *
    from {{ ref('stg_order_items') }}

    qualify row_number() over (
        partition by order_item_id
        order by batch_date desc
    ) = 1

)

select
    oi.order_item_id,
    o.order_id,

    c.customer_key,
    p.product_key,
    s.store_key,

    cast(o.order_date as date) as date_key,

    oi.quantity,
    oi.unit_price_at_sale as unit_price,
    oi.unit_cost_at_sale as unit_cost,
    oi.discount_amount,
    oi.line_total,

    (
        oi.line_total - (oi.quantity * oi.unit_cost_at_sale)
    ) as profit,

    o.order_status,
    o.currency_code,

    oi.batch_date

from latest_order_items oi

left join latest_orders o
    on oi.order_id = o.order_id

left join {{ ref('dim_customer') }} c
    on o.customer_id = c.customer_id

left join {{ ref('dim_product') }} p
    on oi.product_id = p.product_id

left join {{ ref('dim_store') }} s
    on o.store_id = s.store_id