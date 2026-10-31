with latest_customer as (

    select *
    from {{ ref('stg_customers') }}

    qualify row_number() over (
        partition by customer_id
        order by updated_at desc
    ) = 1

)

select
    md5('retail:customer:' || cast(customer_id as varchar)) as customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    country,
    signup_date,
    is_active,
    created_at,
    updated_at,
    batch_date
from latest_customer