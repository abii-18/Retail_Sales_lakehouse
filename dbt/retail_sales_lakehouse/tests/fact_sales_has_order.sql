-- A missing order must fail validation, while retaining the item for investigation.
select order_item_id
from {{ ref('fact_sales') }}
where order_id is null or date_key is null
