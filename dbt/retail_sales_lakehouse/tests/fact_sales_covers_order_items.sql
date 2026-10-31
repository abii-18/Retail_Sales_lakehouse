-- Fail if modeling silently loses an order item (including a missing parent order).
select distinct order_item_id
from {{ ref('stg_order_items') }}
except
select order_item_id
from {{ ref('fact_sales') }}
