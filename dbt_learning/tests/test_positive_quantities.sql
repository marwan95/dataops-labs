-- Finds order items with a non-positive quantity.
-- Any rows returned = test failure.
select
    order_item_id,
    order_id,
    product_id,
    quantity
from {{ ref('stg_order_items') }}
where quantity <= 0
