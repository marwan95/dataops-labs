-- Finds order items where discount_pct is outside the valid range [0, 100].
-- Any rows returned = test failure.
select
    order_item_id,
    order_id,
    product_id,
    discount_pct
from {{ ref('stg_order_items') }}
where discount_pct < 0
   or discount_pct > 100
