-- Finds orders with a negative shipping fee.
-- Any rows returned = test failure.
select
    order_id,
    customer_id,
    order_date,
    shipping_fee
from {{ ref('stg_orders') }}
where shipping_fee < 0
