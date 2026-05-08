-- Finds orders whose order_date is in the future.
-- Any rows returned = test failure.
select
    order_id,
    customer_id,
    order_date
from {{ ref('stg_orders') }}
where order_date > current_date
