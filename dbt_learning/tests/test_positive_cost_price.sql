-- Finds products with a non-positive cost_price.
-- Any rows returned = test failure.
select
    product_id,
    product_name,
    cost_price
from {{ ref('stg_products') }}
where cost_price <= 0
