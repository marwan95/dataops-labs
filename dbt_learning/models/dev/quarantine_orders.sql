{{
    config(materialized='table')
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

future_orders as (
    select
        o.*,
        'future_order_date' as failure_reason
    from orders o
    where o.order_date > current_date
),

null_customer as (
    select
        o.*,
        'null_customer_id' as failure_reason
    from orders o
    where o.customer_id is null
),

invalid_customer as (
    select
        o.*,
        'invalid_customer_id' as failure_reason
    from orders o
    left join customers c on o.customer_id = c.customer_id
    where c.customer_id is null
      and o.customer_id is not null
),

negative_shipping as (
    select
        o.*,
        'negative_shipping_fee' as failure_reason
    from orders o
    where o.shipping_fee < 0
)

select * from future_orders
union all
select * from null_customer
union all
select * from invalid_customer
union all
select * from negative_shipping
