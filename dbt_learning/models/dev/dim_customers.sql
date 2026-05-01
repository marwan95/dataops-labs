with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('fct_order_details') }}
),

aggregated as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.signup_date,
        c.country,
        c.city,
        count(o.order_id)           as total_orders,
        sum(o.total_amount)         as total_spent
    from customers c
    left join orders o on c.customer_id = o.customer_id
    group by
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.signup_date,
        c.country,
        c.city
)

select * from aggregated
