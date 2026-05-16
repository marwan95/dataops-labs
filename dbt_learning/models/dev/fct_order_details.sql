{{
    config(
        materialized='incremental',
        unique_key='order_item_id'
    )
}}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['oi.order_id', 'oi.order_item_id']) }} as order_detail_sk,
        oi.order_item_id,
        oi.order_id,
        o.order_date,
        o.order_status,
        o.store_id,
        oi.product_id,
        p.product_name,
        p.category,
        o.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        oi.quantity,
        oi.unit_price,
        oi.discount_pct,
        o.shipping_fee,
        p.currency,
        (oi.quantity * oi.unit_price)::numeric(12,2)                               as gross_amount,
        {{ calculate_revenue('oi.quantity', 'oi.unit_price', 'oi.discount_pct') }} as net_amount
    from order_items oi
    left join orders   o on oi.order_id   = o.order_id
    left join products p on oi.product_id = p.product_id
    left join customers c on o.customer_id = c.customer_id

    {% if is_incremental() %}
    where o.order_date > (select max(order_date) from {{ this }}) - interval '3 days'
    {% endif %}
),

with_totals as (
    select
        *,
        (net_amount + shipping_fee)::numeric(12,2)              as total_amount,
        {{ convert_currency('net_amount', 'currency', 'USD') }} as net_amount_usd
    from joined
)

select * from with_totals
