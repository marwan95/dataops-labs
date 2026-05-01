with source as (
    select * from {{ source('RAW', 'raw_order_items') }}
),

cleaned as (
    select
        order_item_id::integer                          as order_item_id,
        order_id::integer                               as order_id,
        trim(product_id)::text                          as product_id,
        quantity::integer                               as quantity,
        unit_price::numeric(12,2)                       as unit_price,
        coalesce(discount_pct, 0)::numeric(5,2)         as discount_pct
    from source
)

select * from cleaned
