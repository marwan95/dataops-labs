with source as (
    select * from {{ source('RAW', 'raw_orders') }}
),

cleaned as (
    select
        order_id::integer                           as order_id,
        trim(customer_id)::text                     as customer_id,
        order_date::date                            as order_date,
        lower(trim(status))::text                   as order_status,
        trim(store_id)::text                        as store_id,
        coalesce(shipping_fee, 0)::numeric(12,2)    as shipping_fee,
        trim(currency)::text                        as currency
    from source
)

select * from cleaned
