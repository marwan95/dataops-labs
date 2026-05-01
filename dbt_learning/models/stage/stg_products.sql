with source as (
    select * from {{ source('RAW', 'raw_products') }}
),

cleaned as (
    select
        trim(product_id)::text              as product_id,
        trim(product_name)::text            as product_name,
        trim(category)::text                as category,
        trim(subcategory)::text             as subcategory,
        cost_price::numeric(12,2)           as cost_price,
        list_price::numeric(12,2)           as list_price,
        trim(currency)::text                as currency,
        launch_date::date                   as launch_date,
        is_active::boolean                  as is_active
    from source
)

select * from cleaned
