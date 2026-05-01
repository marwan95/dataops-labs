with source as (
    select * from {{ source('RAW', 'raw_store_locations') }}
),

cleaned as (
    select
        trim(store_id)::text        as store_id,
        trim(store_name)::text      as store_name,
        trim(city)::text            as city,
        trim(country)::text         as country,
        trim(region)::text          as region,
        opened_date::date           as opened_date
    from source
)

select * from cleaned
