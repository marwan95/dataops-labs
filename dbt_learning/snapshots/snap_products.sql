{% snapshot snap_products %}

{{
    config(
        target_schema='RAW',
        unique_key='product_id',
        strategy='check',
        check_cols=['list_price', 'is_active']
    )
}}

select * from {{ source('RAW', 'raw_products') }}

{% endsnapshot %}
