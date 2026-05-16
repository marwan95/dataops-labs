{{
    config(materialized='table')
}}

{% set target_year = 2024 %}
{% set months = [
    ('01', 'jan'), ('02', 'feb'), ('03', 'mar'), ('04', 'apr'),
    ('05', 'may'), ('06', 'jun'), ('07', 'jul'), ('08', 'aug'),
    ('09', 'sep'), ('10', 'oct'), ('11', 'nov'), ('12', 'dec')
] %}

with orders as (
    select
        {{ dbt_utils.star(from=ref('stg_orders'), except=["currency", "order_status"]) }}
    from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

revenue_base as (
    select
        o.store_id,
        o.order_date,
        {{ calculate_revenue('oi.quantity', 'oi.unit_price', 'oi.discount_pct') }} as net_revenue
    from order_items oi
    left join orders o on oi.order_id = o.order_id
    where extract(year from o.order_date) = {{ target_year }}
),

pivoted as (
    select
        store_id,
        {% for month_num, month_name in months %}
        sum(
            case when extract(month from order_date) = {{ month_num | int }}
                 then net_revenue else 0
            end
        )::numeric(12,2) as {{ month_name }}_revenue
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from revenue_base
    group by store_id
)

select * from pivoted
