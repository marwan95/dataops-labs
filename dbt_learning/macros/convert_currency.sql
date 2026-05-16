{% macro convert_currency(amount_column, currency_column, target_currency='USD') %}

{#- Exchange rates relative to USD -#}
{%- set rates_to_usd = {'USD': 1.00, 'OMR': 2.60, 'EUR': 1.08} -%}
{%- set target_rate = rates_to_usd[target_currency] if target_currency in rates_to_usd else 1.00 -%}

case
    when {{ currency_column }} = 'USD'
        then ({{ amount_column }} * 1.00  / {{ target_rate }})::numeric(12,2)
    when {{ currency_column }} = 'OMR'
        then ({{ amount_column }} * 2.60  / {{ target_rate }})::numeric(12,2)
    when {{ currency_column }} = 'EUR'
        then ({{ amount_column }} * 1.08  / {{ target_rate }})::numeric(12,2)
    else ({{ amount_column }})::numeric(12,2)
end

{% endmacro %}
