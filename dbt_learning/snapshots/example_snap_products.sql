{#
  EXAMPLE: This shows the general pattern for a dbt snapshot.
  Your task (Week 2) is to create a REAL snapshot file called snap_products.sql
  that tracks price and active-status changes on the products table.

  A snapshot wraps your query in a special Jinja block:

  {% snapshot your_snapshot_name %}

  {{
      config(
          target_schema='RAW',
          unique_key='primary_key_column',
          strategy='check',
          check_cols=['column_a', 'column_b']
      )
  }}

  select * from {{ source('RAW', 'your_table') }}

  {% endsnapshot %}

  STRATEGY OPTIONS:
    'check'     -> compares specific columns to detect changes
    'timestamp' -> uses an updated_at column to detect changes

  After running `dbt snapshot`, dbt adds these columns automatically:
    dbt_scd_id       -> unique ID for each version
    dbt_updated_at   -> when dbt last checked this row
    dbt_valid_from   -> when this version became active
    dbt_valid_to     -> when this version was replaced (null = current)
#}
