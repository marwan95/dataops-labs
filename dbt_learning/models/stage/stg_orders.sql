-- ══════════════════════════════════════════════════════════════
-- 🟡 STAGE · stg_orders
-- Purpose : Clean, cast, and standardise the raw orders seed.
-- Rules   : This is the ONLY layer that may touch raw sources.
-- ══════════════════════════════════════════════════════════════

with source as (

    select * from {{ ref('raw_orders') }}

),

cleaned as (

    select
        -- ── Primary key ──────────────────────────────────────
        order_id::integer                               as order_id,

        -- ── Foreign key ──────────────────────────────────────
        trim(customer_id)::text                         as customer_id,

        -- ── Date ─────────────────────────────────────────────
        order_date::date                                as order_date,

        -- ── Status — lowercase + trim for consistency ────────
        lower(trim(status))::text                       as order_status,

        -- ── Product details ──────────────────────────────────
        trim(store_id)::text                            as store_id,
        coalesce(shipping_fee, 0)::numeric(12,2)        as shipping_fee,

        -- ── Currency — uppercase for ISO consistency ─────────
        upper(trim(currency))::text                     as currency

    from source

)

select * from cleaned
