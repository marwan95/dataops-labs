# Data Quality Report — Week 3

## Overview

This report documents data quality issues discovered in the ecommerce seed dataset
through dbt generic tests and custom singular SQL tests. Fourteen issues were
intentionally planted across four source tables. Ten or more were identified and
remediated through the quarantine pattern introduced in Task 3.3.

---

## Identified Issues

### Table: raw_orders / stg_orders

| Issue | Affected Row(s) | Test | Remediation |
|---|---|---|---|
| Future order date (`2099-12-31`) | order_id 1201 | `test_no_future_orders` | Reject or flag row; request corrected date from upstream system |
| NULL customer_id | order_id 1200 | `not_null` on `customer_id` | Quarantine row; trace back to order capture system to recover customer reference |
| Invalid customer reference (`C-999`) | order_id 1203 | `relationships` on `customer_id` | Quarantine row; C-999 does not exist in stg_customers — likely a data-entry error |
| Negative shipping fee (`-10.0`) | order_id 1204 | `test_positive_shipping` | Quarantine row; negative fees are logically impossible — treat as data-entry error |
| Duplicate order_id (`1050`) | order_id 1050 (×2) | `unique` on `order_id` | Deduplicate upstream; only one of the two rows is correct |
| Whitespace-padded status (`  Completed  `) | order_id 1202 | `accepted_values` on `order_status` (raw layer) | Staging model already trims/lowercases — confirm upstream system sends clean values |

### Table: raw_order_items / stg_order_items

| Issue | Affected Row(s) | Test | Remediation |
|---|---|---|---|
| Negative quantity (`-2`) | order_item_id 309 (order 1010, P-005) | `test_positive_quantities` | Quarantine or reject; quantities must be ≥ 1 |
| Discount > 100% (`150.0`) | order_item_id 311 (order 1020, P-009) | `test_valid_discount_range` | Cap at 100 or reject; a 150% discount would produce a negative revenue line |
| Invalid product reference (`P-999`) | order_item_id 312 (order 1025) | `relationships` on `product_id` | Quarantine; P-999 is not in the product catalogue |
| Duplicate order_item_id (`1`) | order_item_id 1 (appears twice) | `unique` on `order_item_id` | Deduplicate upstream; use the row linked to the correct order |
| Zero unit_price | order_item_id 310 (order 1015, P-018) | business logic check | Investigate: could be a legitimate promotional price or a data error |

### Table: raw_products / stg_products

| Issue | Affected Row(s) | Test | Remediation |
|---|---|---|---|
| Negative cost_price (`-5.0`) | P-035 (Old Keyboard Model) | `test_positive_cost_price` | Correct the price; negative cost is not physically meaningful |

### Table: raw_customers / stg_customers

| Issue | Affected Row(s) | Test | Remediation |
|---|---|---|---|
| Duplicate email address | C-201 and C-215 share `ahmed.albalushi@email.com` | `unique` on `email` | Investigate which customer owns the address; update the incorrect record |
| NULL email address | C-230 (Samira Al-Kindi) | `not_null` on `email` | Reach out to customer to capture email; required for communications |

---

## Quarantine Strategy

Rows failing critical checks on `stg_orders` are captured in `quarantine_orders`
(see `models/dev/quarantine_orders.sql`). Each row carries a `failure_reason` column
so downstream teams can triage issues without scanning the full order history.

Current quarantine rules:

- `future_order_date` — order_date is after today
- `null_customer_id` — customer_id is missing
- `invalid_customer_id` — customer_id has no match in stg_customers
- `negative_shipping_fee` — shipping_fee is less than zero

---

## Recommendations

1. **Add NOT NULL constraints** at the database/ingestion layer for `customer_id` and `email`.
2. **Enforce referential integrity** between orders and customers at the API/ETL boundary before data lands in RAW.
3. **Validate numeric ranges** (quantity ≥ 1, 0 ≤ discount ≤ 100, cost_price > 0) in the order management system before export.
4. **Schedule `dbt test`** as part of every pipeline run so new violations are caught immediately rather than discovered in downstream dashboards.
5. **Deduplicate primary keys** (`order_id`, `order_item_id`) by adding unique constraints or upsert logic in the ingestion pipeline.
