# DataOps & dbt Mentorship — Weekly Assignments & Evaluation

**Team:** Marwan, Abdul Ahad, Ghazi Alchamat
**Stack:** PostgreSQL · dbt · Airflow · Docker

---

## Seed Data Overview

All assignments use the following interconnected seed tables:

| Seed File | Rows | Description |
|---|---|---|
| `raw_customers.csv` | 50 | Customer dimension (name, email, phone, country) |
| `raw_products.csv` | 35 | Product catalog (category, cost/list price, currency) |
| `raw_orders.csv` | 156 | Order headers (customer FK, store FK, status, date) |
| `raw_order_items.csv` | 313 | Line items (order FK, product FK, qty, price, discount) |
| `raw_store_locations.csv` | 8 | Store dimension (city, country, region) |

> [!IMPORTANT]
> The data contains **intentional quality issues** that mentees should NOT be told about until Week 3. Let them discover issues naturally during Weeks 1–2.

---

## Grading Scale

Each week is scored out of **100 points**. Final grade is the average across all 6 weeks.

| Grade | Range | Meaning |
|---|---|---|
| 🟢 Excellent | 90–100 | Exceeds expectations, production-quality work |
| 🔵 Good | 75–89 | Meets all requirements with minor issues |
| 🟡 Satisfactory | 60–74 | Core tasks complete but gaps in quality |
| 🔴 Needs Work | < 60 | Significant gaps, requires re-submission |

---

## Week 1: Sources, Models, and Seeds

### Learning Objectives
- Understand how dbt seeds load CSV data into PostgreSQL
- Define sources in a YAML schema file
- Build STAGE models (cleansing layer) and DEV models (business logic layer)
- Follow the rule: **DEV never touches raw data — only STAGE**

### Assignment Tasks

#### Task 1.1 — Load All Seeds (15 pts)
Run `dbt seed` to load all 5 CSV files into the `RAW` schema.

**Deliverable:** Screenshot of `dbt seed` output showing all 5 seeds loaded successfully.

| Criteria | Points |
|---|---|
| All 5 seeds load without errors | 10 |
| Seeds land in the `RAW` schema (not `public`) | 5 |

---

#### Task 1.2 — Define Sources (15 pts)
Create `models/stage/sources.yml` that declares all 5 raw tables as dbt sources.

**Deliverable:** The `sources.yml` file.

| Criteria | Points |
|---|---|
| All 5 tables declared under a single `RAW` source | 5 |
| Each source table lists its columns | 5 |
| File uses correct YAML syntax and compiles (`dbt compile`) | 5 |

---

#### Task 1.3 — Build STAGE Models (40 pts)
Create one staging model per seed table in `models/stage/`:

| Model | Key Requirements |
|---|---|
| `stg_customers.sql` | Trim names, lowercase email, cast `signup_date` to date |
| `stg_products.sql` | Trim `product_name`, cast prices to `numeric(12,2)`, cast `is_active` to boolean |
| `stg_orders.sql` | Lowercase + trim `status`, cast `order_date` to date, default `shipping_fee` nulls to 0 |
| `stg_order_items.sql` | Cast qty to integer, prices to `numeric(12,2)`, default `discount_pct` to 0 |
| `stg_store_locations.sql` | Trim all text, cast `opened_date` to date |

**Deliverable:** All 5 SQL files under `models/stage/`.

| Criteria | Points |
|---|---|
| All 5 models created and compile | 10 |
| Models use `{{ source('RAW', 'table') }}` not `{{ ref() }}` | 5 |
| Proper casting and type handling | 10 |
| Trim/lower/coalesce applied consistently | 10 |
| Clean CTE structure (source → cleaned → select) | 5 |

---

#### Task 1.4 — Build DEV Fact Model (20 pts)
Create `models/dev/fct_order_details.sql` that joins staged orders, order items, products, and customers.

**Deliverable:** The `fct_order_details.sql` file that produces one row per order line with:
- Customer name (`first_name || ' ' || last_name`)
- Product name and category
- `gross_amount` = `quantity * unit_price`
- `net_amount` = `gross_amount * (1 - discount_pct / 100)`
- `total_amount` = `net_amount + shipping_fee` (from orders table)

| Criteria | Points |
|---|---|
| Model references ONLY `{{ ref('stg_...') }}` — never raw sources | 5 |
| All joins are correct (items → orders, items → products, orders → customers) | 5 |
| Calculated columns are correct | 5 |
| Model runs without errors (`dbt run`) | 5 |

---

#### Task 1.5 — Build DEV Dimension Model (10 pts)
Create `models/dev/dim_customers.sql` — a customer dimension with:
- Customer ID, full name, email, country, city
- `signup_date`
- `total_orders` (count of orders per customer)
- `total_spent` (sum of `total_amount` per customer)

| Criteria | Points |
|---|---|
| Correct aggregation and joins | 5 |
| References only the STAGE layer | 3 |
| Model runs successfully | 2 |

---

### Week 1 Total: **100 points**

---

## Week 2: Materializations

### Learning Objectives
- Understand Table vs View materializations and when to use each
- Build an incremental model that processes only new records
- Create a snapshot to track slowly changing dimensions

### Assignment Tasks

#### Task 2.1 — Materialization Comparison (15 pts)
Write a short document (`docs/materializations.md`) explaining:
1. What is the difference between a `table` and a `view` in PostgreSQL?
2. When would you use a `view` in the STAGE layer vs a `table` in the DEV layer?
3. What problem does `incremental` materialization solve?

| Criteria | Points |
|---|---|
| Correct explanation of table vs view | 5 |
| Valid reasoning for layer-materialization pairing | 5 |
| Incremental use case explained with example | 5 |

---

#### Task 2.2 — Incremental Fact Model (40 pts)
Convert `fct_order_details` to an **incremental** model that:
- Uses `order_date` as the incremental column
- Only processes orders from the last 3 days on each run (look-back window)
- Uses `unique_key` on `order_item_id` to handle duplicates

**Deliverable:** Updated `fct_order_details.sql` with `{{ config(materialized='incremental') }}`.

| Criteria | Points |
|---|---|
| Correct `config()` block with `materialized='incremental'` | 5 |
| `unique_key` is set to `order_item_id` | 5 |
| `{% if is_incremental() %}` block filters on `order_date` | 10 |
| Look-back window uses `current_date - interval '3 days'` | 5 |
| Full refresh (`dbt run --full-refresh`) succeeds | 5 |
| Regular incremental run succeeds | 5 |
| Student can explain what happens on first run vs subsequent runs | 5 |

---

#### Task 2.3 — Snapshot: Product Price Changes (30 pts)
Create `snapshots/snap_products.sql` that tracks changes to `list_price` and `is_active` in `raw_products`.

**Deliverable:** A working snapshot file.

| Criteria | Points |
|---|---|
| Correct `{% snapshot %}` block syntax | 5 |
| Uses `strategy='check'` with `check_cols=['list_price', 'is_active']` | 10 |
| `unique_key` set to `product_id` | 5 |
| Snapshot runs (`dbt snapshot`) without errors | 5 |
| Student can explain `dbt_valid_from` and `dbt_valid_to` columns | 5 |

---

#### Task 2.4 — Simulation Exercise (15 pts)
1. Run `dbt snapshot` once (baseline)
2. Manually update `list_price` for 2 products in the `raw_products` CSV
3. Run `dbt seed` then `dbt snapshot` again
4. Query the snapshot table and show both the old and new records

**Deliverable:** SQL query output showing the version history.

| Criteria | Points |
|---|---|
| Successfully modified seed data | 3 |
| Snapshot captured both old and new versions | 7 |
| Query correctly filters to show history for changed products | 5 |

---

### Week 2 Total: **100 points**

---

## Week 3: Data Tests

### Learning Objectives
- Write generic tests (`unique`, `not_null`, `accepted_values`, `relationships`)
- Write custom singular tests in SQL
- Save failing rows to a quarantine table for investigation

> [!TIP]
> **For the mentor:** This is where the planted data quality issues come into play. The mentees should discover them through testing. Here is the full list of planted issues:

<details>
<summary>🔑 Answer Key — Planted Data Quality Issues</summary>

**raw_customers.csv:**
| Row | Issue | Test That Catches It |
|---|---|---|
| C-215 | Duplicate email (same as C-201) | `unique` test on `email` |
| C-230 | Empty email | `not_null` test on `email` |
| C-240 | Empty phone | `not_null` test on `phone` |

**raw_products.csv:**
| Row | Issue | Test That Catches It |
|---|---|---|
| P-035 | Negative `cost_price` (-5.00) | Custom test: `cost_price >= 0` |
| P-035 | Extra whitespace in `product_name` | Visual check / trim comparison |

**raw_orders.csv:**
| Row | Issue | Test That Catches It |
|---|---|---|
| order_id 1050 | Duplicate (appears twice) | `unique` test on `order_id` |
| order_id 1200 | Empty `customer_id` | `not_null` test on `customer_id` |
| order_id 1201 | Future date (2099-12-31) | Custom test: `order_date <= current_date` |
| order_id 1202 | Status = `"  Completed  "` (whitespace) | `accepted_values` test |
| order_id 1203 | Orphan `customer_id = C-999` | `relationships` test |
| order_id 1204 | Negative `shipping_fee` (-10.00) | Custom test: `shipping_fee >= 0` |

**raw_order_items.csv:**
| Row | Issue | Test That Catches It |
|---|---|---|
| order_item_id 1 | Duplicate (appears twice) | `unique` test on `order_item_id` |
| one row | quantity = -2 | Custom test: `quantity > 0` |
| one row | unit_price = 0.00 | Custom test: `unit_price > 0` |
| one row | discount_pct = 150% | Custom test: `discount_pct between 0 and 100` |
| one row | product_id = P-999 (orphan) | `relationships` test |

</details>

### Assignment Tasks

#### Task 3.1 — Generic Tests in YAML (30 pts)
Add tests to `models/stage/_schema.yml` for all staging models:

| Model | Required Tests |
|---|---|
| `stg_customers` | `unique` + `not_null` on `customer_id`, `not_null` on `email` |
| `stg_products` | `unique` + `not_null` on `product_id` |
| `stg_orders` | `unique` + `not_null` on `order_id`, `not_null` on `customer_id`, `accepted_values` on `order_status` (completed, pending, shipped, returned, cancelled) |
| `stg_order_items` | `unique` + `not_null` on `order_item_id`, `relationships` to `stg_orders` on `order_id`, `relationships` to `stg_products` on `product_id` |
| `stg_store_locations` | `unique` + `not_null` on `store_id` |

**Deliverable:** Updated `_schema.yml` and `dbt test` output.

| Criteria | Points |
|---|---|
| All required generic tests are defined | 10 |
| `dbt test` runs (some tests SHOULD fail — that's the point) | 5 |
| Student can identify and explain each failure | 10 |
| Correct YAML syntax | 5 |

---

#### Task 3.2 — Custom Singular Tests (35 pts)
Write SQL tests in the `tests/` directory:

| Test File | What It Checks |
|---|---|
| `test_no_future_orders.sql` | No `order_date > current_date` |
| `test_positive_quantities.sql` | No `quantity <= 0` in order items |
| `test_valid_discount_range.sql` | No `discount_pct < 0` or `discount_pct > 100` |
| `test_positive_shipping.sql` | No `shipping_fee < 0` in orders |
| `test_positive_cost_price.sql` | No `cost_price < 0` in products |

**Deliverable:** All 5 test files + `dbt test` output showing failures.

| Criteria | Points |
|---|---|
| All 5 custom tests are created | 10 |
| Tests use `{{ ref() }}` to reference staged models | 5 |
| Tests return failing rows correctly (SELECT bad rows) | 10 |
| Student documents which rows fail and why | 10 |

---

#### Task 3.3 — Quarantine Table (20 pts)
Create `models/dev/quarantine_orders.sql` — a model that captures all "bad" orders:

```sql
-- Orders that fail any quality check:
--   future dates, missing customer, negative shipping, bad status
```

**Deliverable:** A working quarantine model with clear filter logic.

| Criteria | Points |
|---|---|
| Model captures all known bad orders | 10 |
| Clear comments explaining each filter condition | 5 |
| Model materializes as a table | 5 |

---

#### Task 3.4 — Data Quality Report (15 pts)
Write a short summary (in `docs/data_quality_report.md`) listing:
1. Every data issue found
2. Which table and row it's in
3. What test caught it
4. Recommended fix (filter out? fix upstream? default value?)

| Criteria | Points |
|---|---|
| All issues documented (must find at least 10 of the 14 planted issues) | 10 |
| Reasonable fix recommendations | 5 |

---

### Week 3 Total: **100 points**

---

## Week 4: Macros and Packages

### Learning Objectives
- Understand Jinja templating (variables, loops, conditionals)
- Create reusable macros
- Install and use community dbt packages

### Assignment Tasks

#### Task 4.1 — Jinja Basics Exercise (15 pts)
Create `models/dev/fct_monthly_revenue.sql` that uses Jinja to:
- Define a variable for the year to filter: `{% set target_year = 2024 %}`
- Use a `{% for %}` loop to generate `CASE WHEN` statements for each month (Jan–Dec)
- Produce columns: `jan_revenue`, `feb_revenue`, ..., `dec_revenue`

| Criteria | Points |
|---|---|
| Correct use of `{% set %}` variable | 3 |
| `{% for %}` loop generates 12 month columns | 7 |
| Model compiles and produces correct pivot | 5 |

---

#### Task 4.2 — Currency Converter Macro (30 pts)
Create `macros/convert_currency.sql`:

```sql
-- Usage: {{ convert_currency('amount_column', 'currency_column', 'USD') }}
-- Converts OMR and EUR to USD using fixed rates:
--   1 OMR = 2.60 USD
--   1 EUR = 1.08 USD
```

Then use this macro in `fct_order_details` to add a `total_amount_usd` column.

**Deliverable:** The macro file + updated model.

| Criteria | Points |
|---|---|
| Macro accepts column name, source currency, and target currency | 5 |
| Handles at least 3 currencies (USD, OMR, EUR) | 5 |
| Uses `{% if %}` / `{% elif %}` or CASE WHEN | 5 |
| Macro is applied in at least one model | 5 |
| Correct USD conversion results (manually verified for 3 rows) | 10 |

---

#### Task 4.3 — Reusable Metric Macro (20 pts)
Create `macros/calculate_revenue.sql`:

```sql
-- Usage: {{ calculate_revenue('quantity', 'unit_price', 'discount_pct') }}
-- Returns: quantity * unit_price * (1 - discount_pct / 100.0)
```

Replace the hardcoded revenue formulas in `fct_order_details` with this macro.

| Criteria | Points |
|---|---|
| Macro is parameterized and reusable | 5 |
| Applied in `fct_order_details` replacing hardcoded math | 10 |
| Results match the original hardcoded version | 5 |

---

#### Task 4.4 — Install dbt-utils Package (20 pts)
1. Add `dbt-utils` to `packages.yml`
2. Run `dbt deps`
3. Use at least 2 dbt-utils macros in your models:
   - `{{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }}` in `fct_order_details`
   - `{{ dbt_utils.date_spine(...) }}` or `{{ dbt_utils.pivot(...) }}` in a new model

| Criteria | Points |
|---|---|
| `packages.yml` is correct and `dbt deps` succeeds | 5 |
| `generate_surrogate_key` used correctly | 5 |
| Second dbt-utils macro used in a meaningful way | 5 |
| Models compile and run | 5 |

---

#### Task 4.5 — Macro Documentation (15 pts)
Add a YAML description for each custom macro explaining:
- What it does
- Arguments it accepts
- Example usage

| Criteria | Points |
|---|---|
| Both macros documented | 10 |
| Examples are clear and correct | 5 |

---

### Week 4 Total: **100 points**

---

## Week 5: Hooks, Exposures, and Documentation

### Learning Objectives
- Use pre/post hooks to run SQL after a model builds
- Define exposures that link dbt models to dashboards
- Write comprehensive model and column documentation

### Assignment Tasks

#### Task 5.1 — Post-Hook: Create Indexes (25 pts)
Add post-hooks to the following DEV models:

| Model | Index To Create |
|---|---|
| `fct_order_details` | Index on `order_date` |
| `fct_order_details` | Index on `customer_id` |
| `dim_customers` | Index on `country` |

**Deliverable:** Updated `config()` blocks with post-hooks.

| Criteria | Points |
|---|---|
| Correct `post_hook` syntax in model config | 5 |
| Index uses `IF NOT EXISTS` to be idempotent | 10 |
| Models build without errors | 5 |
| Student can query `pg_indexes` to verify the index was created | 5 |

---

#### Task 5.2 — Post-Hook: Grant Permissions (10 pts)
Add a project-level post-hook in `dbt_project.yml` that runs:
```sql
GRANT SELECT ON {{ this }} TO PUBLIC;
```

| Criteria | Points |
|---|---|
| Hook is defined at the project level (not model level) | 5 |
| `{{ this }}` correctly references the model being built | 5 |

---

#### Task 5.3 — Exposures (25 pts)
Create `models/dev/_exposures.yml` that defines 2 exposures:

1. **Revenue Dashboard** — depends on `fct_order_details` and `dim_customers`
2. **Inventory Report** — depends on `stg_products` and `fct_order_details`

Each exposure must include: `name`, `type`, `maturity`, `owner` (with name and email), `depends_on`, and `description`.

| Criteria | Points |
|---|---|
| Both exposures defined with correct syntax | 10 |
| `depends_on` correctly lists model refs | 5 |
| Owner information is filled in | 5 |
| Descriptions are meaningful (not placeholder text) | 5 |

---

#### Task 5.4 — Model Documentation (25 pts)
Create/update `models/stage/_schema.yml` and `models/dev/_schema.yml` to include:
- A `description` for every model
- A `description` for every column in `fct_order_details` and `dim_customers`

| Criteria | Points |
|---|---|
| All models have descriptions | 5 |
| All columns in `fct_order_details` documented | 10 |
| All columns in `dim_customers` documented | 5 |
| Descriptions are clear and helpful (not just the column name restated) | 5 |

---

#### Task 5.5 — Generate and Review Docs Site (15 pts)
1. Run `dbt docs generate`
2. Run `dbt docs serve`
3. Take screenshots showing:
   - The DAG (lineage graph) of your project
   - The documentation page for `fct_order_details`
   - The exposure showing dashboard dependencies

| Criteria | Points |
|---|---|
| Docs generate without errors | 5 |
| DAG screenshot shows correct lineage | 5 |
| Exposure visible in docs | 5 |

---

### Week 5 Total: **100 points**

---

## Week 6: Airflow Automation

### Learning Objectives
- Understand how Airflow DAGs orchestrate tasks
- Build a complete dbt pipeline DAG
- Add task dependencies, scheduling, and error handling

### Assignment Tasks

#### Task 6.1 — Airflow Concepts (10 pts)
Write a short document (`docs/airflow_overview.md`) answering:
1. What is a DAG?
2. What is the difference between a `BashOperator` and a `PythonOperator`?
3. What does the `schedule_interval` parameter control?
4. What is a sensor and when would you use one?

| Criteria | Points |
|---|---|
| All 4 questions answered correctly | 8 |
| Answers are in the student's own words | 2 |

---

#### Task 6.2 — Build the Pipeline DAG (45 pts)
Create `airflow/dags/dbt_pipeline.py` with the following task flow:

```
dbt_seed → dbt_test_sources → dbt_run_stage → dbt_test_stage → dbt_run_dev → dbt_test_dev
```

Requirements:
- Use `BashOperator` to run dbt commands via Docker
- Schedule to run daily at 6:00 AM UTC
- Set `catchup=False`
- Set appropriate `retries` (2) and `retry_delay` (5 minutes)
- Add `default_args` with owner set to the student's name

**Deliverable:** The DAG file.

| Criteria | Points |
|---|---|
| DAG has correct task dependencies (linear chain) | 10 |
| Uses `BashOperator` with correct dbt commands | 10 |
| Schedule is set to daily 6 AM UTC | 5 |
| `catchup=False` is set | 3 |
| `retries` and `retry_delay` configured | 5 |
| `default_args` properly defined | 2 |
| DAG appears in Airflow UI without import errors | 10 |

---

#### Task 6.3 — Trigger a Manual Run (20 pts)
1. Open the Airflow web UI (`localhost:8080`)
2. Trigger the DAG manually
3. Wait for it to complete
4. Take screenshots of:
   - The DAG graph view showing all tasks green
   - The log output of the `dbt_run_dev` task

| Criteria | Points |
|---|---|
| DAG triggered successfully | 5 |
| All tasks pass (green) | 10 |
| Log screenshot shows dbt output | 5 |

---

#### Task 6.4 — Error Handling: Failure Notification (15 pts)
Add an `on_failure_callback` to the DAG that:
- Logs the failure details (which task, when, error message)
- OR sends a notification (Slack webhook, email, or simple file write)

| Criteria | Points |
|---|---|
| Callback function is defined and referenced | 5 |
| Callback receives and uses the Airflow context (`context['task_instance']`) | 5 |
| Callback is tested by intentionally breaking a dbt model and showing the output | 5 |

---

#### Task 6.5 — Reflection Document (10 pts)
Write `docs/pipeline_retrospective.md` answering:
1. What would you change about the pipeline if this were production?
2. What additional monitoring would you add?
3. What was the hardest part of the entire 6-week program?

| Criteria | Points |
|---|---|
| Thoughtful answers showing understanding | 7 |
| At least one specific improvement idea | 3 |

---

### Week 6 Total: **100 points**

---

## Evaluation Summary

### Points Table

| Week | Topic | Points |
|---|---|---|
| 1 | Sources, Models, Seeds | 100 |
| 2 | Materializations | 100 |
| 3 | Data Tests | 100 |
| 4 | Macros & Packages | 100 |
| 5 | Hooks, Exposures, Docs | 100 |
| 6 | Airflow Automation | 100 |
| | **Total** | **600** |

### How to Evaluate

For each week:

1. **Code Review (60%):** Review their dbt models, tests, macros, and YAML files for correctness, style, and adherence to the two-layer architecture. Run `dbt run` and `dbt test` against their branch.

2. **Demo / Walkthrough (25%):** Each mentee should demo their work for 10–15 minutes, explaining:
   - What they built
   - Why they made certain decisions
   - What issues they encountered and how they solved them

3. **Written Deliverables (15%):** Review documentation files for accuracy, clarity, and completeness.

### Evaluation Checklist Per Mentee

```
Week ___  |  Mentee: _______________  |  Date: ___________

Code Review:
  [ ] All required files created
  [ ] dbt run succeeds
  [ ] dbt test output reviewed
  [ ] Code follows CTE style conventions
  [ ] Models reference correct layers (STAGE ← RAW, DEV ← STAGE)

Demo:
  [ ] Student can explain what each model does
  [ ] Student can explain WHY (not just HOW)
  [ ] Student handled questions confidently

Written:
  [ ] Documentation files submitted
  [ ] Content is original and accurate

Score: _____ / 100
Notes: _____________________________________________
```

---

## Quick Reference: dbt Commands for Mentees

```bash
# Start the stack
docker compose up -d

# Load seed data
docker compose run --rm dbt seed --profiles-dir .

# Run all models
docker compose run --rm dbt run --profiles-dir .

# Run specific model
docker compose run --rm dbt run --select fct_order_details --profiles-dir .

# Run tests
docker compose run --rm dbt test --profiles-dir .

# Run snapshot
docker compose run --rm dbt snapshot --profiles-dir .

# Generate docs
docker compose run --rm dbt docs generate --profiles-dir .

# Full refresh (incremental)
docker compose run --rm dbt run --full-refresh --profiles-dir .

# Install packages
docker compose run --rm dbt deps --profiles-dir .
```
