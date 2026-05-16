# Week 4: Macros and Packages

Welcome to Week 4 of the DataOps & dbt Mentorship Program! This week, we'll learn how to write reusable code using Jinja templating, custom macros, and community packages — the tools that separate production-grade dbt from amateur SQL.

---

## ✅ Prerequisites

Before starting Week 4, make sure you have completed **all of Week 3**:

- [ ] `models/stage/schema.yml` with generic tests
- [ ] 5 custom singular tests in `tests/`
- [ ] `models/dev/quarantine_orders.sql` working
- [ ] `docs/data_quality_report.md` written

---

## 📖 Lesson Overview

### What is Jinja?

Jinja is a templating language that dbt uses to make SQL dynamic. Before dbt runs your model, it renders the Jinja into plain SQL. This means you can use variables, loops, and conditionals to generate SQL programmatically.

```sql
-- Jinja template                    -- Compiled SQL
{% set year = 2024 %}           →    -- (nothing — just sets a variable)
select * from orders            →    select * from orders
where year = {{ year }}         →    where year = 2024
```

### What is a Macro?

A macro is a reusable function written in Jinja + SQL. Instead of copy-pasting the same revenue formula across 5 models, you write it once as a macro and call it everywhere — following the DRY principle.

```
macros/calculate_revenue.sql     ← define once
models/dev/fct_order_details.sql ← {{ calculate_revenue(...) }}
models/dev/fct_monthly_revenue.sql ← {{ calculate_revenue(...) }}
```

### What are Packages?

Packages are community-built collections of macros and models you can install into your project. `dbt-utils` (from dbt Labs) is the most popular package and includes:

| Macro | What it does |
|---|---|
| `generate_surrogate_key()` | Creates a hashed primary key from multiple columns |
| `star()` | Selects all columns except a list of exclusions |
| `date_spine()` | Generates a continuous sequence of dates |
| `get_column_values()` | Returns unique values from a column at compile time |

---

## 📝 Assignment Tasks

### Task 4.1 — Jinja Basics: Monthly Revenue Pivot (15 pts)

Create `models/dev/fct_monthly_revenue.sql` — a pivot table showing revenue by store for every month of 2024.

**Requirements:**
1. Use `{% set target_year = 2024 %}` to define the year variable
2. Define the months list using `{% set months = [...] %}`
3. Use `{% for month_num, month_name in months %}` to generate 12 revenue columns
4. Output: one row per `store_id` with columns `jan_revenue`, `feb_revenue`, ..., `dec_revenue`

**Month definitions:**
```sql
{% set months = [
    ('01', 'jan'), ('02', 'feb'), ('03', 'mar'), ('04', 'apr'),
    ('05', 'may'), ('06', 'jun'), ('07', 'jul'), ('08', 'aug'),
    ('09', 'sep'), ('10', 'oct'), ('11', 'nov'), ('12', 'dec')
] %}
```

**💡 Pivot pattern:**
```sql
select
    store_id,
    {% for month_num, month_name in months %}
    sum(case when extract(month from order_date) = {{ month_num | int }}
             then net_revenue else 0 end) as {{ month_name }}_revenue
    {%- if not loop.last %},{% endif %}
    {% endfor %}
from revenue_base
group by store_id
```

**Test your work:**
```bash
dbt compile --select fct_monthly_revenue --profiles-dir .
dbt run --select fct_monthly_revenue --profiles-dir .
```

**Deliverable:** `models/dev/fct_monthly_revenue.sql`

| Criteria | Points |
|---|---|
| `{% set %}` variable used | 3 |
| `{% for %}` loop generates 12 month columns | 4 |
| Monthly revenue columns generated correctly | 4 |
| Model file exists | 4 |

---

### Task 4.2 — Currency Converter Macro (30 pts)

Create `macros/convert_currency.sql` — a macro that converts any monetary amount from its source currency to a target currency.

**Macro signature:**
```sql
{% macro convert_currency(amount_column, currency_column, target_currency='USD') %}
```

**Exchange rates to USD:**
| Currency | Rate to USD |
|---|---|
| USD | 1.00 |
| OMR | 2.60 |
| EUR | 1.08 |

**Verification:**
- `4.90 OMR × 2.60 = 12.74 USD` (P-033, Arabic Keyboard Cover)
- `2.50 OMR × 2.60 = 6.50 USD` (P-034, Oman Flag Mouse Pad)

**Use in `fct_order_details.sql`:**
```sql
{{ convert_currency('net_amount', 'currency', 'USD') }} as net_amount_usd
```

**Deliverable:** `macros/convert_currency.sql` + applied in at least one model.

| Criteria | Points |
|---|---|
| Macro file exists | 5 |
| Accepts 3 parameters (amount, currency, target) | 5 |
| Handles USD, OMR, EUR | 10 |
| Applied in `fct_order_details` | 10 |

---

### Task 4.3 — Reusable Revenue Macro (20 pts)

Create `macros/calculate_revenue.sql` — a macro that standardises the net revenue formula so it's defined in exactly one place.

**Macro signature:**
```sql
{% macro calculate_revenue(quantity, unit_price, discount_pct) %}
    ({{ quantity }} * {{ unit_price }} * (1 - {{ discount_pct }} / 100.0))::numeric(12,2)
{% endmacro %}
```

**Replace hardcoded math in `fct_order_details.sql`:**
```sql
-- Before
(oi.quantity * oi.unit_price * (1 - oi.discount_pct / 100.0))::numeric(12,2) as net_amount

-- After
{{ calculate_revenue('oi.quantity', 'oi.unit_price', 'oi.discount_pct') }} as net_amount
```

**Deliverable:** `macros/calculate_revenue.sql` + applied in `fct_order_details`.

| Criteria | Points |
|---|---|
| Macro file exists | 5 |
| Correct formula (quantity × unit_price × discount) | 5 |
| Applied in `fct_order_details` replacing hardcoded math | 10 |

---

### Task 4.4 — Install and Use dbt-utils Package (20 pts)

**Step 1 — Create `packages.yml`** at the project root (same level as `dbt_project.yml`):

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

**Step 2 — Install the package:**
```bash
dbt deps --profiles-dir .
```

**Step 3 — Use `generate_surrogate_key` in `fct_order_details`:**
```sql
{{ dbt_utils.generate_surrogate_key(['oi.order_id', 'oi.order_item_id']) }} as order_detail_sk,
```

**Step 4 — Use one more dbt-utils macro** in any model. Options:

Option A — `star()` (exclude specific columns):
```sql
select {{ dbt_utils.star(from=ref('stg_orders'), except=["currency"]) }}
from {{ ref('stg_orders') }}
```

Option B — `date_spine()` (generate a date dimension):
```sql
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2024-01-01' as date)",
    end_date="cast('2024-12-31' as date)"
) }}
```

Option C — `get_column_values()` (dynamic column lists at compile time):
```sql
{% set categories = dbt_utils.get_column_values(
    table=ref('stg_products'),
    column='category'
) %}
```

**Deliverable:** `packages.yml` + both macros used in models.

| Criteria | Points |
|---|---|
| `packages.yml` exists with dbt-utils declared | 5 |
| `dbt deps` installs successfully | 5 |
| `generate_surrogate_key` used in `fct_order_details` | 5 |
| Second dbt-utils macro used in any model | 5 |

---

### Task 4.5 — Macro Documentation (15 pts)

Create `macros/macros.yml` documenting both of your custom macros with descriptions and argument specifications.

**Required structure:**
```yaml
version: 2

macros:
  - name: convert_currency
    description: "What this macro does..."
    arguments:
      - name: amount_column
        type: string
        description: "..."
      - name: currency_column
        type: string
        description: "..."
      - name: target_currency
        type: string
        description: "..."

  - name: calculate_revenue
    description: "What this macro does..."
    arguments:
      - name: quantity
        type: string
        description: "..."
      - name: unit_price
        type: string
        description: "..."
      - name: discount_pct
        type: string
        description: "..."
```

**Deliverable:** `macros/macros.yml` with both macros fully documented.

| Criteria | Points |
|---|---|
| `macros.yml` file exists | 5 |
| `convert_currency` documented with arguments | 5 |
| `calculate_revenue` documented with arguments | 5 |

---

### Week 4 Total: **100 points**

---

## 🔧 dbt Commands Reference

```bash
# Install packages
dbt deps --profiles-dir .

# Compile a model (check Jinja renders correctly without running)
dbt compile --select fct_monthly_revenue --profiles-dir .

# Run a specific model
dbt run --select fct_order_details --full-refresh --profiles-dir .

# Run all models
dbt run --profiles-dir .

# Generate and serve documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

---

## 📂 Expected File Structure After Week 4

```
dbt_learning/
├── packages.yml                          ← NEW
├── dbt_packages/
│   └── dbt_utils/                        ← auto-created by dbt deps
├── macros/
│   ├── calculate_revenue.sql             ← NEW
│   ├── convert_currency.sql              ← NEW
│   ├── macros.yml                        ← NEW
│   └── generate_schema_name.sql
├── models/
│   ├── stage/
│   │   ├── sources.yml
│   │   ├── schema.yml
│   │   └── stg_*.sql
│   └── dev/
│       ├── fct_order_details.sql         ← UPDATED (macros + surrogate key)
│       ├── fct_monthly_revenue.sql       ← NEW
│       ├── dim_customers.sql
│       └── quarantine_orders.sql
├── tests/
├── snapshots/
└── docs/
```

Good luck! 🚀
