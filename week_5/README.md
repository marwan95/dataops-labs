# Week 5: Hooks, Exposures, and Documentation

Welcome to Week 5 of the DataOps & dbt Mentorship Program! This week we make our project production-ready: we automate database housekeeping with hooks, declare who consumes our models with exposures, document every model and column, and publish a browsable docs site.

---

## ✅ Prerequisites

Before starting Week 5, make sure you have completed **all of Week 4**:

- [ ] `packages.yml` with dbt-utils installed
- [ ] `macros/calculate_revenue.sql` and `macros/convert_currency.sql`
- [ ] `macros/macros.yml` documentation
- [ ] `models/dev/fct_monthly_revenue.sql` pivot table
- [ ] `fct_order_details.sql` using both macros and `generate_surrogate_key`

---

## 📖 Lesson Overview

### What are Hooks?

Hooks are SQL statements dbt runs automatically **before or after** a model builds. They let you automate DBA tasks without a separate script:

| Hook | Runs | Common uses |
|---|---|---|
| `pre_hook` | Before model builds | Drop temp tables, set session variables |
| `post_hook` | After model builds | Create indexes, grant permissions, audit logs |

Hooks can be defined at **model level** (in the model's `config()`) or at **project level** (in `dbt_project.yml` under `+post_hook`).

### What are Exposures?

Exposures declare **what downstream systems depend on your dbt models** — dashboards, notebooks, ML pipelines, APIs. They give you:

- **Lineage visibility**: the docs DAG shows who consumes which models
- **Impact analysis**: "if I change `fct_order_details`, which dashboards break?"
- **Documentation**: stakeholders can see what data powers their reports

### Why Document Everything?

dbt generates a full data dictionary from your YAML descriptions. When you run `dbt docs generate && dbt docs serve`, every team member gets a searchable portal with lineage graphs, column descriptions, and test coverage — no wiki required.

---

## 📝 Assignment Tasks

### Task 5.1 — Post-Hook: Create Indexes (25 pts)

Add `post_hook` to the config blocks of `fct_order_details.sql` and `dim_customers.sql` so that indexes are automatically created (or skipped if they already exist) after each build.

**`models/dev/fct_order_details.sql`:**
```sql
{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_fct_order_details_order_date ON {{ this }} (order_date)",
            "CREATE INDEX IF NOT EXISTS idx_fct_order_details_customer_id ON {{ this }} (customer_id)"
        ]
    )
}}
```

**`models/dev/dim_customers.sql`:**
```sql
{{
    config(
        materialized='table',
        post_hook="CREATE INDEX IF NOT EXISTS idx_dim_customers_country ON {{ this }} (country)"
    )
}}
```

> **Key concept:** `IF NOT EXISTS` makes the hook **idempotent** — safe to run on every build without errors.

**Verify your indexes exist:**
```sql
SELECT indexname, tablename, indexdef
FROM pg_indexes
WHERE schemaname = 'DEV'
ORDER BY tablename;
```

**Deliverable:** Updated config blocks in both model files.

| Criteria | Points |
|---|---|
| `post_hook` in `fct_order_details` config | 5 |
| `IF NOT EXISTS` used for idempotency | 5 |
| `idx_fct_order_details_order_date` defined | 5 |
| `post_hook` in `dim_customers` config | 5 |
| `idx_dim_customers_country` defined | 5 |

---

### Task 5.2 — Post-Hook: Grant Permissions (10 pts)

Add a project-level `+post_hook` under the `dev` layer in `dbt_project.yml` that grants SELECT to PUBLIC on every DEV model automatically.

**`dbt_project.yml`:**
```yaml
models:
  dbt_learning:
    stage:
      +schema: STAGE
      +materialized: view

    dev:
      +schema: DEV
      +materialized: table
      +post_hook: "GRANT SELECT ON {{ this }} TO PUBLIC"
```

> **Why project-level?** Defining the hook here means every new DEV model gets the grant automatically — no need to remember to add it to each file.

**Deliverable:** Updated `dbt_project.yml`.

| Criteria | Points |
|---|---|
| `+post_hook` defined under `dev` in `dbt_project.yml` | 5 |
| Hook uses `GRANT SELECT ON {{ this }} TO PUBLIC` | 5 |

---

### Task 5.3 — Exposures (25 pts)

Create `models/dev/exposures.yml` declaring two downstream consumers of your dbt models.

**Required exposures:**

| Exposure | Type | Depends on |
|---|---|---|
| `revenue_dashboard` | dashboard | `fct_order_details`, `dim_customers` |
| `inventory_report` | analysis | `stg_products`, `fct_order_details` |

**Template:**
```yaml
version: 2

exposures:
  - name: revenue_dashboard
    type: dashboard
    maturity: high
    description: "..."
    owner:
      name: Your Name
      email: your@email.com
    depends_on:
      - ref('fct_order_details')
      - ref('dim_customers')

  - name: inventory_report
    type: analysis
    maturity: medium
    description: "..."
    owner:
      name: Your Name
      email: your@email.com
    depends_on:
      - ref('stg_products')
      - ref('fct_order_details')
```

**Deliverable:** `models/dev/exposures.yml` with both exposures fully filled in.

| Criteria | Points |
|---|---|
| `exposures.yml` file exists | 5 |
| `revenue_dashboard` defined | 5 |
| `inventory_report` defined | 5 |
| `depends_on` correctly lists refs | 5 |
| Owner name and email filled in | 5 |

---

### Task 5.4 — Model Documentation (25 pts)

Add descriptions to every model and key columns across two schema files.

**Part A — `models/stage/schema.yml`:**  
All 5 staging models should already have `description:` entries. Verify each model has a meaningful description (not placeholder text).

**Part B — `models/dev/schema.yml` (new file):**  
Create this file and document all four DEV models:

```yaml
version: 2

models:
  - name: fct_order_details
    description: "..."
    columns:
      - name: order_detail_sk
        description: "..."
      - name: order_item_id
        description: "..."
      # ... all columns including net_amount, total_amount, net_amount_usd

  - name: dim_customers
    description: "..."
    columns:
      - name: customer_id
        description: "..."
      # ... all columns including total_orders, total_spent

  - name: fct_monthly_revenue
    description: "..."

  - name: quarantine_orders
    description: "..."
    columns:
      - name: failure_reason
        description: "..."
```

**Deliverable:** Updated `models/stage/schema.yml` + new `models/dev/schema.yml`.

| Criteria | Points |
|---|---|
| `stage/schema.yml` has model descriptions | 5 |
| `dev/schema.yml` file exists | 5 |
| `fct_order_details` columns documented (incl. `order_detail_sk`, `net_amount`) | 10 |
| `dim_customers` columns documented (incl. `total_orders`, `total_spent`) | 5 |

---

### Task 5.5 — Generate and Review Docs Site (15 pts)

Generate the dbt documentation site and take three screenshots.

**Commands:**
```bash
docker compose run dbt docs generate --profiles-dir .
docker compose run dbt docs serve --profiles-dir .
# Then open http://localhost:8080 in your browser
```

**Required screenshots** (save in `docs/`):

| File | What to capture |
|---|---|
| `screenshot_dag.png` | Full project lineage DAG showing all models and exposures |
| `screenshot_fct_order_details_docs.png` | The fct_order_details model documentation page |
| `screenshot_exposure.png` | The revenue_dashboard exposure page |

**Deliverable:** Three PNG screenshots + `target/catalog.json` generated.

| Criteria | Points |
|---|---|
| `dbt docs generate` runs without errors (catalog.json created) | 5 |
| DAG screenshot shows correct lineage | 5 |
| Exposure visible in docs | 5 |

---

### Week 5 Total: **100 points**

---

## 🔧 dbt Commands Reference

```bash
# Run all models (triggers post_hooks)
docker compose run dbt run --profiles-dir .

# Run a specific model
docker compose run dbt run --select fct_order_details --full-refresh --profiles-dir .

# Generate documentation
docker compose run dbt docs generate --profiles-dir .

# Serve documentation site (open http://localhost:8080)
docker compose run --service-ports dbt docs serve --profiles-dir .
```

---

## 📂 Expected File Structure After Week 5

```
dbt_learning/
├── dbt_project.yml                       ← UPDATED (+post_hook under dev)
├── models/
│   ├── stage/
│   │   ├── sources.yml
│   │   └── schema.yml                    ← UPDATED (model descriptions)
│   └── dev/
│       ├── fct_order_details.sql         ← UPDATED (post_hook indexes)
│       ├── dim_customers.sql             ← UPDATED (post_hook index)
│       ├── fct_monthly_revenue.sql
│       ├── quarantine_orders.sql
│       ├── exposures.yml                 ← NEW
│       └── schema.yml                    ← NEW (full column docs)
├── docs/
│   ├── materializations.md
│   ├── data_quality_report.md
│   ├── screenshot_dag.png                ← NEW (manual)
│   ├── screenshot_fct_order_details_docs.png ← NEW (manual)
│   └── screenshot_exposure.png           ← NEW (manual)
└── target/
    ├── run_results.json
    └── catalog.json                      ← NEW (dbt docs generate)
```

Good luck! 🚀
