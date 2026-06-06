# Week 6: Airflow Automation

Welcome to Week 6 — the final week of the DataOps & dbt Mentorship Program! This week we automate the entire dbt pipeline using Apache Airflow, add error handling, and reflect on the full 6-week journey.

---

## ✅ Prerequisites

Before starting Week 6, make sure you have completed **all of Week 5**:

- [ ] Post-hook indexes in `fct_order_details` and `dim_customers`
- [ ] `GRANT SELECT` post-hook in `dbt_project.yml`
- [ ] `models/dev/exposures.yml` with two exposures
- [ ] `models/dev/schema.yml` with full column documentation
- [ ] `dbt docs generate` ran successfully

---

## 📖 Lesson Overview

### What is Apache Airflow?

Airflow is a workflow orchestration platform. You write your pipelines as Python code (DAGs), and Airflow handles scheduling, retries, dependency management, and monitoring via a web UI.

```
Airflow Scheduler  →  reads DAG files  →  runs tasks on schedule
Airflow Webserver  →  serves the UI    →  lets you monitor / trigger runs
```

### Why Orchestrate dbt with Airflow?

Running `dbt run` manually is fine for development. In production you need:

| Need | Solution |
|---|---|
| Run automatically every day | `schedule_interval="0 6 * * *"` |
| Retry on transient failures | `retries=2, retry_delay=timedelta(minutes=5)` |
| Catch failures and alert | `on_failure_callback` |
| See what ran and when | Airflow UI / task logs |
| Stop if data quality fails | Linear dependency chain with `dbt test` steps |

### The Pipeline Architecture

```
dbt_seed → dbt_test_sources → dbt_run_stage → dbt_test_stage → dbt_run_dev → dbt_test_dev
```

Each `>>` means "must complete successfully before the next task starts." If `dbt_test_sources` finds bad rows, the whole pipeline stops — preventing bad data from propagating to DEV models.

---

## 📝 Assignment Tasks

### Task 6.1 — Airflow Concepts (10 pts)

Create `dbt_learning/docs/airflow_overview.md` answering these four questions **in your own words**:

1. **What is a DAG?**
2. **What is the difference between a `BashOperator` and a `PythonOperator`?**
3. **What does the `schedule_interval` parameter control?**
4. **What is a sensor and when would you use one?**

**Deliverable:** `dbt_learning/docs/airflow_overview.md`

| Criteria | Points |
|---|---|
| DAG concept explained | 2 |
| BashOperator vs PythonOperator covered | 2 |
| schedule_interval explained | 2 |
| Sensor explained with use case | 2 |
| Answers in own words (not copy-pasted) | 2 |

---

### Task 6.2 — Build the Pipeline DAG (45 pts)

Create `airflow/dags/dbt_pipeline.py` — a production-quality Airflow DAG that runs the full dbt pipeline daily.

**Required task chain:**
```
dbt_seed → dbt_test_sources → dbt_run_stage → dbt_test_stage → dbt_run_dev → dbt_test_dev
```

**DAG configuration requirements:**
- Operator: `BashOperator`
- `schedule_interval="0 6 * * *"` (daily at 6 AM UTC)
- `catchup=False`
- `retries=2`, `retry_delay=timedelta(minutes=5)`
- `owner` set in `default_args`
- dbt commands run with `--profiles-dir /opt/airflow/dbt --target dev`

**dbt commands:**
```python
DBT_DIR = "/opt/airflow/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"
DBT_FLAGS = f"--profiles-dir {DBT_DIR} --target dev"

# Task commands:
# dbt_seed          → f"{DBT_CMD} seed {DBT_FLAGS}"
# dbt_test_sources  → f'{DBT_CMD} test --select "source:*" {DBT_FLAGS}'
# dbt_run_stage     → f"{DBT_CMD} run --select stage {DBT_FLAGS}"
# dbt_test_stage    → f"{DBT_CMD} test --select stage {DBT_FLAGS}"
# dbt_run_dev       → f"{DBT_CMD} run --select dev {DBT_FLAGS}"
# dbt_test_dev      → f"{DBT_CMD} test --select dev {DBT_FLAGS}"
```

**Dependency chain:**
```python
dbt_seed >> dbt_test_sources >> dbt_run_stage >> dbt_test_stage >> dbt_run_dev >> dbt_test_dev
```

**Test your DAG:**
```bash
# Check for import errors
docker compose exec airflow-scheduler airflow dags list-import-errors

# Verify DAG is registered
docker compose exec airflow-scheduler airflow dags list | grep dbt_pipeline

# Test a single task
docker compose exec airflow-scheduler airflow tasks test dbt_pipeline dbt_seed 2024-01-01
```

**Deliverable:** `airflow/dags/dbt_pipeline.py`

| Criteria | Points |
|---|---|
| BashOperator with correct dbt commands | 5 |
| Schedule set to `0 6 * * *` | 5 |
| `catchup=False` | 3 |
| `retries=2` and `retry_delay` set | 6 |
| All 6 task IDs defined | 8 |
| Linear dependency chain with `>>` | 5 |
| `on_failure_callback` in `default_args` | 3 |
| Valid Python syntax | 5 |
| DAG appears in UI without import errors | 5 |

---

### Task 6.3 — Infrastructure Setup + Manual Run (20 pts)

**Step 1 — Create `Dockerfile.airflow`** at the project root:
```dockerfile
FROM apache/airflow:2.10.5
USER airflow
RUN pip install --no-cache-dir dbt-postgres==1.9.*
```

**Step 2 — Update `docker-compose.yml`** to use the new image and mount dbt:
```yaml
x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile.airflow
  # ...
  environment:
    # ... existing vars ...
    POSTGRES_HOST: postgres
    POSTGRES_PORT: "5432"
  volumes:
    - ./airflow/dags:/opt/airflow/dags
    - ./airflow/logs:/opt/airflow/logs
    - ./airflow/plugins:/opt/airflow/plugins
    - ./dbt_learning:/opt/airflow/dbt      # ← NEW
```

**Step 3 — Start Airflow:**
```bash
docker compose up -d airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

**Step 4 — Trigger a manual run:**
1. Open `http://localhost:8080` (admin / admin)
2. Find `dbt_pipeline`, unpause it, click **▶ Trigger DAG**
3. Watch all 6 tasks turn dark green

**Step 5 — Take screenshots** and save in `week_6/`:
- `screenshot_graph.png` — Graph view with all tasks green
- `screenshot_dbt_run_dev_log.png` — Log for the `dbt_run_dev` task

| Criteria | Points |
|---|---|
| `Dockerfile.airflow` exists with dbt-postgres | 5 |
| `docker-compose.yml` uses Dockerfile.airflow | 5 |
| `dbt_learning` volume mounted in Airflow | 5 |
| Screenshots show successful run | 5 |

---

### Task 6.4 — Error Handling: Failure Callback (15 pts)

Add an `on_failure_callback` to the DAG's `default_args` that fires when any task fails after all retries are exhausted.

**Implementation:**
```python
def on_failure_callback(context):
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    execution_date = context["execution_date"]
    log_msg = (
        f"[FAILURE] DAG={dag_id} | Task={task_id} | "
        f"ExecutionDate={execution_date}\n"
    )
    with open("/opt/airflow/logs/dbt_failure.log", "a") as f:
        f.write(log_msg)
    print(log_msg)

default_args = {
    ...
    "on_failure_callback": on_failure_callback,
}
```

**Test it:**
1. Temporarily set `retries=0`
2. Break a stage model (introduce a SQL syntax error)
3. Run: `docker compose exec airflow-scheduler airflow dags test dbt_pipeline 2024-01-01`
4. Check `/opt/airflow/logs/dbt_failure.log` for the failure entry
5. Restore the model and `retries=2`

> ⚠️ `airflow tasks test` skips callbacks. You must use `airflow dags test` to trigger the callback.

| Criteria | Points |
|---|---|
| `on_failure_callback` function defined | 5 |
| Callback accesses `context["task_instance"]` | 5 |
| Callback attached in `default_args` | 5 |

---

### Task 6.5 — Reflection Document (10 pts)

Create `dbt_learning/docs/pipeline_retrospective.md` answering three questions:

1. **What would you change about the pipeline if this were production?**
2. **What additional monitoring would you add?**
3. **What was the hardest part of the entire 6-week program?**

**Deliverable:** `dbt_learning/docs/pipeline_retrospective.md` (at least 100 words)

| Criteria | Points |
|---|---|
| File exists | 3 |
| At least 100 words | 4 |
| Includes at least one specific production improvement | 3 |

---

### Week 6 Total: **100 points**

---

## 🔧 Commands Reference

```bash
# Build and start all services
docker compose up -d --build

# Start only Airflow (postgres must be running)
docker compose up -d airflow-webserver airflow-scheduler

# Check DAG for import errors
docker compose exec airflow-scheduler airflow dags list-import-errors

# List all DAGs
docker compose exec airflow-scheduler airflow dags list

# Test a single task (no callbacks)
docker compose exec airflow-scheduler airflow tasks test dbt_pipeline dbt_seed 2024-01-01

# Test full DAG run (triggers callbacks on failure)
docker compose exec airflow-scheduler airflow dags test dbt_pipeline 2024-01-01

# View Airflow UI
# http://localhost:8080  (admin / admin)
```

---

## 📂 Expected File Structure After Week 6

```
dataops-labs/
├── Dockerfile.airflow                          ← NEW
├── docker-compose.yml                          ← UPDATED
├── airflow/
│   └── dags/
│       └── dbt_pipeline.py                     ← NEW
├── dbt_learning/
│   └── docs/
│       ├── airflow_overview.md                 ← NEW
│       └── pipeline_retrospective.md           ← NEW
└── week_6/
    ├── README.md
    ├── screenshot_graph.png                    ← NEW (manual)
    └── screenshot_dbt_run_dev_log.png          ← NEW (manual)
```

Congratulations on completing the 6-week DataOps & dbt Mentorship Program! 🎉
