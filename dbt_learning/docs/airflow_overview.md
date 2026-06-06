# Airflow Overview — Week 6 Concepts

## 1. What is a DAG?

A DAG (Directed Acyclic Graph) is Airflow's core abstraction for a workflow. It is a collection
of tasks arranged so that each task can only depend on tasks that come before it — there are no
cycles. "Directed" means each edge has a direction (task A must finish before task B starts).
"Acyclic" means you can never follow the arrows back to where you started.

In practice, a DAG defines:
- **What** runs (tasks — BashOperators, PythonOperators, sensors, etc.)
- **When** it runs (the `schedule_interval`)
- **In what order** tasks run (the dependency chain defined by `>>`)

Every Python file in the `dags/` folder that creates a `DAG` object is automatically picked up
by the Airflow scheduler. When we write `dbt_seed >> dbt_run_stage`, we are telling Airflow
that `dbt_seed` must succeed before `dbt_run_stage` is allowed to start.

---

## 2. BashOperator vs PythonOperator

| | BashOperator | PythonOperator |
|---|---|---|
| **What it runs** | A shell command string | A Python callable (function) |
| **Best for** | CLI tools (dbt, spark-submit, scripts) | Python logic, API calls, data manipulation |
| **How to pass data** | Via environment variables or bash substitution | Via `op_kwargs` or XComs |
| **Error detection** | Non-zero exit code = failure | Uncaught exception = failure |

**BashOperator** is ideal for our dbt pipeline because dbt is a command-line tool. We simply
pass `dbt run --select stage --profiles-dir /opt/airflow/dbt` as the `bash_command` and Airflow
handles retries, logging, and status tracking for us.

**PythonOperator** is better when you need to write custom Python — for example, calling an
external API to fetch data, validating a DataFrame with pandas, or sending a Slack notification
with rich formatting that requires Python logic rather than a shell script.

---

## 3. What does `schedule_interval` control?

`schedule_interval` tells Airflow **how often to automatically trigger the DAG**. It accepts:

- **Cron expressions**: `"0 6 * * *"` = every day at 06:00 UTC
- **Presets**: `"@daily"`, `"@hourly"`, `"@weekly"`, `"@monthly"`
- **`timedelta` objects**: `timedelta(hours=6)` = every 6 hours
- **`None`**: DAG is only triggered manually

In our pipeline we use `"0 6 * * *"` (daily at 6 AM UTC) so that fresh seed data is loaded
and all models are rebuilt once a day before business hours. Combined with `catchup=False`, Airflow
will not retroactively run the DAG for every day between `start_date` and today — it will only
trigger going forward from the next scheduled time.

---

## 4. What is a Sensor and when would you use one?

A **sensor** is a special type of operator that **waits for a condition to become true** before
allowing downstream tasks to proceed. It polls repeatedly (using a `poke_interval`) until the
condition is met or a `timeout` is reached.

Common sensors in Airflow:

| Sensor | Waits for |
|---|---|
| `FileSensor` | A file to appear in a directory or S3 path |
| `ExternalTaskSensor` | A task in another DAG to complete |
| `HttpSensor` | An HTTP endpoint to return a specific response |
| `SqlSensor` | A SQL query to return at least one row |
| `S3KeySensor` | An S3 object to exist |

**When to use a sensor:** Whenever your pipeline depends on something external that arrives at
an unpredictable time. For example, if a vendor dumps a CSV file into S3 every morning but the
exact time varies, you would use a `FileSensor` (or `S3KeySensor`) to wait for the file before
triggering the dbt seed step. This avoids hardcoding a generous sleep and failing when the file
is late. In our pipeline we could add a sensor before `dbt_seed` to wait for the raw data export
to land in the database before running.
