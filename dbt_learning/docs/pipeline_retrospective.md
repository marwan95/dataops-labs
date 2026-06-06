# Pipeline Retrospective — Week 6

## 1. What would you change if this were production?

**Separate environments.** The current setup uses a single `dev` target that writes to the same
database the seeds land in. In production I would add a `prod` target pointing to a dedicated
read-optimised database (or a separate schema with stricter access control) and only promote
models after they pass all tests in a staging environment.

**Replace `dbt seed` with a proper ingestion layer.** Seeds are CSV files committed to git —
fine for a course but not for production data that changes daily. In production I would replace
the seed step with an ELT tool (Fivetran, Airbyte, or a custom Python ingestor) that pulls from
the source API or database directly into the RAW schema. The Airflow DAG would then call that
ingestion job rather than `dbt seed`.

**Alerting and observability.** The current `on_failure_callback` writes to a local log file.
In production I would send failures to PagerDuty or Slack so the on-call engineer is notified
immediately. I would also instrument dbt Cloud or Elementary for model-level observability —
tracking row counts, freshness, and schema changes over time.

**Idempotent, parameterised runs.** The DAG should accept a `logical_date` parameter so that a
specific date range can be re-run without re-processing the entire history. The incremental model
already supports this via the look-back window, but the seed and test steps should be idempotent
too.

**Secret management.** Postgres credentials are currently in a `.env` file committed alongside
the project. In production these would live in Airflow Connections or a secrets backend (AWS
Secrets Manager, HashiCorp Vault) so that credentials are never stored in version control.

---

## 2. What additional monitoring would you add?

**Data freshness checks.** Add a `freshness` block to `sources.yml` so dbt alerts if raw tables
have not been updated within the expected window. Combined with a `SourceFreshnessSensor`, the
pipeline could abort early rather than running expensive transformations on stale data.

**Row count assertions.** Use dbt's `dbt_utils.expression_is_true` or a custom singular test to
assert that `fct_order_details` has at least N rows after each run. A sudden drop in row count
usually indicates an upstream ingestion failure.

**Pipeline run metrics in Grafana.** Export Airflow task durations and success/failure rates
into a Prometheus/Grafana stack. Visualise average run time per task so that regressions
(a suddenly slow `dbt run --select dev`) are caught before they breach an SLA.

**Great Expectations or Elementary integration.** Add a post-run step that generates a data
quality report and publishes it to a shared Slack channel or wiki page, giving stakeholders
visibility into exactly which tests passed and failed each day.

---

## 3. What was the hardest part of the 6-week program?

The hardest part was **debugging the incremental model logic in Week 2**. Understanding exactly
when `is_incremental()` evaluates to `True` versus `False`, and making the 3-day look-back
window robust against re-runs and back-fills, required careful reasoning about dbt's execution
model. It is easy to write a look-back filter that silently drops valid rows during a full
refresh or misses rows when the scheduler is paused for a day.

The second hardest part was **getting Jinja right in Week 4**. Jinja renders before SQL executes,
which means errors in a macro can be cryptic — dbt reports a compilation error with a line number
in the rendered SQL, not the original template. Learning to use `dbt compile` to inspect the
generated SQL before running it was the key insight that made macro debugging manageable.

Overall, the program gave me a solid end-to-end mental model: raw CSV → sources → staging →
facts/dimensions → tests → documentation → automation. Each week built directly on the last,
which made the accumulation of concepts feel natural rather than overwhelming.
