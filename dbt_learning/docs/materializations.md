# Materializations in dbt

## 1. Table vs View in PostgreSQL

A **view** is essentially a saved SQL query. When you query a view, PostgreSQL runs the underlying SELECT statement at that moment and returns the result. Nothing is stored on disk beyond the query definition itself. This means the data is always fresh, but every query pays the full computation cost.

A **table** is the opposite — PostgreSQL actually executes the query once and stores the resulting rows on disk as a physical object. When you query a table, you read pre-computed data directly from storage. This makes reads very fast, but the data only reflects what was present when the table was last built or refreshed.

The key trade-off: views are cheap to create and always up to date, but slow to query on large datasets. Tables are fast to query but require explicit refreshing when source data changes.

## 2. View in STAGE vs Table in DEV

The **STAGE layer** uses `view` materialization because these models are doing lightweight work — trimming strings, casting types, standardising casing. The views always reflect the latest seed or source data without needing to be rebuilt, and they are rarely queried by end users or dashboards directly.

The **DEV layer** uses `table` materialization because these are the final models that analysts and dashboards query repeatedly. A fact table like `fct_order_details` joins four sources and performs arithmetic on every row — re-running that logic on every dashboard refresh would be expensive and slow. Materializing it as a table means the heavy lifting happens once during `dbt run`, and every subsequent read is just a fast scan against stored rows.

## 3. What Problem Does Incremental Solve?

Incremental materialization solves the problem of unnecessary full rebuilds on large, append-only datasets.

In our ecommerce project, `fct_order_details` will grow every day as new orders come in. Without incremental, every `dbt run` drops the entire table and reprocesses all 300+ rows — which is manageable today but becomes a serious performance and cost problem when the table reaches millions of rows.

With incremental, dbt checks the maximum `order_date` already in the table and only processes rows from the last 3 days. New orders are merged into the existing table rather than rebuilding it from scratch. The first run still does a full build, but every run after that touches only the new data. This is exactly how production data pipelines at scale handle large fact tables — process only what changed, not everything.
