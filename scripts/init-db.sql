-- ══════════════════════════════════════════════════════════════
-- Bootstrap script — runs once when the postgres container
-- is created for the first time.
-- ══════════════════════════════════════════════════════════════

-- Airflow metadata database
CREATE DATABASE airflow_db;

-- Grant full access to the dataops user
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO dataops;

-- dbt schemas inside the main ecommerce database
-- (these will also be created by dbt, but having them
--  up-front avoids permission issues on first run)
\connect ecommerce;

CREATE SCHEMA IF NOT EXISTS "RAW";
CREATE SCHEMA IF NOT EXISTS "STAGE";
CREATE SCHEMA IF NOT EXISTS "DEV";
