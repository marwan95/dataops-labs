"""
DataOps Mentorship — Automated Grading Script
==============================================
Grades student dbt submissions for Weeks 1–3.

Usage:
    python scripts/grade_assignment.py --week 1
    python scripts/grade_assignment.py --week 2
    python scripts/grade_assignment.py --week 3
"""

import argparse
import io
import json
import os
import re
import sys

# Fix Unicode output on Windows terminals
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )

# ── Paths ─────────────────────────────────────────────────────
DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_learning")
MODELS_DIR = os.path.join(DBT_PROJECT_DIR, "models")
STAGE_DIR = os.path.join(MODELS_DIR, "stage")
DEV_DIR = os.path.join(MODELS_DIR, "dev")
SNAPSHOTS_DIR = os.path.join(DBT_PROJECT_DIR, "snapshots")
DOCS_DIR = os.path.join(DBT_PROJECT_DIR, "docs")
RESULTS_PATH = os.path.join(DBT_PROJECT_DIR, "target", "run_results.json")


# ═════════════════════════════════════════════════════════════
#  HELPERS
# ═════════════════════════════════════════════════════════════

def file_exists(path):
    """Check if a file exists and return its content if so."""
    full = os.path.normpath(path)
    if os.path.isfile(full):
        with open(full, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    return None


def check_file_exists(path, label):
    """Return (pass, message) for file existence check."""
    content = file_exists(path)
    if content is not None:
        return True, f"✅ {label} — file found"
    return False, f"❌ {label} — file NOT found"


def check_file_contains(path, pattern, label, case_insensitive=True):
    """Check if file content matches a regex pattern."""
    content = file_exists(path)
    if content is None:
        return False, f"❌ {label} — file not found"
    flags = re.IGNORECASE if case_insensitive else 0
    if re.search(pattern, content, flags):
        return True, f"✅ {label}"
    return False, f"❌ {label}"


def check_word_count(path, min_words, label):
    """Check if a file has at least min_words words."""
    content = file_exists(path)
    if content is None:
        return False, f"❌ {label} — file not found"
    words = len(content.split())
    if words >= min_words:
        return True, f"✅ {label} ({words} words)"
    return False, f"❌ {label} — only {words} words (need {min_words}+)"


def load_dbt_results():
    """Load dbt run_results.json if available."""
    if os.path.isfile(RESULTS_PATH):
        with open(RESULTS_PATH, "r") as f:
            return json.load(f)
    return None


def check_dbt_result(results_data, model_unique_id_fragment, label):
    """Check if a specific model/test passed in dbt results."""
    if results_data is None:
        return False, f"⏳ {label} — no dbt results found (run dbt first)"

    for res in results_data.get("results", []):
        uid = res.get("unique_id", "")
        if model_unique_id_fragment in uid:
            status = res.get("status", "")
            if status in ("pass", "success"):
                return True, f"✅ {label}"
            else:
                msg = res.get("message", "")[:80] if res.get("message") else ""
                return False, f"❌ {label} — status: {status} {msg}"

    return False, f"⏳ {label} — not found in dbt results"


# ═════════════════════════════════════════════════════════════
#  WEEK 1 GRADING
# ═════════════════════════════════════════════════════════════

def grade_week_1():
    """Grade Week 1: Sources, Models, and Seeds."""
    report = []
    total = 0
    max_score = 0

    report.append("# 📊 Week 1 — Grade Report\n")
    report.append("## Sources, Models, and Seeds\n")
    report.append("| Task | Check | Points | Status |")
    report.append("| :--- | :--- | :---: | :---: |")

    checks = []

    # ── Task 1.2: Sources (15 pts) ──────────────────────────
    sources_path = os.path.join(STAGE_DIR, "sources.yml")
    checks.append(("1.2", *check_file_exists(sources_path, "sources.yml exists"), 5))
    checks.append(("1.2", *check_file_contains(sources_path, r"name:\s*RAW", "Sources reference RAW schema"), 5))
    checks.append(("1.2", *check_file_contains(sources_path, r"raw_customers", "raw_customers declared"), 5))

    # ── Task 1.3: Stage Models (40 pts) ─────────────────────
    stage_models = [
        ("stg_customers.sql", 8),
        ("stg_products.sql", 8),
        ("stg_orders.sql", 8),
        ("stg_order_items.sql", 8),
        ("stg_store_locations.sql", 8),
    ]
    for model, pts in stage_models:
        path = os.path.join(STAGE_DIR, model)
        checks.append(("1.3", *check_file_exists(path, model), pts))

    # ── Task 1.4: Fact Model (20 pts) ───────────────────────
    fct_path = os.path.join(DEV_DIR, "fct_order_details.sql")
    checks.append(("1.4", *check_file_exists(fct_path, "fct_order_details.sql exists"), 5))
    checks.append(("1.4", *check_file_contains(fct_path, r"ref\s*\(\s*['\"]stg_", "Uses ref() to staged models"), 5))
    checks.append(("1.4", *check_file_contains(fct_path, r"gross_amount|net_amount|total_amount", "Calculated columns present"), 5))
    checks.append(("1.4", *check_file_contains(fct_path, r"join", "Has JOIN logic"), 5))

    # ── Task 1.5: Dimension Model (10 pts) ──────────────────
    dim_path = os.path.join(DEV_DIR, "dim_customers.sql")
    checks.append(("1.5", *check_file_exists(dim_path, "dim_customers.sql exists"), 5))
    checks.append(("1.5", *check_file_contains(dim_path, r"total_orders|total_spent", "Aggregation columns present"), 5))

    # ── Build report ────────────────────────────────────────
    for task, passed, message, points in checks:
        max_score += points
        earned = points if passed else 0
        total += earned
        status = f"{earned}/{points}"
        report.append(f"| {task} | {message} | {status} | {'✅' if passed else '❌'} |")

    _append_summary(report, total, max_score)
    return "\n".join(report)


# ═════════════════════════════════════════════════════════════
#  WEEK 2 GRADING
# ═════════════════════════════════════════════════════════════

def grade_week_2():
    """Grade Week 2: Materializations."""
    report = []
    total = 0
    max_score = 0

    report.append("# 📊 Week 2 — Grade Report\n")
    report.append("## Materializations\n")
    report.append("| Task | Check | Points | Status |")
    report.append("| :--- | :--- | :---: | :---: |")

    checks = []
    dbt_results = load_dbt_results()

    # ── Task 2.1: Materialization Doc (15 pts) ──────────────
    doc_path = os.path.join(DOCS_DIR, "materializations.md")
    checks.append(("2.1", *check_file_exists(doc_path, "materializations.md exists"), 5))
    checks.append(("2.1", *check_file_contains(doc_path, r"(?i)(table|view)", "Discusses table vs view"), 5))
    checks.append(("2.1", *check_word_count(doc_path, 200, "At least 200 words"), 5))

    # ── Task 2.2: Incremental Model (40 pts) ────────────────
    fct_path = os.path.join(DEV_DIR, "fct_order_details.sql")

    checks.append(("2.2", *check_file_contains(
        fct_path,
        r"materialized\s*=\s*['\"]incremental['\"]",
        "materialized='incremental' in config"
    ), 5))

    checks.append(("2.2", *check_file_contains(
        fct_path,
        r"unique_key\s*=\s*['\"]order_item_id['\"]",
        "unique_key='order_item_id'"
    ), 5))

    checks.append(("2.2", *check_file_contains(
        fct_path,
        r"is_incremental\s*\(\s*\)",
        "{% if is_incremental() %} block present"
    ), 10))

    checks.append(("2.2", *check_file_contains(
        fct_path,
        r"interval\s*['\"]3\s*days?['\"]",
        "3-day look-back window"
    ), 5))

    checks.append(("2.2", *check_file_contains(
        fct_path,
        r"order_date",
        "Filters on order_date"
    ), 5))

    # Check dbt run results for fct_order_details
    checks.append(("2.2", *check_dbt_result(
        dbt_results,
        "fct_order_details",
        "fct_order_details runs successfully"
    ), 10))

    # ── Task 2.3: Snapshot (30 pts) ─────────────────────────
    snap_path = os.path.join(SNAPSHOTS_DIR, "snap_products.sql")

    checks.append(("2.3", *check_file_exists(snap_path, "snap_products.sql exists"), 5))

    checks.append(("2.3", *check_file_contains(
        snap_path,
        r"strategy\s*=\s*['\"]check['\"]",
        "Uses strategy='check'"
    ), 5))

    checks.append(("2.3", *check_file_contains(
        snap_path,
        r"check_cols\s*=\s*\[\s*['\"]list_price['\"]",
        "check_cols includes list_price"
    ), 5))

    checks.append(("2.3", *check_file_contains(
        snap_path,
        r"check_cols.*is_active",
        "check_cols includes is_active"
    ), 5))

    checks.append(("2.3", *check_file_contains(
        snap_path,
        r"unique_key\s*=\s*['\"]product_id['\"]",
        "unique_key='product_id'"
    ), 5))

    checks.append(("2.3", *check_file_contains(
        snap_path,
        r"\{%\s*snapshot",
        "{% snapshot %} block syntax"
    ), 5))

    # ── Task 2.4: Simulation (15 pts) ───────────────────────
    # Simulation is manually verified — we check if snapshot ran
    checks.append(("2.4", *check_dbt_result(
        dbt_results,
        "snap_products",
        "Snapshot ran successfully (dbt snapshot)"
    ), 15))

    # ── Build report ────────────────────────────────────────
    for task, passed, message, points in checks:
        max_score += points
        earned = points if passed else 0
        total += earned
        status = f"{earned}/{points}"
        report.append(f"| {task} | {message} | {status} | {'✅' if passed else '❌'} |")

    _append_summary(report, total, max_score)
    return "\n".join(report)


# ═════════════════════════════════════════════════════════════
#  WEEK 3 GRADING
# ═════════════════════════════════════════════════════════════

def grade_week_3():
    """Grade Week 3: Data Tests."""
    report = []
    total = 0
    max_score = 0

    report.append("# 📊 Week 3 — Grade Report\n")
    report.append("## Data Tests\n")
    report.append("| Task | Check | Points | Status |")
    report.append("| :--- | :--- | :---: | :---: |")

    checks = []
    tests_dir = os.path.join(DBT_PROJECT_DIR, "tests")

    # ── Task 3.1: schema.yml with generic tests (25 pts) ────
    schema_path = os.path.join(STAGE_DIR, "schema.yml")
    checks.append(("3.1", *check_file_exists(schema_path, "schema.yml exists"), 5))
    checks.append(("3.1", *check_file_contains(schema_path, r"\bunique\b", "unique test defined"), 5))
    checks.append(("3.1", *check_file_contains(schema_path, r"\bnot_null\b", "not_null test defined"), 5))
    checks.append(("3.1", *check_file_contains(schema_path, r"accepted_values", "accepted_values test defined"), 5))
    checks.append(("3.1", *check_file_contains(schema_path, r"relationships", "relationships test defined"), 5))

    # ── Task 3.2: Custom singular tests (35 pts) ────────────
    singular_tests = [
        ("test_no_future_orders.sql", 7),
        ("test_positive_quantities.sql", 7),
        ("test_valid_discount_range.sql", 7),
        ("test_positive_shipping.sql", 7),
        ("test_positive_cost_price.sql", 7),
    ]
    for fname, pts in singular_tests:
        path = os.path.join(tests_dir, fname)
        checks.append(("3.2", *check_file_exists(path, fname), pts))

    # ── Task 3.3: Quarantine model (25 pts) ─────────────────
    quarantine_path = os.path.join(DEV_DIR, "quarantine_orders.sql")
    checks.append(("3.3", *check_file_exists(quarantine_path, "quarantine_orders.sql exists"), 5))
    checks.append(("3.3", *check_file_contains(
        quarantine_path, r"failure_reason", "failure_reason column present"), 10))
    checks.append(("3.3", *check_file_contains(
        quarantine_path, r"ref\s*\(", "Uses ref() to staged models"), 5))
    checks.append(("3.3", *check_file_contains(
        quarantine_path, r"union\s+all|case\s+when", "Multiple failure conditions captured"), 5))

    # ── Task 3.4: Data quality report (15 pts) ──────────────
    report_path = os.path.join(DOCS_DIR, "data_quality_report.md")
    checks.append(("3.4", *check_file_exists(report_path, "data_quality_report.md exists"), 5))
    checks.append(("3.4", *check_word_count(report_path, 150, "At least 150 words"), 5))
    checks.append(("3.4", *check_file_contains(
        report_path, r"remediat|recommend|fix|root cause",
        "Includes remediation recommendations"), 5))

    # ── Build report ────────────────────────────────────────
    for task, passed, message, points in checks:
        max_score += points
        earned = points if passed else 0
        total += earned
        status = f"{earned}/{points}"
        report.append(f"| {task} | {message} | {status} | {'✅' if passed else '❌'} |")

    _append_summary(report, total, max_score)
    return "\n".join(report)


# ═════════════════════════════════════════════════════════════
#  WEEK 4 GRADING
# ═════════════════════════════════════════════════════════════

def grade_week_4():
    """Grade Week 4: Macros and Packages."""
    report = []
    total = 0
    max_score = 0

    report.append("# 📊 Week 4 — Grade Report\n")
    report.append("## Macros and Packages\n")
    report.append("| Task | Check | Points | Status |")
    report.append("| :--- | :--- | :---: | :---: |")

    checks = []
    macros_dir  = os.path.join(DBT_PROJECT_DIR, "macros")
    packages_path      = os.path.join(DBT_PROJECT_DIR, "packages.yml")
    fct_path           = os.path.join(DEV_DIR, "fct_order_details.sql")
    monthly_path       = os.path.join(DEV_DIR, "fct_monthly_revenue.sql")
    calc_macro_path    = os.path.join(macros_dir, "calculate_revenue.sql")
    conv_macro_path    = os.path.join(macros_dir, "convert_currency.sql")
    macros_yml_path    = os.path.join(macros_dir, "macros.yml")

    # ── Task 4.1: Jinja pivot model (15 pts) ────────────────
    checks.append(("4.1", *check_file_exists(monthly_path, "fct_monthly_revenue.sql exists"), 3))
    checks.append(("4.1", *check_file_contains(monthly_path, r"\{%[-\s]*set\b", "{% set %} variable used"), 4))
    checks.append(("4.1", *check_file_contains(monthly_path, r"\{%[-\s]*for\b", "{% for %} loop used"), 4))
    checks.append(("4.1", *check_file_contains(monthly_path, r"'jan'.*'feb'|jan_revenue|month_name.*_revenue|_revenue.*month_name", "Monthly revenue columns generated"), 4))

    # ── Task 4.2: Currency converter macro (30 pts) ─────────
    checks.append(("4.2", *check_file_exists(conv_macro_path, "convert_currency.sql exists"), 5))
    checks.append(("4.2", *check_file_contains(
        conv_macro_path,
        r"macro\s+convert_currency\s*\(\s*\w+\s*,\s*\w+\s*,\s*\w+",
        "Macro accepts 3 parameters"
    ), 5))
    checks.append(("4.2", *check_file_contains(conv_macro_path, r"OMR.*2\.60|2\.60.*OMR", "OMR rate defined"), 5))
    checks.append(("4.2", *check_file_contains(conv_macro_path, r"EUR.*1\.08|1\.08.*EUR", "EUR rate defined"), 5))
    checks.append(("4.2", *check_file_contains(fct_path, r"convert_currency", "Macro applied in fct_order_details"), 10))

    # ── Task 4.3: Revenue macro (20 pts) ────────────────────
    checks.append(("4.3", *check_file_exists(calc_macro_path, "calculate_revenue.sql exists"), 5))
    checks.append(("4.3", *check_file_contains(
        calc_macro_path,
        r"quantity.*unit_price|unit_price.*quantity",
        "Formula uses quantity × unit_price"
    ), 5))
    checks.append(("4.3", *check_file_contains(fct_path, r"calculate_revenue", "Macro applied in fct_order_details"), 10))

    # ── Task 4.4: dbt-utils (20 pts) ────────────────────────
    checks.append(("4.4", *check_file_exists(packages_path, "packages.yml exists"), 5))
    checks.append(("4.4", *check_file_contains(packages_path, r"dbt.?utils", "dbt-utils package declared"), 5))
    checks.append(("4.4", *check_file_contains(fct_path, r"generate_surrogate_key", "generate_surrogate_key used"), 5))

    # Check for any second dbt-utils macro across dev models
    dev_files = [
        os.path.join(DEV_DIR, f)
        for f in os.listdir(DEV_DIR) if f.endswith(".sql")
    ] if os.path.isdir(DEV_DIR) else []
    second_macro_found = any(
        file_exists(p) and re.search(
            r"dbt_utils\.(star|date_spine|get_column_values|pivot|union_relations|safe_cast)",
            file_exists(p) or "", re.IGNORECASE
        )
        for p in dev_files
    )
    checks.append(("4.4", (
        second_macro_found,
        "✅ Second dbt-utils macro used" if second_macro_found else "❌ Second dbt-utils macro not found"
    )[0], (
        "✅ Second dbt-utils macro used" if second_macro_found else "❌ Second dbt-utils macro not found"
    ), 5))

    # ── Task 4.5: Macro documentation (15 pts) ──────────────
    checks.append(("4.5", *check_file_exists(macros_yml_path, "macros.yml exists"), 5))
    checks.append(("4.5", *check_file_contains(
        macros_yml_path,
        r"convert_currency",
        "convert_currency documented"
    ), 5))
    checks.append(("4.5", *check_file_contains(
        macros_yml_path,
        r"calculate_revenue",
        "calculate_revenue documented"
    ), 5))

    # ── Build report ────────────────────────────────────────
    for task, passed, message, points in checks:
        max_score += points
        earned = points if passed else 0
        total += earned
        status = f"{earned}/{points}"
        report.append(f"| {task} | {message} | {status} | {'✅' if passed else '❌'} |")

    _append_summary(report, total, max_score)
    return "\n".join(report)


# ═════════════════════════════════════════════════════════════
#  SHARED
# ═════════════════════════════════════════════════════════════

def _append_summary(report, total, max_score):
    """Append score summary and emoji feedback."""
    pct = (total / max_score * 100) if max_score > 0 else 0
    report.append(f"\n## **Total Score: {total} / {max_score}  ({pct:.0f}%)**")

    if pct >= 90:
        report.append("\n🟢 **Excellent Work!** Exceeds expectations.")
    elif pct >= 75:
        report.append("\n🔵 **Good progress.** Review the failing checks to reach 90%+.")
    elif pct >= 60:
        report.append("\n🟡 **Satisfactory.** Core tasks present but gaps remain.")
    else:
        report.append("\n🔴 **Needs Work.** Significant gaps — review the assignment instructions.")


# ═════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="DataOps Mentorship — Assignment Grader"
    )
    parser.add_argument(
        "--week", type=int, required=True, choices=[1, 2, 3, 4],
        help="Which week to grade (1, 2, 3, or 4)"
    )
    args = parser.parse_args()

    if args.week == 1:
        print(grade_week_1())
    elif args.week == 2:
        print(grade_week_2())
    elif args.week == 3:
        print(grade_week_3())
    elif args.week == 4:
        print(grade_week_4())


if __name__ == "__main__":
    main()
