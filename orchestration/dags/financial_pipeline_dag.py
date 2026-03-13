import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ── Make project modules importable inside Airflow ────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, PROJECT_ROOT)

# ── Default args — mirrors production standards ───────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry":   False,
    "sla":              timedelta(hours=2),   # SLA breach triggers a callback
}

# ── SLA miss callback — logs breach, mimics alerting ─────
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA BREACH ALERT — DAG: {dag.dag_id} | Tasks: {task_list}")


# ═══════════════════════════════════════════════════════════
# TASK FUNCTIONS — each wraps one pipeline stage
# ═══════════════════════════════════════════════════════════
def task_ingest_bronze(**ctx):
    from pipelines.batch.bronze_to_gold import ingest_bronze
    print(f"[{ctx['ts']}] Starting Bronze ingestion...")
    ingest_bronze()
    print("Bronze ingestion complete.")


def task_quality_gate(**ctx):
    """Runs DQ checks — raises exception if any check fails, blocking Silver."""
    from quality.dq_framework import run_all_checks
    print(f"[{ctx['ts']}] Running data quality checks on Bronze...")
    passed = run_all_checks()
    if not passed:
        raise ValueError("Quality gate FAILED — Silver and Gold tasks blocked.")
    print("Quality gate PASSED — proceeding to Silver.")


def task_process_silver(**ctx):
    from pipelines.batch.bronze_to_gold import process_silver
    print(f"[{ctx['ts']}] Starting Silver processing...")
    process_silver()
    print("Silver processing complete.")


def task_build_gold(**ctx):
    from pipelines.batch.bronze_to_gold import build_gold
    print(f"[{ctx['ts']}] Building Gold tables...")
    build_gold()
    print("Gold tables ready.")


def task_log_completion(**ctx):
    """Final task — logs run metadata, mimics a completion notification."""
    run_date = ctx["ds"]
    print(f"Pipeline run complete for {run_date}")
    print(f"  DAG       : {ctx['dag'].dag_id}")
    print(f"  Run ID    : {ctx['run_id']}")
    print(f"  Triggered : {ctx['ts']}")


# ═══════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════
with DAG(
    dag_id="financial_data_pipeline",
    description="Bronze → Quality Gate → Silver → Gold financial data pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",   # runs daily at 06:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "delta-lake", "batch"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    # ── Start marker ──────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Bronze ingestion ──────────────────────────────────
    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=task_ingest_bronze,
    )

    # ── Quality gate — blocks Silver if checks fail ───────
    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=task_quality_gate,
    )

    # ── Silver processing ─────────────────────────────────
    process_silver = PythonOperator(
        task_id="process_silver",
        python_callable=task_process_silver,
    )

    # ── Gold aggregations ─────────────────────────────────
    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=task_build_gold,
    )

    # ── Completion log ────────────────────────────────────
    complete = PythonOperator(
        task_id="log_completion",
        python_callable=task_log_completion,
    )

    # ── End marker ────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── DAG dependency chain ──────────────────────────────
    start >> ingest_bronze >> quality_gate >> process_silver >> build_gold >> complete >> end