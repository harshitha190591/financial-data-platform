# Financial Data Platform

A production-grade financial data platform built with PySpark, Delta Lake, Apache Airflow, and dbt. Designed to mirror real-world data engineering patterns used in insurance and trading environments.

---

## Architecture

```
Raw CSV Files
     │
     ▼
┌─────────────┐
│   Bronze    │  Raw ingestion + audit metadata
└─────────────┘
     │
     ▼
┌─────────────┐
│Quality Gate │  Config-driven DQ checks (blocks pipeline on failure)
└─────────────┘
     │
     ▼
┌─────────────┐
│   Silver    │  Cleaned, deduplicated, validated, typed
└─────────────┘
     │
     ▼
┌─────────────┐
│    Gold     │  Business-ready aggregations (loss ratio, P&L)
└─────────────┘
     │
     ▼
┌─────────────┐
│  dbt Models │  Staging → Intermediate → Marts
└─────────────┘
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Processing | PySpark, Delta Lake |
| Orchestration | Apache Airflow |
| Transformation | dbt Core, DuckDB |
| Data Generation | Python, Faker |
| Testing | pytest |
| CI/CD | GitHub Actions |

---

## Project Structure

```
financial-data-platform/
├── data_generator/         # Faker-based mock financial data generator
├── pipelines/
│   └── batch/              # PySpark Bronze → Silver → Gold pipeline
├── quality/                # Config-driven data quality framework
├── orchestration/
│   └── dags/               # Airflow DAGs with quality gate
├── transformation/
│   └── financial_transforms/  # dbt models (staging/intermediate/marts)
└── tests/                  # Unit and integration tests
```

---

## Key Features

### 1. Bronze → Silver → Gold Delta Lake Pipeline
- Ingests raw financial data (policies, claims, trades) into Bronze layer
- Silver layer handles deduplication via window functions, null flagging, and type casting
- Gold layer produces business aggregations: loss ratio, daily P&L, portfolio summary
- Delta Lake provides ACID transactions, schema enforcement, and time-travel

### 2. Config-Driven Data Quality Framework
- YAML-configurable checks: null rates, row counts, min values, allowed values, uniqueness
- Quality gate blocks downstream tasks on failure
- Every run writes a timestamped audit log to `data/quality_metrics/`
- Reduces pipeline failures by enforcing schema-compliant, audit-ready data assets

### 3. Apache Airflow Orchestration
- Full DAG: `start → ingest_bronze → quality_gate → process_silver → build_gold → end`
- SLA miss callbacks for breach alerting
- Retry logic and failure handling on all tasks
- Runs daily at 06:00

### 4. dbt Transformation Layer
- Staging models: clean column names and basic casting
- Intermediate models: enriched claims with risk tier classification
- Mart models: `mart_loss_ratio` and `mart_trading_summary`
- Built-in dbt tests for not-null, accepted values, and uniqueness
- Auto-generated dbt docs with full lineage graph

---

## Getting Started

### Prerequisites
- Python 3.12
- Java 11+ (required for PySpark)

### Setup

```bash
# clone the repo
git clone https://github.com/harshitha190591/financial-data-platform.git
cd financial-data-platform

# create virtual environment
python3 -m venv venv
source venv/bin/activate

# install dependencies
pip install pyspark==3.5.0 delta-spark==3.1.0 faker pytest pyyaml
pip install apache-airflow dbt-core dbt-duckdb
```

### Run the Pipeline

```bash
# generate mock data
python3 data_generator/generate_data.py

# run Bronze → Silver → Gold pipeline
python3 pipelines/batch/bronze_to_gold.py

# run data quality checks
python3 quality/dq_framework.py

# run dbt transformations
cd transformation/financial_transforms
dbt seed && dbt run && dbt test
```

### Start Airflow

```bash
export AIRFLOW_HOME=~/financial-data-platform/orchestration
airflow db init
airflow webserver --port 8080 &
airflow scheduler &
airflow dags trigger financial_data_pipeline
```

---

## Data Quality Checks

| Check | Description |
|---|---|
| `not_null` | Ensures critical columns have no nulls |
| `null_rate` | Allows configurable null % threshold |
| `min_value` | Validates numeric columns are above a minimum |
| `row_count` | Ensures row counts are within expected range |
| `unique` | Validates no duplicate primary keys |
| `allowed_values` | Validates categorical columns against a whitelist |

---

## Author

**Harshitha Shetty** — Data Engineer  
[LinkedIn](https://linkedin.com/in/harshitha-shetty) | [GitHub](https://github.com/harshitha190591)