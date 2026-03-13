import os
import json
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ── Spark session ─────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("DQFramework")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.shuffle.partitions", "4")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

BASE        = os.path.dirname(os.path.abspath(__file__))
SILVER_DIR  = os.path.join(BASE, "../data/silver")
METRICS_DIR = os.path.join(BASE, "../data/quality_metrics")
os.makedirs(METRICS_DIR, exist_ok=True)

# ═══════════════════════════════════════════════════════════
# CHECK DEFINITIONS — config-driven, no code change per table
# ═══════════════════════════════════════════════════════════
DQ_CHECKS = {
    "policies": {
        "path": f"{SILVER_DIR}/policies",
        "checks": [
            # (check_type, params, threshold)
            ("not_null",      {"col": "policy_id"},      0.0),   # 0% nulls allowed
            ("not_null",      {"col": "customer_id"},    0.0),
            ("not_null",      {"col": "premium_amount"}, 0.0),
            ("null_rate",     {"col": "agent_id"},       0.05),  # max 5% nulls
            ("min_value",     {"col": "premium_amount",  "min": 0},    0.0),
            ("row_count",     {"min": 400, "max": 600},  0.0),   # expect ~500 rows
            ("unique",        {"col": "policy_id"},      0.0),   # must be unique
            ("allowed_values",{"col": "status",
                               "values": ["ACTIVE","EXPIRED","CANCELLED"]}, 0.0),
        ]
    },
    "claims": {
        "path": f"{SILVER_DIR}/claims",
        "checks": [
            ("not_null",  {"col": "claim_id"},    0.0),
            ("not_null",  {"col": "policy_id"},   0.0),
            ("null_rate", {"col": "claim_amount"}, 0.15),  # we know ~10% are null
            ("min_value", {"col": "claim_amount", "min": 0}, 0.0),
            ("row_count", {"min": 250, "max": 350}, 0.0),
            ("allowed_values", {"col": "status",
                                "values": ["OPEN","IN REVIEW","APPROVED",
                                           "REJECTED","CLOSED"]}, 0.0),
        ]
    },
    "trades": {
        "path": f"{SILVER_DIR}/trades",
        "checks": [
            ("not_null",  {"col": "trade_id"},   0.0),
            ("not_null",  {"col": "instrument"}, 0.0),
            ("min_value", {"col": "price",    "min": 0}, 0.0),
            ("min_value", {"col": "quantity", "min": 0}, 0.0),
            ("row_count", {"min": 800, "max": 1100}, 0.0),
            ("allowed_values", {"col": "trade_type", "values": ["BUY","SELL"]}, 0.0),
        ]
    },
}

# ═══════════════════════════════════════════════════════════
# CHECK RUNNERS
# ═══════════════════════════════════════════════════════════
def check_not_null(df: DataFrame, col: str, threshold: float) -> dict:
    total    = df.count()
    null_cnt = df.filter(F.col(col).isNull()).count()
    null_rate = null_cnt / total if total > 0 else 0
    passed   = null_rate <= threshold
    return {
        "check":     "not_null",
        "column":    col,
        "null_rate": round(null_rate, 4),
        "threshold": threshold,
        "passed":    passed,
        "detail":    f"{null_cnt}/{total} nulls"
    }

def check_null_rate(df: DataFrame, col: str, threshold: float) -> dict:
    total    = df.count()
    null_cnt = df.filter(F.col(col).isNull()).count()
    null_rate = null_cnt / total if total > 0 else 0
    passed   = null_rate <= threshold
    return {
        "check":     "null_rate",
        "column":    col,
        "null_rate": round(null_rate, 4),
        "threshold": threshold,
        "passed":    passed,
        "detail":    f"{null_cnt}/{total} nulls ({round(null_rate*100,1)}%)"
    }

def check_min_value(df: DataFrame, col: str, min_val: float, threshold: float) -> dict:
    total      = df.count()
    fail_cnt   = df.filter(F.col(col) < min_val).count()
    fail_rate  = fail_cnt / total if total > 0 else 0
    passed     = fail_rate <= threshold
    return {
        "check":     "min_value",
        "column":    col,
        "fail_rate": round(fail_rate, 4),
        "threshold": threshold,
        "passed":    passed,
        "detail":    f"{fail_cnt} rows below min={min_val}"
    }

def check_row_count(df: DataFrame, min_rows: int, max_rows: int) -> dict:
    total  = df.count()
    passed = min_rows <= total <= max_rows
    return {
        "check":   "row_count",
        "column":  None,
        "count":   total,
        "min":     min_rows,
        "max":     max_rows,
        "passed":  passed,
        "detail":  f"{total} rows (expected {min_rows}–{max_rows})"
    }

def check_unique(df: DataFrame, col: str) -> dict:
    total    = df.count()
    distinct = df.select(col).distinct().count()
    passed   = total == distinct
    dupes    = total - distinct
    return {
        "check":   "unique",
        "column":  col,
        "passed":  passed,
        "detail":  f"{dupes} duplicate(s) found"
    }

def check_allowed_values(df: DataFrame, col: str, values: list, threshold: float) -> dict:
    total    = df.count()
    fail_cnt = df.filter(~F.col(col).isin(values)).count()
    fail_rate = fail_cnt / total if total > 0 else 0
    passed   = fail_rate <= threshold
    return {
        "check":     "allowed_values",
        "column":    col,
        "fail_rate": round(fail_rate, 4),
        "threshold": threshold,
        "passed":    passed,
        "detail":    f"{fail_cnt} rows with unexpected values"
    }

# ═══════════════════════════════════════════════════════════
# DISPATCHER
# ═══════════════════════════════════════════════════════════
def run_check(df: DataFrame, check_type: str, params: dict, threshold: float) -> dict:
    if check_type == "not_null":
        return check_not_null(df, params["col"], threshold)
    elif check_type == "null_rate":
        return check_null_rate(df, params["col"], threshold)
    elif check_type == "min_value":
        return check_min_value(df, params["col"], params["min"], threshold)
    elif check_type == "row_count":
        return check_row_count(df, params["min"], params["max"])
    elif check_type == "unique":
        return check_unique(df, params["col"])
    elif check_type == "allowed_values":
        return check_allowed_values(df, params["col"], params["values"], threshold)
    else:
        return {"check": check_type, "passed": False, "detail": "Unknown check type"}

# ═══════════════════════════════════════════════════════════
# MAIN RUNNER — loops all tables and checks
# ═══════════════════════════════════════════════════════════
def run_all_checks() -> bool:
    run_ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    all_pass = True
    report   = []

    for table, config in DQ_CHECKS.items():
        print(f"\n── Checking: {table} ──")
        df = spark.read.format("delta").load(config["path"])

        for (check_type, params, threshold) in config["checks"]:
            result = run_check(df, check_type, params, threshold)
            result["table"]    = table
            result["run_ts"]   = run_ts
            status = "PASS" if result["passed"] else "FAIL"
            col    = result.get("column") or ""
            print(f"  [{status}] {check_type:<16} {col:<16} {result['detail']}")
            if not result["passed"]:
                all_pass = False
            report.append(result)

    # ── Write audit log ───────────────────────────────────
    log_path = os.path.join(METRICS_DIR, f"dq_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(log_path, "w") as f:
        json.dump({"run_ts": run_ts, "overall_pass": all_pass, "results": report}, f, indent=2)
    print(f"\n── Audit log saved → {log_path}")

    return all_pass

# ═══════════════════════════════════════════════════════════
# GATE — call this from Airflow to block downstream tasks
# ═══════════════════════════════════════════════════════════
def quality_gate():
    print("\n Starting data quality checks...\n")
    passed = run_all_checks()
    if passed:
        print("\n Quality gate PASSED — pipeline can proceed\n")
    else:
        print("\n Quality gate FAILED — downstream tasks blocked\n")
        raise Exception("Data quality gate failed. Check audit log for details.")

if __name__ == "__main__":
    quality_gate()
    spark.stop()