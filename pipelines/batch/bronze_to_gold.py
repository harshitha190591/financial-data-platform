import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# ── Spark session with Delta Lake ─────────────────────────
spark = (
    SparkSession.builder
    .appName("FinancialDataPlatform")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.shuffle.partitions", "4")   # keep it light locally
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

BASE       = os.path.dirname(os.path.abspath(__file__))
RAW_DIR    = os.path.join(BASE, "../../data/raw")
BRONZE_DIR = os.path.join(BASE, "../../data/bronze")
SILVER_DIR = os.path.join(BASE, "../../data/silver")
GOLD_DIR   = os.path.join(BASE, "../../data/gold")

# ── Schemas ───────────────────────────────────────────────
POLICY_SCHEMA = StructType([
    StructField("policy_id",      StringType()),
    StructField("customer_id",    StringType()),
    StructField("policy_type",    StringType()),
    StructField("start_date",     StringType()),
    StructField("end_date",       StringType()),
    StructField("premium_amount", DoubleType()),
    StructField("currency",       StringType()),
    StructField("status",         StringType()),
    StructField("agent_id",       StringType()),
    StructField("created_at",     StringType()),
])

CLAIM_SCHEMA = StructType([
    StructField("claim_id",     StringType()),
    StructField("policy_id",    StringType()),
    StructField("claim_date",   StringType()),
    StructField("claim_amount", DoubleType()),
    StructField("currency",     StringType()),
    StructField("status",       StringType()),
    StructField("description",  StringType()),
    StructField("handler_id",   StringType()),
    StructField("created_at",   StringType()),
])

TRADE_SCHEMA = StructType([
    StructField("trade_id",        StringType()),
    StructField("instrument",      StringType()),
    StructField("trade_type",      StringType()),
    StructField("quantity",        IntegerType()),
    StructField("price",           DoubleType()),
    StructField("currency",        StringType()),
    StructField("trader_id",       StringType()),
    StructField("desk_id",         StringType()),
    StructField("trade_timestamp", StringType()),
    StructField("settlement_date", StringType()),
    StructField("status",          StringType()),
])

# ═══════════════════════════════════════════════════════════
# BRONZE — raw ingestion, no transformation, add metadata
# ═══════════════════════════════════════════════════════════
def ingest_bronze():
    print("\n── Bronze: ingesting raw files ──")

    for name, schema, path in [
        ("policies",    POLICY_SCHEMA, f"{RAW_DIR}/policies/policies.csv"),
        ("claims",      CLAIM_SCHEMA,  f"{RAW_DIR}/claims/claims.csv"),
        ("trades",      TRADE_SCHEMA,  f"{RAW_DIR}/trades/trades.csv"),
    ]:
        df = (
            spark.read
            .option("header", "true")
            .option("mode", "PERMISSIVE")       # don't fail on bad rows
            .schema(schema)
            .csv(path)
        )
        # add audit columns
        df = df.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_file", F.lit(path))

        out = f"{BRONZE_DIR}/{name}"
        df.write.format("delta").mode("overwrite").save(out)
        print(f"  {name}: {df.count()} rows → {out}")


# ═══════════════════════════════════════════════════════════
# SILVER — clean, deduplicate, validate, cast types
# ═══════════════════════════════════════════════════════════
def process_silver():
    print("\n── Silver: cleaning & validating ──")

    # ── Policies ──────────────────────────────────────────
    policies = spark.read.format("delta").load(f"{BRONZE_DIR}/policies")

    # deduplicate on policy_id keeping latest created_at
    w = F.window if hasattr(F, "window") else None
    from pyspark.sql.window import Window
    dedup_w = Window.partitionBy("policy_id").orderBy(F.col("created_at").desc())

    policies_clean = (
        policies
        .withColumn("rn", F.row_number().over(dedup_w))
        .filter(F.col("rn") == 1).drop("rn")
        .filter(F.col("policy_id").isNotNull())
        .filter(F.col("premium_amount") > 0)
        .withColumn("start_date", F.to_date("start_date", "yyyy-MM-dd"))
        .withColumn("end_date",   F.to_date("end_date",   "yyyy-MM-dd"))
        .withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("policy_type", F.upper(F.trim("policy_type")))
        .withColumn("status",      F.upper(F.trim("status")))
    )
    policies_clean.write.format("delta").mode("overwrite").save(f"{SILVER_DIR}/policies")
    print(f"  policies: {policies_clean.count()} rows (deduped + validated)")

    # ── Claims ────────────────────────────────────────────
    claims = spark.read.format("delta").load(f"{BRONZE_DIR}/claims")

    claims_clean = (
        claims
        .filter(F.col("claim_id").isNotNull())
        .filter(F.col("policy_id").isNotNull())
        # flag nulls instead of dropping — keeps audit trail
        .withColumn("claim_amount_flag",
                    F.when(F.col("claim_amount").isNull(), "MISSING").otherwise("OK"))
        # fill null amounts with 0 for aggregation safety
        .withColumn("claim_amount", F.coalesce(F.col("claim_amount"), F.lit(0.0)))
        .withColumn("claim_date",  F.to_date("claim_date", "yyyy-MM-dd"))
        .withColumn("created_at",  F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("status",      F.upper(F.trim("status")))
    )
    claims_clean.write.format("delta").mode("overwrite").save(f"{SILVER_DIR}/claims")
    print(f"  claims: {claims_clean.count()} rows (nulls flagged + validated)")

    # ── Trades ────────────────────────────────────────────
    trades = spark.read.format("delta").load(f"{BRONZE_DIR}/trades")

    trades_clean = (
        trades
        .filter(F.col("trade_id").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("price") > 0)
        .withColumn("trade_timestamp",  F.to_timestamp("trade_timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("settlement_date",  F.to_date("settlement_date", "yyyy-MM-dd"))
        .withColumn("trade_value",      F.round(F.col("quantity") * F.col("price"), 2))
        .withColumn("trade_type",       F.upper(F.trim("trade_type")))
        .withColumn("status",           F.upper(F.trim("status")))
    )
    trades_clean.write.format("delta").mode("overwrite").save(f"{SILVER_DIR}/trades")
    print(f"  trades: {trades_clean.count()} rows (validated + trade_value added)")


# ═══════════════════════════════════════════════════════════
# GOLD — business-ready aggregations for analytics
# ═══════════════════════════════════════════════════════════
def build_gold():
    print("\n── Gold: building analytical tables ──")

    policies = spark.read.format("delta").load(f"{SILVER_DIR}/policies")
    claims   = spark.read.format("delta").load(f"{SILVER_DIR}/claims")
    trades   = spark.read.format("delta").load(f"{SILVER_DIR}/trades")

    # ── Gold 1: claims summary per policy type ────────────
    claims_with_type = (
        claims.join(policies.select("policy_id", "policy_type", "premium_amount"),
                    on="policy_id", how="left")
    )
    claims_summary = (
        claims_with_type
        .groupBy("policy_type", "status")
        .agg(
            F.count("claim_id").alias("total_claims"),
            F.sum("claim_amount").alias("total_claim_amount"),
            F.avg("claim_amount").alias("avg_claim_amount"),
            F.sum("premium_amount").alias("total_premium_collected"),
        )
        .withColumn("loss_ratio",
                    F.round(F.col("total_claim_amount") / F.col("total_premium_collected"), 4))
        .withColumn("_created_at", F.current_timestamp())
    )
    claims_summary.write.format("delta").mode("overwrite").save(f"{GOLD_DIR}/claims_summary")
    print(f"  claims_summary: {claims_summary.count()} rows")

    # ── Gold 2: daily trading P&L per desk ────────────────
    daily_pnl = (
        trades
        .filter(F.col("status") == "SETTLED")
        .withColumn("trade_date", F.to_date("trade_timestamp"))
        .groupBy("trade_date", "desk_id", "instrument")
        .agg(
            F.sum(
                F.when(F.col("trade_type") == "BUY",  -F.col("trade_value"))
                 .when(F.col("trade_type") == "SELL",  F.col("trade_value"))
                 .otherwise(0)
            ).alias("net_pnl"),
            F.count("trade_id").alias("total_trades"),
            F.sum("quantity").alias("total_volume"),
        )
        .withColumn("_created_at", F.current_timestamp())
        .orderBy("trade_date", "desk_id")
    )
    daily_pnl.write.format("delta").mode("overwrite").save(f"{GOLD_DIR}/daily_pnl")
    print(f"  daily_pnl: {daily_pnl.count()} rows")

    # ── Gold 3: policy portfolio summary ─────────────────
    portfolio = (
        policies
        .groupBy("policy_type", "status")
        .agg(
            F.count("policy_id").alias("total_policies"),
            F.sum("premium_amount").alias("total_premium"),
            F.avg("premium_amount").alias("avg_premium"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("_created_at", F.current_timestamp())
    )
    portfolio.write.format("delta").mode("overwrite").save(f"{GOLD_DIR}/policy_portfolio")
    print(f"  policy_portfolio: {portfolio.count()} rows")

    print("\n── Gold tables ready for analytics ──")


# ── Delta time-travel demo ────────────────────────────────
def demo_time_travel():
    print("\n── Demo: Delta time-travel query ──")
    df = spark.read.format("delta").option("versionAsOf", 0).load(f"{SILVER_DIR}/policies")
    print(f"  policies at version 0: {df.count()} rows")


# ── Run all ───────────────────────────────────────────────
if __name__ == "__main__":
    ingest_bronze()
    process_silver()
    build_gold()
    demo_time_travel()
    print("\nPipeline complete!")
    spark.stop()