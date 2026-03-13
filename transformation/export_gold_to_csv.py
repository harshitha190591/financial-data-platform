import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ExportGold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.shuffle.partitions", "4")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

BASE     = os.path.dirname(os.path.abspath(__file__))
GOLD_DIR = os.path.join(BASE, "../data/gold")
SEED_DIR = os.path.join(BASE, "financial_transforms/seeds")
os.makedirs(SEED_DIR, exist_ok=True)

tables = ["claims_summary", "daily_pnl", "policy_portfolio"]

for table in tables:
    print(f"Exporting {table}...")
    df = spark.read.format("delta").load(f"{GOLD_DIR}/{table}")
    # write as single CSV using pandas-free approach
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{SEED_DIR}/tmp_{table}")
    # rename the part file to a clean CSV name
    tmp_dir = f"{SEED_DIR}/tmp_{table}"
    for f in os.listdir(tmp_dir):
        if f.startswith("part-") and f.endswith(".csv"):
            os.rename(f"{tmp_dir}/{f}", f"{SEED_DIR}/{table}.csv")
    # clean up tmp folder
    import shutil
    shutil.rmtree(tmp_dir)
    print(f"  → seeds/{table}.csv")

print("\nAll Gold tables exported to dbt seeds!")
spark.stop()