from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BRONZE_PATH = (
    "hdfs://namenode:8020/"
    "smart-energy/bronze/meter-readings/partition=0/*.json"
)

OUTPUT_PATH = (
    "hdfs://namenode:8020/"
    "smart-energy/test/spark-smoke-output"
)

spark = (
    SparkSession.builder
    .appName("SmartEnergy-HDFS-SmokeTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    print("\n=== 1. READ BRONZE JSON ===")

    df = spark.read.json(BRONZE_PATH)

    print("\n=== 2. INFERRED SCHEMA ===")
    df.printSchema()

    count = df.count()
    print(f"\nBronze record count: {count}")

    print("\n=== 3. SAMPLE RECORDS ===")
    df.select(
        "meter_id",
        "reading_ts_utc",
        "voltage_v",
        "anomaly_family",
        "quality_status"
    ).show(10, truncate=False)

    print("\n=== 4. ANOMALY CHECK ===")
    df.filter(
        (col("anomaly_label") == 1)
    ).select(
        "meter_id",
        "voltage_v",
        "anomaly_family",
        "quality_status"
    ).show(truncate=False)

    print("\n=== 5. WRITE TEST OUTPUT TO HDFS ===")

    # Keep this output separate from the production Silver dataset.
    result = df.select(
        "meter_id",
        "reading_ts_utc",
        "voltage_v",
        "anomaly_family",
        "anomaly_label",
        "quality_status"
    )

    result.write.mode("overwrite").parquet(OUTPUT_PATH)

    print(f"Test output written to: {OUTPUT_PATH}")
    print("\nSMOKE TEST PASSED")

finally:
    spark.stop()
