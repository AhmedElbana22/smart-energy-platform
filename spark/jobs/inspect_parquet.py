
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PARQUET_PATH = "/workspace/data/raw/ami_readings_final"

spark = (
    SparkSession.builder
    .appName("InspectRealAMIParquet")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    # Read the actual partitioned Parquet dataset.
    df = spark.read.parquet(PARQUET_PATH)

    print("\n========== 1. ACTUAL SCHEMA ==========")
    df.printSchema()

    print("\n========== 2. COLUMN NAMES ==========")
    for name, dtype in df.dtypes:
        print(f"{name}: {dtype}")

    print("\n========== 3. SAMPLE RECORDS ==========")
    df.show(10, truncate=False)

    print("\n========== 4. TOTAL ROW COUNT ==========")
    print(f"Total records: {df.count():,}")

    print("\n========== 5. PARTITION VALUES ==========")
    if "reading_date" in df.columns:
        df.groupBy("reading_date").count().orderBy("reading_date").show(
            20, truncate=False
        )
    else:
        print("No reading_date column detected.")

    print("\n========== 6. NULL COUNTS ==========")
    null_counts = df.agg(*[
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ])
    null_counts.show(truncate=False)

    print("\n========== 7. NUMERIC COLUMN SUMMARY ==========")
    numeric_types = {
        "tinyint", "smallint", "int", "bigint",
        "float", "double", "decimal"
    }
    numeric_columns = [
        name for name, dtype in df.dtypes
        if any(dtype.lower().startswith(t) for t in numeric_types)
    ]

    if numeric_columns:
        df.select(*numeric_columns).summary(
            "count", "min", "max", "mean"
        ).show(truncate=False)
    else:
        print("No numeric columns detected.")

finally:
    spark.stop()
