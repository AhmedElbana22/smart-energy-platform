from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SmartEnergy-Iceberg-SmokeTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    print("\n=== 1. CREATE ICEBERG NAMESPACE ===")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS energy.test_db")

    print("\n=== 2. CREATE ICEBERG TABLE ===")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS energy.test_db.spark_smoke_test (
            meter_id STRING,
            voltage_v DOUBLE,
            quality_status STRING
        )
        USING iceberg
    """)

    print("\n=== 3. INSERT TEST RECORDS ===")
    spark.sql("""
        INSERT INTO energy.test_db.spark_smoke_test
        VALUES
            ('ICEBERG-TEST-001', 229.5, 'good'),
            ('ICEBERG-TEST-002', 198.5, 'suspect'),
            ('ICEBERG-TEST-003', 214.0, 'suspect')
    """)

    print("\n=== 4. READ TABLE ===")
    spark.sql("""
        SELECT *
        FROM energy.test_db.spark_smoke_test
        ORDER BY meter_id
    """).show(truncate=False)

    print("\n=== 5. CHECK ICEBERG SNAPSHOTS ===")
    spark.sql("""
        SELECT committed_at, snapshot_id, operation
        FROM energy.test_db.spark_smoke_test.snapshots
    """).show(truncate=False)

    print("\nICEBERG SMOKE TEST PASSED")

finally:
    spark.stop()
