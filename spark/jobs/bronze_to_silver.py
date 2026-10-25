
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, DoubleType, BooleanType, LongType
)
from pyspark.sql.window import Window

BRONZE_PATH = "hdfs://namenode:8020/smart-energy/bronze/meter-readings"
CHECKPOINT = "hdfs://namenode:8020/checkpoints/bronze-to-silver"

SILVER_TABLE = "energy.silver.meter_readings"
QUARANTINE_TABLE = "energy.silver.quarantine"

# Match the actual 23-column Parquet dataset schema.
schema = StructType([
    StructField("meter_id", StringType()),
    StructField("source_system", StringType()),
    StructField("source_meter_id", StringType()),
    StructField("reading_ts_utc", TimestampType()),
    StructField("interval_minutes", IntegerType()),
    StructField("active_energy_kwh_import", DoubleType()),
    StructField("active_power_kw", DoubleType()),
    StructField("reactive_energy_kvarh_import", DoubleType()),
    StructField("reactive_power_kvar", DoubleType()),
    StructField("voltage_v_baseline", DoubleType()),
    StructField("voltage_v", DoubleType()),
    StructField("apparent_power_kva", DoubleType()),
    StructField("transformer_id", StringType()),
    StructField("feeder_id", StringType()),
    StructField("substation_id", StringType()),
    StructField("transformer_rating_kva", DoubleType()),
    StructField("loading_ratio", DoubleType()),
    StructField("anomaly_family", StringType()),
    StructField("anomaly_label", BooleanType()),
    StructField("value_status", StringType()),
    StructField("quality_status", StringType()),
    StructField("generator_seed", LongType()),
    StructField("reading_date", StringType()),
])

spark = (
    SparkSession.builder
    .appName("BronzeToSilverMicroBatch")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS energy.silver")

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Standardize strings and derive the partition date.
    df = (
        batch_df
        .withColumn("meter_id", F.trim("meter_id"))
        .withColumn(
            "reading_date",
            F.coalesce(
                F.col("reading_date"),
                F.to_date("reading_ts_utc").cast("string")
            )
        )
        .withColumn("ingested_at", F.current_timestamp())
    )

    # Basic data-quality rules.
    valid_condition = (
        F.col("meter_id").isNotNull()
        & (F.col("meter_id") != "")
        & F.col("reading_ts_utc").isNotNull()
        & F.col("interval_minutes").isNotNull()
        & (F.col("interval_minutes") > 0)
        & F.col("active_energy_kwh_import").isNotNull()
        & (F.col("active_energy_kwh_import") >= 0)
    )

    valid = df.filter(valid_condition)
    invalid = (
        df.filter(~valid_condition)
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                "; ",
                F.when(F.col("meter_id").isNull() |
                       (F.col("meter_id") == ""), "missing_meter_id"),
                F.when(F.col("reading_ts_utc").isNull(),
                       "missing_reading_timestamp"),
                F.when(F.col("interval_minutes").isNull() |
                       (F.col("interval_minutes") <= 0),
                       "invalid_interval"),
                F.when(
                    F.col("active_energy_kwh_import").isNull() |
                    (F.col("active_energy_kwh_import") < 0),
                    "invalid_energy"
                )
            )
        )
    )

    # Create the quarantine table from the first batch schema.
    if not spark.catalog.tableExists(QUARANTINE_TABLE):
        (
            invalid.limit(0).writeTo(QUARANTINE_TABLE)
            .using("iceberg").create()
        )

    if not invalid.isEmpty():
        invalid.writeTo(QUARANTINE_TABLE).append()

    if valid.isEmpty():
        print(f"Batch {batch_id}: no valid rows")
        return

    # Keep the latest record for each meter and timestamp.
    window = Window.partitionBy(
        "meter_id", "reading_ts_utc"
    ).orderBy(F.col("ingested_at").desc())

    valid = (
        valid
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Create the Silver table once, using the transformed schema.
    if not spark.catalog.tableExists(SILVER_TABLE):
        (
            valid.limit(0).writeTo(SILVER_TABLE)
            .using("iceberg").create()
        )

    valid.createOrReplaceTempView("silver_microbatch")

    # Idempotent upsert: rerunning a batch won't duplicate its keys.
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING silver_microbatch AS source
        ON target.meter_id = source.meter_id
           AND target.reading_ts_utc = source.reading_ts_utc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(
        f"Batch {batch_id}: "
        f"valid={valid.count()}, "
        f"invalid={invalid.count()}"
    )


stream = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 10)
    .option("recursiveFileLookup", "true")
    .json(BRONZE_PATH)
)

query = (
    stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
