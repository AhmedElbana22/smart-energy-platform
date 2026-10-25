
from pyspark.sql import SparkSession, functions as F

# --------------------------------------------------
# Configuration
# --------------------------------------------------
SILVER_TABLE = "energy.silver.meter_readings"
GOLD_TABLE = "energy.gold.hourly_energy_metrics"

CHECKPOINT_PATH = (
    "hdfs://namenode:8020/checkpoints/silver-to-gold"
)

spark = (
    SparkSession.builder
    .appName("SilverToGoldHourlyAggregation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS energy.gold")


def process_batch(batch_df, batch_id):
    """Aggregate each Silver micro-batch into hourly Gold metrics."""

    if batch_df.isEmpty():
        return

    # Normalize timestamps and derive the hourly bucket.
    hourly = (
        batch_df
        .withColumn(
            "reading_hour_utc",
            F.date_trunc("hour", F.col("reading_ts_utc"))
        )
        .groupBy(
            "reading_hour_utc",
            "substation_id",
            "feeder_id",
            "transformer_id"
        )
        .agg(
            F.count("*").alias("reading_count"),
            F.countDistinct("meter_id").alias("distinct_meter_count"),

            F.sum("active_energy_kwh_import")
            .alias("total_energy_kwh"),

            F.avg("active_power_kw")
            .alias("avg_active_power_kw"),

            F.max("active_power_kw")
            .alias("max_active_power_kw"),

            F.avg("voltage_v")
            .alias("avg_voltage_v"),

            F.min("voltage_v")
            .alias("min_voltage_v"),

            F.max("voltage_v")
            .alias("max_voltage_v"),

            F.sum(
                F.when(F.col("anomaly_label") == True, 1).otherwise(0)
            ).alias("anomaly_reading_count"),

            F.sum(
                F.when(F.col("quality_status") != "valid", 1)
                .otherwise(0)
            ).alias("non_valid_quality_count"),

            F.avg("loading_ratio").alias("avg_loading_ratio"),
            F.max("loading_ratio").alias("max_loading_ratio")
        )
        .withColumn(
            "anomaly_rate",
            F.when(
                F.col("reading_count") > 0,
                F.col("anomaly_reading_count") / F.col("reading_count")
            )
        )
        .withColumn(
            "quality_issue_rate",
            F.when(
                F.col("reading_count") > 0,
                F.col("non_valid_quality_count") / F.col("reading_count")
            )
        )
        .withColumn(
            "processing_batch_id",
            F.lit(batch_id)
        )
        .withColumn(
            "aggregated_at_utc",
            F.current_timestamp()
        )
    )

    # Create the Gold table if it does not exist.
    if not spark.catalog.tableExists(GOLD_TABLE):
        (
            hourly.writeTo(GOLD_TABLE)
            .using("iceberg")
            .partitionedBy(F.days("reading_hour_utc"))
            .create()
        )
        print(f"Batch {batch_id}: Gold table created.")
        return

    # Upsert hourly aggregates instead of appending duplicate groups.
    hourly.createOrReplaceTempView("gold_hourly_microbatch")

    spark.sql(f"""
        MERGE INTO {GOLD_TABLE} AS target
        USING gold_hourly_microbatch AS source
        ON target.reading_hour_utc = source.reading_hour_utc
           AND target.substation_id <=> source.substation_id
           AND target.feeder_id <=> source.feeder_id
           AND target.transformer_id <=> source.transformer_id

        WHEN MATCHED THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"Batch {batch_id}: hourly Gold aggregates merged.")


# Read Silver as a stream of changes/newly committed data.
silver_stream = (
    spark.readStream
    .format("iceberg")
    .load(SILVER_TABLE)
)

query = (
    silver_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="1 minute")
    .start()
)

query.awaitTermination()
