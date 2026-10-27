
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. CONFIGURATION
# =========================================================

SILVER_TABLE = "energy.silver.meter_readings"

# Written by bronze_to_silver.py
SILVER_PROCESSING_LOG_PATH = (
    "hdfs://namenode:8020/smart-energy/control/processing-log"
)

# Dedicated, persistent checkpoint for Silver-to-Gold.
GOLD_PROCESSING_LOG_PATH = (
    "hdfs://namenode:8020/smart-energy/control/"
    "silver-to-gold-processing-log"
)

# Three business-oriented Gold tables.
GOLD_GRID_DAILY = "energy.gold.daily_grid_kpis"
GOLD_TRANSFORMER_INTERVAL = (
    "energy.gold.transformer_loading_15m"
)
GOLD_FEEDER_DAILY = "energy.gold.daily_feeder_health"

# This follows the voltage-drop threshold already used by the
# project's Flink SQL detector.
VOLTAGE_DROP_THRESHOLD_V = 15.0

# Match the existing Spark/Iceberg catalog configuration.
spark = (
    SparkSession.builder
    .appName("SmartEnergy_Silver_To_Gold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

Path = spark._jvm.org.apache.hadoop.fs.Path
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()


# =========================================================
# 2. HDFS AND PROCESSING-LOG HELPERS
# =========================================================

def get_fs(path_string):
    path = Path(path_string)
    fs = path.getFileSystem(hadoop_conf)
    return fs, path


def list_parquet_files(root_path):
    """Recursively list Parquet files using HDFS metadata."""

    fs, root = get_fs(root_path)

    if not fs.exists(root):
        return []

    found = []
    pending = [root]

    while pending:
        current = pending.pop()

        for status in fs.listStatus(current):
            path = status.getPath()

            if status.isDirectory():
                pending.append(path)

            elif path.getName().lower().endswith(".parquet"):
                found.append(path.toString())

    return sorted(set(found))


def read_successful_files(log_path):
    """
    Return file paths marked SUCCESS.

    Missing/empty logs mean no successful files yet.
    An existing but unreadable or incompatible log fails visibly.
    """

    fs, path = get_fs(log_path)
    if not fs.exists(path):
        return set()

    parquet_files = list_parquet_files(log_path)
    if not parquet_files:
        raise ValueError(
            f"Processing log exists but contains no Parquet entries: "
            f"{log_path}. Refusing to process files as if the log were empty."
        )

    log_df = spark.read.parquet(log_path)

    required = {"file_path", "status"}
    missing = required - set(log_df.columns)

    if missing:
        raise ValueError(
            f"Invalid processing log {log_path}; "
            f"missing columns: {sorted(missing)}"
        )

    rows = (
        log_df
        .filter(F.col("status") == "SUCCESS")
        .select("file_path")
        .distinct()
        .collect()
    )

    return {
        row["file_path"]
        for row in rows
        if row["file_path"] is not None
    }


def append_gold_success(files):
    """Record files only after all Gold table updates have succeeded."""

    if not files:
        return

    (
        spark.createDataFrame(
            [(path, "SUCCESS") for path in sorted(set(files))],
            "file_path STRING, status STRING",
        )
        .write
        .mode("append")
        .parquet(GOLD_PROCESSING_LOG_PATH)
    )


def get_dates_in_source_files(file_paths):
    """
    Read only reading_ts_utc from Silver-successful source files
    to identify which dates must be recomputed in Gold.

    The processing log's file_path points back to the immutable
    Bronze Parquet input used by the Silver job.
    """

    if not file_paths:
        return set()

    source = spark.read.parquet(*file_paths)

    if "reading_ts_utc" not in source.columns:
        raise ValueError(
            "Source files lack reading_ts_utc: "
            f"{len(file_paths)} file(s)"
        )

    rows = (
        source
        .select(
            F.to_date(
                F.to_timestamp(F.col("reading_ts_utc"))
            ).alias("reading_date")
        )
        .filter(F.col("reading_date").isNotNull())
        .distinct()
        .collect()
    )

    return {
        row["reading_date"].isoformat()
        for row in rows
    }


# =========================================================
# 3. GOLD UPSERT HELPER
# =========================================================

def merge_gold(df, table_name, key_columns, view_name):
    """
    Create an Iceberg Gold table if required; otherwise MERGE
    source rows into existing rows using the supplied business key.
    """

    if df.limit(1).count() == 0:
        print(f"No rows to write: {table_name}")
        return

    df.createOrReplaceTempView(view_name)

    if not spark.catalog.tableExists(table_name):
        spark.sql(f"""
            CREATE TABLE {table_name}
            USING iceberg
            PARTITIONED BY (reading_date)
            AS
            SELECT *
            FROM {view_name}
            WHERE 1 = 0
        """)

        print(f"Created Iceberg table: {table_name}")

    columns = df.columns

    match_condition = " AND ".join(
        f"target.`{key}` = source.`{key}`"
        for key in key_columns
    )

    update_columns = [
        column
        for column in columns
        if column not in key_columns
    ]

    update_clause = ", ".join(
        f"target.`{column}` = source.`{column}`"
        for column in update_columns
    )

    insert_columns = ", ".join(
        f"`{column}`"
        for column in columns
    )

    insert_values = ", ".join(
        f"source.`{column}`"
        for column in columns
    )

    spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING {view_name} AS source
        ON {match_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
    """)

    print(f"MERGE completed: {table_name}")


# =========================================================
# 4. INCREMENTAL INPUT DISCOVERY
# =========================================================

def main():

    print("=" * 80)
    print("SMART ENERGY: SILVER TO GOLD")
    print("=" * 80)

    print("Silver table:", SILVER_TABLE)
    print("Silver processing log:", SILVER_PROCESSING_LOG_PATH)
    print("Gold processing log:", GOLD_PROCESSING_LOG_PATH)

    # The Silver processing log is written by bronze_to_silver.py.
    # Only successfully processed input files are eligible for Gold.
    silver_success_files = read_successful_files(
        SILVER_PROCESSING_LOG_PATH
    )

    gold_success_files = read_successful_files(
        GOLD_PROCESSING_LOG_PATH
    )

    pending_files = sorted(
        silver_success_files - gold_success_files
    )

    print("Files successfully processed by Silver:", len(silver_success_files))
    print("Files already handled by Gold:", len(gold_success_files))
    print("New files to account for in Gold:", len(pending_files))

    if not pending_files:
        print("No new Silver input files. Gold is already up to date.")
        return

    # -----------------------------------------------------
    # Determine every date affected by the newly processed files.
    # -----------------------------------------------------

    affected_dates = get_dates_in_source_files(pending_files)

    print("Affected dates:", sorted(affected_dates))

    # -----------------------------------------------------
    # Read the curated Silver Iceberg table.
    # -----------------------------------------------------

    silver = spark.table(SILVER_TABLE)

    required_columns = {
        "meter_id",
        "reading_ts_utc",
        "interval_minutes",
        "active_energy_kwh_import",
        "active_power_kw",
        "reactive_power_kvar",
        "voltage_v",
        "voltage_v_baseline",
        "apparent_power_kva",
        "transformer_id",
        "feeder_id",
        "substation_id",
        "transformer_rating_kva",
        "anomaly_label",
    }

    missing_columns = required_columns - set(silver.columns)

    if missing_columns:
        raise ValueError(
            "Silver table is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    # The source label is ground truth from the dataset.
    # It is not the same as an alert detected by Flink.
    silver = (
        silver
        .withColumn(
            "reading_date",
            F.to_date(F.col("reading_ts_utc")),
        )
        .withColumn(
            "_ground_truth_anomaly",
            F.when(
                F.col("anomaly_label").cast("boolean") == True,
                F.lit(1).cast("long"),
            ).otherwise(F.lit(0).cast("long")),
        )
    )

    # =====================================================
    # 5. RECOMPUTE ONLY AFFECTED DATES
    # =====================================================

    for date_string in sorted(affected_dates):

        print("\n" + "=" * 80)
        print("Refreshing Gold for date:", date_string)

        # Recompute the complete affected day from Silver.
        # This avoids double-counting when an additional file
        # arrives for a date that Gold already calculated.

        day_df = (
            silver
            .filter(
                F.col("reading_date")
                == F.lit(date_string).cast("date")
            )
            .cache()
        )

        if day_df.limit(1).count() == 0:
            print(
                "No valid Silver rows for this date. "
                "No aggregate rows will be added."
            )
            day_df.unpersist()
            continue

        # =================================================
        # 5.1 DAILY GRID KPIs
        # Table: energy.gold.daily_grid_kpis
        # Grain: one row per date for the entire grid.
        # =================================================

        grid_daily = (
            day_df
            .agg(
                F.sum("active_energy_kwh_import")
                .alias("total_energy_import_kwh"),

                F.avg("voltage_v")
                .alias("avg_voltage_v"),

                F.min("voltage_v")
                .alias("min_voltage_v"),

                F.countDistinct("meter_id")
                .cast("long")
                .alias("active_meter_count"),

                F.count(F.lit(1))
                .cast("long")
                .alias("reading_count"),

                F.sum("_ground_truth_anomaly")
                .cast("long")
                .alias("ground_truth_anomaly_count"),
            )
            .withColumn(
                "reading_date",
                F.lit(date_string).cast("date"),
            )
        )

        # System peak power must be calculated by timestamp:
        # sum concurrent meter loads first, then take the maximum.
        grid_timestamp_load = (
            day_df
            .filter(F.col("reading_ts_utc").isNotNull())
            .groupBy("reading_ts_utc")
            .agg(
                F.sum("active_power_kw")
                .alias("_system_active_load_kw"),

                F.sum("apparent_power_kva")
                .alias("_system_apparent_load_kva"),
            )
        )

        grid_peaks = grid_timestamp_load.agg(
            F.max("_system_active_load_kw")
            .alias("system_peak_active_power_kw"),

            F.max("_system_apparent_load_kva")
            .alias("system_peak_apparent_power_kva"),
        )

        grid_daily = (
            grid_daily
            .crossJoin(grid_peaks)
            .withColumn(
                "calculated_at_utc",
                F.current_timestamp(),
            )
        )

        merge_gold(
            grid_daily,
            GOLD_GRID_DAILY,
            ["reading_date"],
            "gold_grid_daily_source",
        )

        # =================================================
        # 5.2 TRANSFORMER LOADING AT 15-MINUTE INTERVALS
        # Table: energy.gold.transformer_loading_15m
        # Grain: transformer + timestamp.
        # =================================================

        transformer_source = day_df.filter(
            F.col("transformer_id").isNotNull()
            & F.col("reading_ts_utc").isNotNull()
        )

        transformer_interval = (
            transformer_source
            .groupBy(
                "reading_date",
                "reading_ts_utc",
                "transformer_id",
            )
            .agg(
                F.sum("active_power_kw")
                .alias("active_load_kw"),

                F.sum("reactive_power_kvar")
                .alias("reactive_load_kvar"),

                F.sum("apparent_power_kva")
                .alias("apparent_load_kva"),

                # Rating is transformer capacity, not a meter load.
                # MAX avoids summing the same rating across meters.
                F.max("transformer_rating_kva")
                .alias("transformer_rating_kva"),

                F.countDistinct("meter_id")
                .cast("int")
                .alias("meter_count"),

                F.sum("_ground_truth_anomaly")
                .cast("long")
                .alias("ground_truth_anomaly_count"),
            )
            .withColumn(
                "loading_ratio",
                F.when(
                    F.col("transformer_rating_kva") > 0,
                    F.col("apparent_load_kva")
                    / F.col("transformer_rating_kva"),
                ).otherwise(F.lit(None).cast("double")),
            )
            .withColumn(
                "is_overloaded",
                F.col("loading_ratio") > 1.0,
            )
            .withColumn(
                "calculated_at_utc",
                F.current_timestamp(),
            )
        )

        merge_gold(
            transformer_interval,
            GOLD_TRANSFORMER_INTERVAL,
            ["transformer_id", "reading_ts_utc"],
            "gold_transformer_interval_source",
        )

        # =================================================
        # 5.3 DAILY FEEDER HEALTH
        # Table: energy.gold.daily_feeder_health
        # Grain: one feeder per date.
        # =================================================

        feeder_source = day_df.filter(
            F.col("feeder_id").isNotNull()
        )

        # Calculate simultaneous feeder load before identifying
        # the daily peak. Do not take MAX of individual meter loads.
        feeder_timestamp_load = (
            feeder_source
            .filter(F.col("reading_ts_utc").isNotNull())
            .groupBy(
                "reading_date",
                "feeder_id",
                "reading_ts_utc",
            )
            .agg(
                F.sum("active_power_kw")
                .alias("_feeder_active_load_kw")
            )
        )

        feeder_peaks = (
            feeder_timestamp_load
            .groupBy("reading_date", "feeder_id")
            .agg(
                F.max("_feeder_active_load_kw")
                .alias("peak_active_load_kw")
            )
        )

        feeder_daily_base = (
            feeder_source
            .groupBy("reading_date", "feeder_id")
            .agg(
                F.first(
                    "substation_id",
                    ignorenulls=True,
                ).alias("substation_id"),

                F.sum("active_energy_kwh_import")
                .alias("total_energy_import_kwh"),

                F.avg("voltage_v")
                .alias("avg_voltage_v"),

                F.min("voltage_v")
                .alias("min_voltage_v"),

                F.max("voltage_v")
                .alias("max_voltage_v"),

                F.count(F.lit(1))
                .cast("long")
                .alias("reading_count"),

                F.countDistinct("meter_id")
                .cast("long")
                .alias("meter_count"),

                F.sum(
                    F.when(
                        F.col("voltage_v_baseline").isNotNull()
                        & F.col("voltage_v").isNotNull()
                        & (
                            F.col("voltage_v_baseline")
                            - F.col("voltage_v")
                            >= VOLTAGE_DROP_THRESHOLD_V
                        ),
                        F.lit(1),
                    ).otherwise(F.lit(0))
                )
                .cast("long")
                .alias("voltage_drop_reading_count"),
            )
        )

        feeder_daily = (
            feeder_daily_base
            .join(
                feeder_peaks,
                ["reading_date", "feeder_id"],
                "left",
            )
            .withColumn(
                "calculated_at_utc",
                F.current_timestamp(),
            )
        )

        merge_gold(
            feeder_daily,
            GOLD_FEEDER_DAILY,
            ["feeder_id", "reading_date"],
            "gold_feeder_daily_source",
        )

        day_df.unpersist()

    # =====================================================
    # 6. ACKNOWLEDGE INPUT FILES AFTER ALL GOLD WRITES
    # =====================================================

    # If any computation or merge above fails, this code is not
    # reached. The pending files remain eligible on the next run.
    # Reruns recompute the affected dates and MERGE by stable keys.

    append_gold_success(pending_files)

    print("\n" + "=" * 80)
    print("SILVER-TO-GOLD COMPLETED SUCCESSFULLY")
    print("Input files handled:", len(pending_files))
    print("Affected dates:", len(affected_dates))
    print("Gold checkpoint:", GOLD_PROCESSING_LOG_PATH)
    print("=" * 80)


# =========================================================
# 7. EXECUTION
# =========================================================

try:
    # Ensure the target namespace exists in the configured Iceberg catalog.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS energy.gold")
    main()

finally:
    spark.stop()
