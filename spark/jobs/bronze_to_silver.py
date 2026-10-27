import hashlib
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# =========================================================
# 0. SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("SmartEnergy_Bronze_To_Silver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")


# =========================================================
# 1. PATHS AND TABLE NAMES
# =========================================================

BRONZE_PATH = (
    "hdfs://namenode:8020/smart-energy/bronze/meter-readings-parquet"
)

PROCESSING_LOG_PATH = (
    "hdfs://namenode:8020/smart-energy/control/processing-log"
)

QUALITY_METRICS_PATH = (
    "hdfs://namenode:8020/smart-energy/control/quality-metrics"
)

SILVER_TABLE = "energy.silver.meter_readings"
QUARANTINE_TABLE = "energy.silver.quarantine"

OPTIONAL_COLUMNS = {
    "anomaly_family": "string",
    "anomaly_label": "boolean",
    "generator_seed": "long",
}


# =========================================================
# 2. EXPECTED SOURCE COLUMNS
# =========================================================

EXPECTED_COLUMNS = [
    "meter_id",
    "source_system",
    "source_meter_id",
    "reading_ts_utc",
    "interval_minutes",
    "active_energy_kwh_import",
    "active_power_kw",
    "reactive_energy_kvarh_import",
    "reactive_power_kvar",
    "voltage_v_baseline",
    "voltage_v",
    "apparent_power_kva",
    "transformer_id",
    "feeder_id",
    "substation_id",
    "transformer_rating_kva",
    "loading_ratio",
    "anomaly_family",
    "anomaly_label",
    "value_status",
    "quality_status",
    "generator_seed",
]

STRING_COLUMNS = [
    "meter_id",
    "source_system",
    "source_meter_id",
    "transformer_id",
    "feeder_id",
    "substation_id",
    "anomaly_family",
    "anomaly_label",
    "value_status",
    "quality_status",
]

CATEGORICAL_COLUMNS = [
    "source_system",
    "anomaly_family",
    "anomaly_label",
    "value_status",
    "quality_status",
]

INTEGER_COLUMNS = [
    "interval_minutes",
]

LONG_COLUMNS = [
    "generator_seed",
]

DOUBLE_COLUMNS = [
    "active_energy_kwh_import",
    "active_power_kw",
    "reactive_energy_kvarh_import",
    "reactive_power_kvar",
    "voltage_v_baseline",
    "voltage_v",
    "apparent_power_kva",
    "transformer_rating_kva",
    "loading_ratio",
]

REQUIRED_COLUMNS = [
    "meter_id",
    "reading_ts_utc",
    "interval_minutes",
]


# =========================================================
# 3. HDFS UTILITIES
# =========================================================

# Use Hadoop's filesystem API to discover paths without
# reading the contents of every Bronze file.

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
Path = spark._jvm.org.apache.hadoop.fs.Path

bronze_root = Path(BRONZE_PATH)
bronze_fs = bronze_root.getFileSystem(hadoop_conf)


def list_parquet_files(root_path):
    """Return all Parquet file paths recursively from HDFS."""

    root = Path(root_path)
    fs = root.getFileSystem(hadoop_conf)

    if not fs.exists(root):
        raise FileNotFoundError(
            f"Required HDFS path does not exist: {root_path}"
        )

    discovered = []
    pending = [root]

    while pending:
        current = pending.pop()

        for status in fs.listStatus(current):
            path = status.getPath()

            if status.isDirectory():
                pending.append(path)

            elif path.getName().lower().endswith(".parquet"):
                discovered.append(path.toString())

    return sorted(set(discovered))


# =========================================================
# 4. READ THE PROCESSING LOG SAFELY
# =========================================================

print("=" * 80)
print("BRONZE-TO-SILVER INCREMENTAL PROCESSING")
print("Bronze:", BRONZE_PATH)
print("Processing log:", PROCESSING_LOG_PATH)
print("=" * 80)

# Discover current Bronze files.
all_files = list_parquet_files(BRONZE_PATH)

print("Total Bronze Parquet files discovered:", len(all_files))

# Check whether the log contains actual Parquet data.
log_path = Path(PROCESSING_LOG_PATH)
log_fs = log_path.getFileSystem(hadoop_conf)

if log_fs.exists(log_path):
    log_files = list_parquet_files(PROCESSING_LOG_PATH)
    if not log_files:
        raise ValueError(
            "Processing log exists but contains no Parquet entries: "
            f"{PROCESSING_LOG_PATH}. Refusing to reprocess historical files."
        )
else:
    log_files = []

if log_files:
    # Do not catch read/schema errors here. Reprocessing all
    # historical files because the log is unreadable is unsafe.
    processing_log = spark.read.parquet(PROCESSING_LOG_PATH)

    required_log_columns = {"file_path", "status"}
    actual_log_columns = set(processing_log.columns)

    if not required_log_columns.issubset(actual_log_columns):
        raise ValueError(
            "Processing log has an incompatible schema. "
            f"Expected at least {sorted(required_log_columns)}; "
            f"found {sorted(actual_log_columns)}."
        )

    processed_rows = (
        processing_log
        .filter(F.col("status") == "SUCCESS")
        .select("file_path")
        .distinct()
        .collect()
    )

    processed_files = {
        row["file_path"]
        for row in processed_rows
        if row["file_path"] is not None
    }

    print("Previously successful files:", len(processed_files))

else:
    # A genuinely absent or empty log has no successful entries.
    # An existing but unreadable log will raise an error above.
    processed_files = set()
    print("No processing-log entries found. Initial processing run.")

# Only files whose exact HDFS paths are not marked SUCCESS.
reprocess_files = {
    file_path.strip()
    for file_path in os.environ.get(
        "BRONZE_TO_SILVER_REPROCESS_FILES",
        "",
    ).split(",")
    if file_path.strip()
}

unknown_reprocess_files = reprocess_files - set(all_files)
if unknown_reprocess_files:
    raise ValueError(
        "Requested recovery files are not present in Bronze: "
        f"{sorted(unknown_reprocess_files)}"
    )

if reprocess_files:
    print(
        "Explicitly reprocessing files for checkpoint/metrics recovery:",
        len(reprocess_files),
    )
    for file_path in sorted(reprocess_files):
        print("Recovery input:", file_path)

new_files = [
    file_path
    for file_path in all_files
    if file_path not in processed_files or file_path in reprocess_files
]

print("New files to process:", len(new_files))

if not new_files:
    print("No new Bronze files. Silver is already up to date.")
    spark.stop()
    raise SystemExit(0)


# =========================================================
# 5. PROCESS EACH NEW FILE
# =========================================================

for file_path in new_files:

    print("\n" + "=" * 80)
    print("Processing:", file_path)

    # -----------------------------------------------------
    # 5.1 READ ONE BRONZE PARQUET FILE
    # -----------------------------------------------------

    df = spark.read.parquet(file_path)

    # -----------------------------------------------------
    # 5.2 VALIDATE SOURCE SCHEMA
    # -----------------------------------------------------

    actual_columns = set(df.columns)
    expected_columns = set(EXPECTED_COLUMNS)
    allowed_extra_columns = {"reading_date", "partition"}

    missing_columns = expected_columns - actual_columns - set(OPTIONAL_COLUMNS)
    unexpected_columns = (
        actual_columns - expected_columns - allowed_extra_columns
    )

    if missing_columns:
        raise ValueError(
            f"Missing columns in {file_path}: "
            f"{sorted(missing_columns)}"
        )

    if unexpected_columns:
        raise ValueError(
            f"Unexpected columns in {file_path}: "
            f"{sorted(unexpected_columns)}"
        )

    for column, data_type in OPTIONAL_COLUMNS.items():
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast(data_type))

    extra_metadata_columns = [
        column
        for column in allowed_extra_columns
        if column in df.columns
    ]
    if extra_metadata_columns:
        df = df.drop(*extra_metadata_columns)

    # -----------------------------------------------------
    # 5.3 CLEAN STRING COLUMNS
    # -----------------------------------------------------

    for column in STRING_COLUMNS:
        df = df.withColumn(
            column,
            F.trim(F.col(column).cast("string"))
        )

        df = df.withColumn(
            column,
            F.when(
                F.col(column) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.col(column))
        )

    # -----------------------------------------------------
    # 5.4 STANDARDIZE CATEGORICAL VALUES
    # -----------------------------------------------------

    for column in CATEGORICAL_COLUMNS:
        df = df.withColumn(
            column,
            F.lower(F.col(column))
        )

    # -----------------------------------------------------
    # 5.5 CAST INTEGER COLUMNS
    # -----------------------------------------------------

    for column in INTEGER_COLUMNS:
        df = df.withColumn(
            column,
            F.col(column).cast("int")
        )

    # -----------------------------------------------------
    # 5.6 CAST LONG COLUMNS
    # -----------------------------------------------------

    for column in LONG_COLUMNS:
        df = df.withColumn(
            column,
            F.col(column).cast("long")
        )

    # -----------------------------------------------------
    # 5.7 CAST NUMERIC COLUMNS
    # -----------------------------------------------------

    for column in DOUBLE_COLUMNS:
        df = df.withColumn(
            column,
            F.col(column).cast("double")
        )

    # -----------------------------------------------------
    # 5.8 CONVERT TIMESTAMP
    # -----------------------------------------------------

    df = df.withColumn(
        "reading_ts_utc",
        F.to_timestamp("reading_ts_utc")
    )

    # -----------------------------------------------------
    # 5.9 CONVERT ANOMALY LABEL TO BOOLEAN
    # -----------------------------------------------------

    label = F.lower(F.trim(F.col("anomaly_label").cast("string")))
    df = df.withColumn(
        "anomaly_label",
        F.when(label.isin("true", "1"), F.lit(True))
        .when(label.isin("false", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean")),
    )

    # -----------------------------------------------------
    # 5.10 REMOVE EXACT DUPLICATES
    # -----------------------------------------------------

    pre_dedup_df = df.cache()
    rows_before = pre_dedup_df.count()
    df = pre_dedup_df.dropDuplicates().cache()

    rows_after = df.count()
    pre_dedup_df.unpersist()

    print("Rows before deduplication:", rows_before)
    print("Rows after deduplication:", rows_after)
    print("Exact duplicates removed:", rows_before - rows_after)

    # =====================================================
    # 6. DATA QUALITY CHECKS
    # =====================================================

    print("\n========== DATA QUALITY CHECKS ==========")

    invalid_interval = (
        F.col("interval_minutes").isNull()
        | (F.col("interval_minutes") <= 0)
    )
    invalid_energy = (
        F.col("active_energy_kwh_import").isNull()
        | (F.col("active_energy_kwh_import") < 0)
    )
    quality_aggregates = [
        F.count(
            F.when(F.col(column).isNull(), F.lit(1))
        ).alias(f"null_{column}")
        for column in REQUIRED_COLUMNS
    ]
    quality_aggregates.extend([
        F.count(F.when(invalid_interval, F.lit(1)))
        .alias("invalid_interval_count"),
        F.count(F.when(invalid_energy, F.lit(1)))
        .alias("invalid_energy_count"),
    ])
    quality_counts = df.agg(*quality_aggregates).first()

    for column in REQUIRED_COLUMNS:
        null_count = quality_counts[f"null_{column}"]
        print(f"{column} null values: {null_count}")

    invalid_interval_count = quality_counts["invalid_interval_count"]
    print("Invalid intervals:", invalid_interval_count)

    invalid_energy_count = quality_counts["invalid_energy_count"]
    print("Missing or negative energy:", invalid_energy_count)

    # =====================================================
    # 7. DEFINE VALID RECORDS
    # =====================================================

    valid_condition = (
        F.col("meter_id").isNotNull()
        & (F.col("meter_id") != "")
        & F.col("reading_ts_utc").isNotNull()
        & F.col("interval_minutes").isNotNull()
        & (F.col("interval_minutes") > 0)
        & F.col("active_energy_kwh_import").isNotNull()
        & (F.col("active_energy_kwh_import") >= 0)
    )

    duplicate_keys = (
        df
        .filter(
            F.col("meter_id").isNotNull()
            & F.col("reading_ts_utc").isNotNull()
        )
        .groupBy("meter_id", "reading_ts_utc")
        .count()
        .filter(F.col("count") > 1)
        .select("meter_id", "reading_ts_utc")
        .withColumn("_duplicate_business_key", F.lit(True))
    )

    checked_df = (
        df
        .join(duplicate_keys, ["meter_id", "reading_ts_utc"], "left")
        .withColumn(
            "_duplicate_business_key",
            F.coalesce(F.col("_duplicate_business_key"), F.lit(False)),
        )
    ).cache()

    classification_counts = checked_df.agg(
        F.count(
            F.when(F.col("_duplicate_business_key"), F.lit(1))
        ).alias("conflicting_rows"),
        F.count(
            F.when(
                valid_condition & ~F.col("_duplicate_business_key"),
                F.lit(1),
            )
        ).alias("accepted_rows"),
        F.count(
            F.when(
                ~valid_condition | F.col("_duplicate_business_key"),
                F.lit(1),
            )
        ).alias("rejected_rows"),
    ).first()
    conflicting_rows = classification_counts["conflicting_rows"]

    valid_df = (
        checked_df
        .filter(valid_condition & ~F.col("_duplicate_business_key"))
        .drop("_duplicate_business_key")
    )

    invalid_df = (
        checked_df
        .filter(~valid_condition | F.col("_duplicate_business_key"))
        .withColumn("source_file", F.lit(file_path))
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                "; ",
                F.when(
                    F.col("meter_id").isNull()
                    | (F.col("meter_id") == ""),
                    F.lit("missing_meter_id")
                ),
                F.when(
                    F.col("reading_ts_utc").isNull(),
                    F.lit("missing_reading_timestamp")
                ),
                F.when(
                    F.col("interval_minutes").isNull()
                    | (F.col("interval_minutes") <= 0),
                    F.lit("invalid_interval")
                ),
                F.when(
                    F.col("active_energy_kwh_import").isNull()
                    | (F.col("active_energy_kwh_import") < 0),
                    F.lit("invalid_energy")
                ),
                F.when(
                    F.col("_duplicate_business_key"),
                    F.lit("duplicate_meter_timestamp_key"),
                ),
            )
        )
        .drop("_duplicate_business_key")
    )

    quarantine_columns = invalid_df.columns
    invalid_df = invalid_df.withColumn(
        "quarantine_id",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(column) for column in quarantine_columns])
            ),
            256,
        ),
    )

    valid_count = classification_counts["accepted_rows"]
    invalid_count = classification_counts["rejected_rows"]

    print("Valid rows:", valid_count)
    print("Invalid rows:", invalid_count)
    print("Conflicting meter/timestamp rows:", conflicting_rows)

    metrics_path = (
        f"{QUALITY_METRICS_PATH}/"
        f"{hashlib.sha256(file_path.encode('utf-8')).hexdigest()}"
    )
    metrics = spark.createDataFrame(
        [(
            file_path,
            rows_before,
            rows_before - rows_after,
            conflicting_rows,
            valid_count,
            invalid_count,
            quality_counts["null_meter_id"],
            quality_counts["null_reading_ts_utc"],
            invalid_interval_count,
            invalid_energy_count,
            datetime.now(timezone.utc).isoformat(),
        )],
        """
        source_file STRING,
        input_rows LONG,
        exact_duplicate_rows_removed LONG,
        conflicting_business_key_rows LONG,
        accepted_rows LONG,
        rejected_rows LONG,
        missing_meter_id_rows LONG,
        missing_timestamp_rows LONG,
        invalid_interval_rows LONG,
        invalid_energy_rows LONG,
        measured_at_utc STRING
        """,
    )
    metrics.write.mode("overwrite").parquet(metrics_path)
    print("Quality metrics persisted:", metrics_path)

    # =====================================================
    # 8. CREATE SILVER ICEBERG TABLE IF NEEDED
    # =====================================================

    spark.sql("CREATE NAMESPACE IF NOT EXISTS energy.silver")
    valid_df.createOrReplaceTempView("silver_incoming")

    if not spark.catalog.tableExists(SILVER_TABLE):
        spark.sql(f"""
            CREATE TABLE {SILVER_TABLE}
            USING iceberg
            PARTITIONED BY (days(reading_ts_utc))
            AS
            SELECT *
            FROM silver_incoming
            WHERE 1 = 0
        """)

        print("Silver Iceberg table created.")

    # =====================================================
    # 9. CREATE QUARANTINE TABLE IF NEEDED
    # =====================================================

    if invalid_count > 0:
        invalid_df.createOrReplaceTempView("quarantine_incoming")

        if not spark.catalog.tableExists(QUARANTINE_TABLE):
            spark.sql(f"""
                CREATE TABLE {QUARANTINE_TABLE}
                USING iceberg
                PARTITIONED BY (days(reading_ts_utc))
                AS
                SELECT *
                FROM quarantine_incoming
                WHERE 1 = 0
            """)

            print("Quarantine Iceberg table created.")

        quarantine_schema = spark.table(QUARANTINE_TABLE).schema
        if "quarantine_id" not in {
            field.name for field in quarantine_schema.fields
        }:
            spark.sql(
                f"ALTER TABLE {QUARANTINE_TABLE} "
                "ADD COLUMNS (quarantine_id STRING)"
            )
            quarantine_schema = spark.table(QUARANTINE_TABLE).schema

        for field in quarantine_schema.fields:
            if field.name not in invalid_df.columns:
                invalid_df = invalid_df.withColumn(
                    field.name,
                    F.lit(None).cast(field.dataType),
                )
            else:
                invalid_df = invalid_df.withColumn(
                    field.name,
                    F.col(field.name).cast(field.dataType),
                )

        quarantine_columns = [
            field.name for field in quarantine_schema.fields
        ]
        invalid_df = invalid_df.select(*quarantine_columns)
        invalid_df.createOrReplaceTempView("quarantine_incoming")

        spark.sql(f"""
            MERGE INTO {QUARANTINE_TABLE} AS target
            USING quarantine_incoming AS source
            ON target.quarantine_id = source.quarantine_id
            WHEN NOT MATCHED THEN
                INSERT *
        """)

        print("Invalid records written to Quarantine.")

    # =====================================================
    # 10. ALIGN VALID DATA WITH SILVER SCHEMA
    # =====================================================

    if valid_count > 0:
        target_schema = spark.table(SILVER_TABLE).schema

        target_columns = [
            field.name
            for field in target_schema.fields
        ]

        for field in target_schema.fields:
            if field.name not in valid_df.columns:
                raise ValueError(
                    f"Silver column {field.name} is missing from "
                    f"the incoming data for {file_path}."
                )

            valid_df = valid_df.withColumn(
                field.name,
                F.col(field.name).cast(field.dataType)
            )

        valid_df = valid_df.select(*target_columns)

        # =================================================
        # 11. MERGE INTO SILVER ICEBERG
        # =================================================

        valid_df.createOrReplaceTempView("silver_incoming")

        spark.sql(f"""
            MERGE INTO {SILVER_TABLE} AS target
            USING silver_incoming AS source
            ON target.meter_id = source.meter_id
               AND target.reading_ts_utc = source.reading_ts_utc
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)

        print("Valid records merged into Silver.")

    else:
        print("No valid records to merge into Silver.")

    df.unpersist()
    checked_df.unpersist()

    # =====================================================
    # 12. RECORD SUCCESS IN THE PROCESSING LOG
    # =====================================================

    # Write SUCCESS only after all processing steps above finish.
    # If any step fails, the exception stops the job and this
    # file is not marked successful, so a future run can retry it.

    success_row = spark.createDataFrame(
        [(file_path, "SUCCESS")],
        """
        file_path STRING,
        status STRING
        """
    )

    (
        success_row
        .write
        .mode("append")
        .parquet(PROCESSING_LOG_PATH)
    )

    print("Processing Log updated: SUCCESS")
    print("Completed:", file_path)
    print("=" * 80)


# =========================================================
# 13. END
# =========================================================

print("\nBronze-to-Silver incremental processing finished.")
print("Files processed in this run:", len(new_files))

spark.stop()