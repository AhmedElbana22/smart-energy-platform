import hashlib

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


RAW_JSON_PATH = "hdfs://namenode:8020/smart-energy/bronze/meter-readings"
BRONZE_PARQUET_PATH = (
    "hdfs://namenode:8020/smart-energy/bronze/meter-readings-parquet"
)
PROCESSING_LOG_PATH = (
    "hdfs://namenode:8020/smart-energy/control/json-to-parquet-log"
)

spark = (
    SparkSession.builder
    .appName("SmartEnergy_Bronze_JSON_To_Parquet")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

Path = spark._jvm.org.apache.hadoop.fs.Path
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

json_schema = StructType([
    StructField("meter_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_meter_id", StringType(), True),
    StructField("reading_ts_utc", StringType(), True),
    StructField("interval_minutes", IntegerType(), True),
    StructField("active_energy_kwh_import", DoubleType(), True),
    StructField("active_power_kw", DoubleType(), True),
    StructField("reactive_energy_kvarh_import", DoubleType(), True),
    StructField("reactive_power_kvar", DoubleType(), True),
    StructField("voltage_v_baseline", DoubleType(), True),
    StructField("voltage_v", DoubleType(), True),
    StructField("apparent_power_kva", DoubleType(), True),
    StructField("transformer_id", StringType(), True),
    StructField("feeder_id", StringType(), True),
    StructField("substation_id", StringType(), True),
    StructField("transformer_rating_kva", DoubleType(), True),
    StructField("loading_ratio", DoubleType(), True),
    StructField("anomaly_family", StringType(), True),
    StructField("anomaly_label", StringType(), True),
    StructField("value_status", StringType(), True),
    StructField("quality_status", StringType(), True),
    StructField("generator_seed", LongType(), True),
    StructField("reading_date", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])


def get_fs(path_string):
    path = Path(path_string)
    return path.getFileSystem(hadoop_conf), path


def list_json_files(root_path):
    fs, root = get_fs(root_path)
    if not fs.exists(root):
        raise FileNotFoundError(f"Required HDFS path does not exist: {root_path}")

    discovered = []
    pending = [root]
    while pending:
        current = pending.pop()
        for status in fs.listStatus(current):
            path = status.getPath()
            if status.isDirectory():
                pending.append(path)
            elif path.getName().lower().endswith(".json"):
                discovered.append(path.toString())
    return sorted(set(discovered))


def successful_files():
    fs, log_path = get_fs(PROCESSING_LOG_PATH)
    if not fs.exists(log_path):
        return set()

    log_files = []
    pending = [log_path]
    while pending:
        current = pending.pop()
        for status in fs.listStatus(current):
            path = status.getPath()
            if status.isDirectory():
                pending.append(path)
            elif path.getName().lower().endswith(".parquet"):
                log_files.append(path.toString())

    if not log_files:
        raise ValueError(
            f"Processing log exists but has no readable Parquet entries: "
            f"{PROCESSING_LOG_PATH}. Refusing to replay historical Bronze files."
        )

    log_df = spark.read.parquet(PROCESSING_LOG_PATH)
    if not {"file_path", "status"}.issubset(log_df.columns):
        raise ValueError(
            f"Processing log has an incompatible schema: {PROCESSING_LOG_PATH}"
        )

    rows = (
        log_df
        .filter(F.col("status") == "SUCCESS")
        .select("file_path")
        .distinct()
        .collect()
    )
    return {row["file_path"] for row in rows if row["file_path"] is not None}


def main():
    discovered = list_json_files(RAW_JSON_PATH)
    processed = successful_files()
    pending = [path for path in discovered if path not in processed]

    print("Raw HDFS JSON files discovered:", len(discovered))
    print("Previously promoted successfully:", len(processed))
    print("New immutable JSON files selected:", len(pending))

    if not pending:
        print("No new raw JSON files to promote.")
        return

    for source_path in pending:
        print("Promoting:", source_path)
        source_df = spark.read.schema(json_schema).json(source_path).cache()

        if source_df.filter(F.col("_corrupt_record").isNotNull()).limit(1).count():
            source_df.unpersist()
            raise ValueError(f"Malformed JSON record found in {source_path}")

        label = F.lower(F.trim(F.col("anomaly_label")))
        output_df = (
            source_df
            .drop("_corrupt_record", "reading_date", "partition")
            .withColumn(
                "anomaly_label",
                F.when(label.isin("true", "1"), F.lit(True))
                .when(label.isin("false", "0"), F.lit(False))
                .otherwise(F.lit(None).cast("boolean")),
            )
        )

        row_count = output_df.count()
        file_id = hashlib.sha256(source_path.encode("utf-8")).hexdigest()
        output_path = f"{BRONZE_PARQUET_PATH}/{file_id}"

        if row_count:
            output_df.write.mode("overwrite").parquet(output_path)
        source_df.unpersist()

        success_row = spark.createDataFrame(
            [(source_path, output_path, row_count, "SUCCESS")],
            "file_path STRING, output_path STRING, row_count LONG, status STRING",
        )
        success_row.write.mode("append").parquet(PROCESSING_LOG_PATH)
        print(
            f"Promoted {row_count:,} records to {output_path}; "
            "checkpoint saved."
        )

    print("JSON-to-Parquet promotion finished.")


try:
    main()
finally:
    spark.stop()
