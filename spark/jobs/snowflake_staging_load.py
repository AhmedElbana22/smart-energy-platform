import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_TABLE = "energy.silver.meter_readings"
SILVER_LOG_PATH = (
    "hdfs://namenode:8020/smart-energy/control/processing-log"
)
GOLD_LOG_PATH = (
    "hdfs://namenode:8020/smart-energy/control/"
    "silver-to-gold-processing-log"
)
LOAD_LOG_PATH = (
    "hdfs://namenode:8020/smart-energy/control/"
    "snowflake-staging-load-log"
)
QUALITY_METRICS_PATH = (
    "hdfs://namenode:8020/smart-energy/control/quality-metrics"
)

GOLD_TABLES = {
    "energy.gold.daily_grid_kpis": (
        "STG_GOLD_DAILY_GRID_KPIS",
        ["reading_date"],
    ),
    "energy.gold.transformer_loading_15m": (
        "STG_GOLD_TRANSFORMER_LOADING_15M",
        ["transformer_id", "reading_ts_utc"],
    ),
    "energy.gold.daily_feeder_health": (
        "STG_GOLD_DAILY_FEEDER_HEALTH",
        ["feeder_id", "reading_date"],
    ),
}

spark = (
    SparkSession.builder
    .appName("SmartEnergy_Snowflake_Staging_Load")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

Path = spark._jvm.org.apache.hadoop.fs.Path
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()


def get_fs(path_string):
    path = Path(path_string)
    return path.getFileSystem(hadoop_conf), path


def list_parquet_files(root_path):
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
    fs, path = get_fs(log_path)
    if not fs.exists(path):
        return set()

    if not list_parquet_files(log_path):
        raise ValueError(
            f"Processing log exists but has no readable Parquet entries: "
            f"{log_path}"
        )

    log_df = spark.read.parquet(log_path)
    if not {"file_path", "status"}.issubset(log_df.columns):
        raise ValueError(f"Processing log has an incompatible schema: {log_path}")

    return {
        row["file_path"]
        for row in (
            log_df
            .filter(F.col("status") == "SUCCESS")
            .select("file_path")
            .distinct()
            .collect()
        )
        if row["file_path"] is not None
    }


def new_event_keys(source_files):
    if not source_files:
        return spark.createDataFrame(
            [],
            "meter_id STRING, reading_ts_utc TIMESTAMP",
        ), set(), []

    source = spark.read.parquet(*source_files)
    required = {"meter_id", "reading_ts_utc"}
    if not required.issubset(source.columns):
        raise ValueError(
            "Silver source files lack event key columns: "
            f"{len(source_files)} file(s)"
        )

    keys = (
        source
        .select(
            "meter_id",
            F.to_timestamp("reading_ts_utc").alias("reading_ts_utc"),
        )
        .filter(
            F.col("meter_id").isNotNull()
            & F.col("reading_ts_utc").isNotNull()
        )
        .dropDuplicates()
        .cache()
    )
    timestamps = [
        row["reading_ts_utc"]
        for row in (
            keys.select("reading_ts_utc")
            .distinct()
            .collect()
        )
    ]
    dates = {timestamp.date().isoformat() for timestamp in timestamps}
    return keys, dates, timestamps


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def snowflake_connection():
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise ValueError(
            "Missing required Snowflake environment variables: "
            + ", ".join(missing)
        )

    account = os.environ["SNOWFLAKE_ACCOUNT"].strip()
    if account.startswith("https://"):
        account = account[len("https://"):]
    elif account.startswith("http://"):
        account = account[len("http://"):]
    account = account.rstrip("/")
    if not account.endswith(".snowflakecomputing.com"):
        account = f"{account}.snowflakecomputing.com"

    raw_schema = (
        os.environ.get("SNOWFLAKE_RAW_SCHEMA")
        or os.environ.get("SNOWFLAKE_SCHEMA")
        or "RAW"
    )
    database = os.environ["SNOWFLAKE_DATABASE"]

    options = {
        "sfURL": account,
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfDatabase": database,
        "sfSchema": raw_schema,
        "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    }
    if os.environ.get("SNOWFLAKE_ROLE"):
        options["sfRole"] = os.environ["SNOWFLAKE_ROLE"]

    properties = spark._jvm.java.util.Properties()
    properties.setProperty("user", options["sfUser"])
    properties.setProperty("password", options["sfPassword"])
    properties.setProperty("warehouse", options["sfWarehouse"])
    properties.setProperty("db", database)
    properties.setProperty("schema", raw_schema)
    if "sfRole" in options:
        properties.setProperty("role", options["sfRole"])

    driver_loader = (
        spark._jvm.java.lang.Thread.currentThread()
        .getContextClassLoader()
    )
    driver_class = driver_loader.loadClass(
        "net.snowflake.client.jdbc.SnowflakeDriver"
    )
    driver = driver_class.newInstance()
    jdbc_url = f"jdbc:snowflake://{account}"
    connection = driver.connect(jdbc_url, properties)
    if connection is None:
        raise RuntimeError("Snowflake JDBC driver rejected the configured URL")
    return options, connection, database, raw_schema


def table_reference(database, schema, table):
    return ".".join(
        quote_identifier(part)
        for part in (database, schema, table)
    )


def table_exists(statement, database, schema, table):
    schema_ref = ".".join(
        quote_identifier(part)
        for part in (database, schema)
    )
    result = statement.executeQuery(
        f"SHOW TABLES LIKE '{table}' IN SCHEMA {schema_ref}"
    )
    try:
        while result.next():
            if result.getString(2).upper() == table.upper():
                return True
        return False
    finally:
        result.close()


def row_count(statement, reference):
    result = statement.executeQuery(f"SELECT COUNT(*) FROM {reference}")
    try:
        if not result.next():
            raise RuntimeError(f"Snowflake returned no count for {reference}")
        return result.getLong(1)
    finally:
        result.close()


def merge_to_snowflake(df, target_name, key_columns, options, statement,
                       database, schema):
    if df.limit(1).count() == 0:
        print(f"No rows to stage: {target_name}")
        return None

    target_ref = table_reference(database, schema, target_name)
    staging_name = f"_LOAD_{target_name}"
    staging_ref = table_reference(database, schema, staging_name)
    exists_before = table_exists(
        statement,
        database,
        schema,
        target_name,
    )
    before_count = row_count(statement, target_ref) if exists_before else 0

    (
        df.write
        .format("net.snowflake.spark.snowflake")
        .options(**options)
        .option("dbtable", staging_name)
        .mode("overwrite")
        .save()
    )

    if not exists_before:
        statement.execute(
            f"CREATE TABLE IF NOT EXISTS {target_ref} LIKE {staging_ref}"
        )

    staged_count = row_count(statement, staging_ref)
    columns = df.columns
    quoted_columns = [quote_identifier(column.upper()) for column in columns]
    key_set = {column.lower() for column in key_columns}
    update_columns = [
        column for column in columns
        if column.lower() not in key_set
    ]
    on_clause = " AND ".join(
        f"target.{quote_identifier(key.upper())} = "
        f"source.{quote_identifier(key.upper())}"
        for key in key_columns
    )
    update_clause = ", ".join(
        f"target.{quote_identifier(column.upper())} = "
        f"source.{quote_identifier(column.upper())}"
        for column in update_columns
    )
    insert_columns = ", ".join(quoted_columns)
    insert_values = ", ".join(
        f"source.{column}" for column in quoted_columns
    )

    statement.execute(
        f"""
        MERGE INTO {target_ref} AS target
        USING {staging_ref} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
        """
    )

    after_count = row_count(statement, target_ref)
    statement.execute(f"DROP TABLE {staging_ref}")
    print(
        f"{target_name}: staged={staged_count:,}, "
        f"target_before={before_count:,}, target_after={after_count:,}"
    )
    return staged_count


def append_load_success(files):
    if not files:
        return
    (
        spark.createDataFrame(
            [(path, "SUCCESS") for path in sorted(files)],
            "file_path STRING, status STRING",
        )
        .write
        .mode("append")
        .parquet(LOAD_LOG_PATH)
    )


def main():
    silver_success = read_successful_files(SILVER_LOG_PATH)
    gold_success = read_successful_files(GOLD_LOG_PATH)
    loaded_success = read_successful_files(LOAD_LOG_PATH)
    gold_completed = silver_success & gold_success
    pending = sorted(gold_completed - loaded_success)

    print("Silver-successful source files:", len(silver_success))
    print("Gold-successful source files:", len(gold_success))
    print("Snowflake-loaded source files:", len(loaded_success))
    print("Gold-complete source files:", len(gold_completed))
    print("New source files to load:", len(pending))
    if not pending:
        print("No new successful Silver inputs. Snowflake staging is current.")
        return

    event_keys, dates, timestamps = new_event_keys(pending)
    print("New event keys:", event_keys.count())
    print("Affected dates:", sorted(dates))
    print("Affected timestamps:", len(timestamps))

    options, connection, database, schema = snowflake_connection()
    statement = connection.createStatement()
    try:
        silver = spark.table(SILVER_TABLE).join(
            event_keys,
            ["meter_id", "reading_ts_utc"],
            "inner",
        )
        silver_staged = merge_to_snowflake(
            silver,
            "STG_SILVER_METER_READINGS",
            ["meter_id", "reading_ts_utc"],
            options,
            statement,
            database,
            schema,
        )

        if silver_staged:
            for iceberg_name, (target_name, keys) in GOLD_TABLES.items():
                if not spark.catalog.tableExists(iceberg_name):
                    raise ValueError(
                        f"Expected Gold Iceberg table does not exist: "
                        f"{iceberg_name}"
                    )
                gold = spark.table(iceberg_name)
                if iceberg_name == "energy.gold.transformer_loading_15m":
                    gold = gold.filter(
                        F.col("reading_ts_utc").isin(timestamps)
                    )
                else:
                    gold = gold.filter(
                        F.col("reading_date").isin(sorted(dates))
                    )
                merge_to_snowflake(
                    gold,
                    target_name,
                    keys,
                    options,
                    statement,
                    database,
                    schema,
                )
        else:
            print("No new clean Silver records; skipping Gold staging.")

        metrics_files = list_parquet_files(QUALITY_METRICS_PATH)
        if not metrics_files:
            raise ValueError(
                "No persisted quality metrics found for successful Silver "
                f"inputs under {QUALITY_METRICS_PATH}"
            )
        metrics = spark.read.parquet(*metrics_files)
        if "source_file" not in metrics.columns:
            raise ValueError(
                "Quality metrics are missing their source_file business key"
            )
        metrics = metrics.filter(F.col("source_file").isin(pending))
        metric_sources = {
            row["source_file"]
            for row in metrics.select("source_file").distinct().collect()
        }
        missing_metrics = sorted(set(pending) - metric_sources)
        if missing_metrics:
            raise ValueError(
                "Quality metrics are missing for "
                f"{len(missing_metrics)} successful Silver source files"
            )
        merge_to_snowflake(
            metrics,
            "STG_PIPELINE_QUALITY_METRICS",
            ["source_file"],
            options,
            statement,
            database,
            schema,
        )
    finally:
        statement.close()
        connection.close()

    append_load_success(pending)
    event_keys.unpersist()
    print("Snowflake staging load completed and checkpointed.")


try:
    main()
finally:
    spark.stop()
