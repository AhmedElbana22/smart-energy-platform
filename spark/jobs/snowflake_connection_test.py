import os
from pyspark.sql import SparkSession

account = os.environ["SNOWFLAKE_ACCOUNT"].strip()
url = account if account.endswith(".snowflakecomputing.com") else account + ".snowflakecomputing.com"

options = {
    "sfURL": url,
    "sfUser": os.environ["SNOWFLAKE_USER"],
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
    "sfSchema": os.environ.get("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "sfRole": os.environ["SNOWFLAKE_ROLE"],
}

spark = SparkSession.builder.appName("SnowflakeConnectionTest").getOrCreate()

try:
    print("Testing Snowflake connection...")
    result = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**options)
        .option("query", "SELECT CURRENT_VERSION() AS VERSION")
        .load()
    )
    result.show(truncate=False)
    print("SNOWFLAKE CONNECTION TEST PASSED")
finally:
    spark.stop()
