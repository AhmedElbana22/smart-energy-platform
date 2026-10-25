
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)


def required_env(name):
    """Read a required environment variable without exposing its value."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Required environment variable {name} is missing.")
    return value


def main():
    # Read Snowflake connection settings from the existing .env file.
    account = required_env("SNOWFLAKE_ACCOUNT")
    database = required_env("SNOWFLAKE_DATABASE")
    warehouse = required_env("SNOWFLAKE_WAREHOUSE")
    role = required_env("SNOWFLAKE_ROLE")
    user = required_env("SNOWFLAKE_USER")
    password = required_env("SNOWFLAKE_PASSWORD")

    # This test targets the table created manually in Snowflake.
    schema_name = "PUBLIC"
    table_name = "SPARK_WRITE_TEST"

    # Accept either an account identifier or a Snowflake hostname.
    account = account.removeprefix("https://").removeprefix("http://")
    account = account.rstrip("/")
    if not account.endswith(".snowflakecomputing.com"):
        account = f"{account}.snowflakecomputing.com"

    options = {
        "sfURL": account,
        "sfUser": user,
        "sfPassword": password,
        "sfDatabase": database,
        "sfSchema": schema_name,
        "sfWarehouse": warehouse,
        "sfRole": role,
    }

    spark = (
        SparkSession.builder
        .appName("SnowflakeWriteTest")
        .getOrCreate()
    )

    try:
        print("=" * 60)
        print("SPARK -> SNOWFLAKE WRITE TEST")
        print("=" * 60)
        print(f"Database: {database}")
        print(f"Schema:   {schema_name}")
        print(f"Table:    {table_name}")
        print(f"Role:     {role}")
        print(f"Warehouse:{warehouse}")

        # Check the active Snowflake session context.
        print("\nChecking Snowflake session context...")

        context_df = (
            spark.read
            .format("net.snowflake.spark.snowflake")
            .options(**options)
            .option(
                "query",
                """
                SELECT
                    CURRENT_DATABASE() AS CURRENT_DATABASE,
                    CURRENT_SCHEMA() AS CURRENT_SCHEMA,
                    CURRENT_ROLE() AS CURRENT_ROLE,
                    CURRENT_WAREHOUSE() AS CURRENT_WAREHOUSE
                """
            )
            .load()
        )

        context_df.show(truncate=False)

        # Prepare two synthetic records.
        print("Preparing test records...")

        records = [
            (1, "spark_write_test_A"),
            (2, "spark_write_test_B"),
        ]

        test_schema = StructType([
            StructField("TEST_ID", IntegerType(), nullable=False),
            StructField("TEST_VALUE", StringType(), nullable=False),
        ])

        test_df = spark.createDataFrame(records, test_schema)

        test_df.show(truncate=False)

        # Append to the table that already exists in Snowflake.
        print(
            f"Writing records to "
            f"{database}.{schema_name}.{table_name}..."
        )

        (
            test_df.write
            .format("net.snowflake.spark.snowflake")
            .options(**options)
            .option("dbtable", table_name)
            .mode("append")
            .save()
        )

        print("WRITE COMPLETED.")

        # Read the records back from the existing Snowflake table.
        print("\nReading the test records back from Snowflake...")

        verification_df = (
            spark.read
            .format("net.snowflake.spark.snowflake")
            .options(**options)
            .option(
                "query",
                f"""
                SELECT TEST_ID, TEST_VALUE
                FROM {database}.{schema_name}.{table_name}
                WHERE TEST_VALUE IN (
                    'spark_write_test_A',
                    'spark_write_test_B'
                )
                ORDER BY TEST_ID
                """
            )
            .load()
        )

        verification_df.show(truncate=False)

        actual_count = verification_df.count()

        if actual_count >= 2:
            print("\nSNOWFLAKE WRITE TEST PASSED.")
            print(f"Verified matching records: {actual_count}")
        else:
            raise RuntimeError(
                "Write completed, but fewer than two matching "
                f"records were found. Found: {actual_count}"
            )

    except Exception as exc:
        print("\nSNOWFLAKE WRITE TEST FAILED.", file=sys.stderr)
        print(f"Error type: {type(exc).__name__}", file=sys.stderr)
        print(f"Error: {exc}", file=sys.stderr)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
