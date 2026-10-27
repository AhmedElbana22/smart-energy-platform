# Kafka Connect to HDFS Bronze

The Connect image pins Confluent HDFS 3 Sink 3.0.24. Start Kafka, HDFS,
and the Connect worker, then register the sink once:

```powershell
docker compose --profile batch --profile connect -f docker-compose.yml up -d kafka namenode datanode kafka-connect
Invoke-RestMethod http://localhost:8083/connectors
```

If `meter-readings-hdfs-sink` is not listed, register it:

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  --data-binary "@connect/hdfs3-sink.json"
```

The connector writes closed JSON files under
`/smart-energy/bronze/meter-readings`. It uses HDFS write-ahead logs and
offset-bearing output filenames for recovery. The configured JSON
converter preserves the existing schemaless JSON contract consumed by
Flink. Parquet output cannot be written directly from this plain-JSON
topic with the connector's Parquet formatter; the Parquet formatter
requires schema-aware records. Promote closed JSON files into immutable
Parquet batches with:

```powershell
$SparkSubmit = @(
  '--rm',
  '--network', 'container:smart-energy-platform-spark-master-1',
  '--volumes-from', 'smart-energy-platform-spark-master-1',
  '--user', '185',
  '--entrypoint', '/opt/spark/bin/spark-submit',
  'smart-energy-spark-iceberg:3.5.9-1.9.2',
  '--master', 'spark://spark-master:7077',
  '--deploy-mode', 'client',
  '--conf', 'spark.driver.host=spark-master'
)
docker run @SparkSubmit /opt/spark/jobs/bronze_json_to_parquet.py
```

The sink's flush size is set to 20,000 records to avoid the tiny-file
explosion caused by a low test flush size. Connector task status and
HDFS output should be checked before running the Parquet promotion job.
Wait until Kafka's consumer offset matches its log-end offset and the
closed HDFS file ranges cover those offsets. Then run the transformations
in order:

```powershell
docker run @SparkSubmit /opt/spark/jobs/bronze_to_silver.py
docker run @SparkSubmit /opt/spark/jobs/silver_to_gold.py
```

If HDFS `fsck` identifies corrupt processing-log or quality-metric files,
preserve those files outside the active log directories and pass only the
confirmed affected immutable Bronze paths in the comma-separated
`BRONZE_TO_SILVER_REPROCESS_FILES` environment variable. The job reports each
forced input and uses the existing idempotent Silver/Quarantine merges and
per-source metric overwrite.

The Spark-submit commands use the live cluster's network namespace to
avoid giving a one-off Compose container the same `spark-master` DNS
alias as the master itself. For the Snowflake staging load, pass the
existing local environment file and the Spark 3.5-compatible connector:

```powershell
docker run --rm `
  --network container:smart-energy-platform-spark-master-1 `
  --volumes-from smart-energy-platform-spark-master-1 `
  --user 185 `
  --env-file .env `
  --entrypoint /opt/spark/bin/spark-submit `
  smart-energy-spark-iceberg:3.5.9-1.9.2 `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark-master `
  --conf spark.jars.ivy=/tmp/spark-ivy `
  --repositories https://repo.maven.apache.org/maven2 `
  --packages net.snowflake:spark-snowflake_2.12:3.2.3-spark_3.5 `
  /opt/spark/jobs/snowflake_staging_load.py
```

The loader MERGEs Silver meter readings into
`STG_SILVER_METER_READINGS`, the three Gold aggregates into
`STG_GOLD_DAILY_GRID_KPIS`, `STG_GOLD_TRANSFORMER_LOADING_15M`, and
`STG_GOLD_DAILY_FEEDER_HEALTH`, and per-file quality metrics into
`STG_PIPELINE_QUALITY_METRICS` in
`SNOWFLAKE_DATABASE.SNOWFLAKE_RAW_SCHEMA`. The `STG_` names keep these
landing contracts separate from the existing dbt `METER_READINGS`
source. Detailed Silver rows are selected by business keys from newly
completed Bronze inputs; transformer intervals are limited to affected
timestamps, while daily aggregates refresh only affected dates. Durable
HDFS load checkpoints make a repeated run a no-op. Detector alerts are
not loaded because this project does not
currently persist `grid-alerts` to HDFS or expose another batch-readable
alert source. Ground-truth anomaly labels remain fields in the Silver
readings and are not represented as detector alerts.
