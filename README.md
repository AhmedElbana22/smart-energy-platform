# Smart Energy Data Platform

A containerized data engineering platform for real-time smart-meter
monitoring, grid anomaly detection, distributed data processing, and
cloud analytics.

The platform integrates Apache Kafka, Apache Flink, Hadoop HDFS, Apache
Spark, Apache Iceberg, Snowflake, and dbt to support streaming and batch
workflows. Docker Compose provides the local development environment,
while Apache Airflow and Prometheus/Grafana provide orchestration and
monitoring foundations.

## Architecture Overview

The platform separates real-time processing from batch analytics. Kafka
is the event-streaming layer, and Snowflake is the cloud analytics
destination.

``` text
                  Smart Meter Readings
                          |
                          v
                    Apache Kafka
                   meter-readings
                          |
             +------------+------------+
             |                         |
             v                         v
        Apache Flink              Kafka Connect
      Stream Processing           HDFS Sink
             |                         |
             v                         v
        Apache Kafka                  HDFS
         grid-alerts              Raw Data Storage
                                       |
                                       v
                                  Apache Spark
                                Batch Processing
                                       |
                                       v
                                  Apache Iceberg
                                 Managed Tables
                                       |
                                       v
                                    Snowflake
                                Cloud Data Warehouse
                                       |
                                       v
                                      dbt
                              Analytical Data Models

       Apache Airflow: Workflow Orchestration
       Prometheus + Grafana: Monitoring
```

The diagram describes the intended architecture. Components may be
configured and tested independently; it does not imply that every
integration is automatically scheduled or fully operational end to end.

## Technology Stack

  -----------------------------------------------------------------------
  Technology                          Purpose
  ----------------------------------- -----------------------------------
  Apache Kafka                        Event ingestion, messaging, and
                                      stream distribution

  Apache Flink                        Real-time stream processing and
                                      grid-alert generation

  Kafka Connect                       Connector-based integration between
                                      Kafka and external storage

  Hadoop HDFS                         Distributed data storage

  Apache Spark                        Batch processing and data
                                      transformations

  Apache Iceberg                      Table management for analytical
                                      datasets

  Snowflake                           Cloud data warehouse and analytical
                                      storage

  dbt                                 SQL transformations and analytical
                                      data modeling

  Apache Airflow                      Workflow orchestration

  PostgreSQL                          Airflow metadata storage

  Prometheus                          Metrics collection

  Grafana                             Monitoring and visualization

  Docker Compose                      Local service deployment

  Python                              Data generation and validation
                                      utilities
  -----------------------------------------------------------------------

## Data Pipeline

### 1. Event Ingestion --- Apache Kafka

Apache Kafka provides the messaging foundation. Incoming smart-meter
readings are published to Kafka topics so streaming applications and
storage connectors can consume the event stream independently.

The main topics are:

-   `meter-readings`: incoming meter measurements.
-   `grid-alerts`: alerts produced by the Flink streaming pipeline.

Start Kafka:

``` bash
docker compose up -d kafka
```

Create the topics if they do not already exist:

``` bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic meter-readings \
  --partitions 3 \
  --replication-factor 1

docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic grid-alerts \
  --partitions 3 \
  --replication-factor 1
```

List topics:

``` bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

The single-broker configuration uses replication factor `1` for local
development, not production-grade fault tolerance.

### 2. Real-Time Grid Monitoring --- Apache Flink

Apache Flink consumes JSON records from `meter-readings`, evaluates the
measurements against configured rules, and publishes qualifying events
to `grid-alerts`.

The streaming implementation is located in:

``` text
flink/
├── Dockerfile
└── sql/
    └── alerts.sql
```

The Flink Docker image includes the Kafka SQL connector. The SQL
definition specifies the Kafka source, Kafka sink, and alert-generation
rules.

The SQL currently implements these detectors:

- **Transformer loading:** 15-minute tumbling windows; use summed
  apparent power divided by a consistent transformer rating when
  available, otherwise use the maximum source `loading_ratio`. Emit
  above 100% loading, with `HIGH` severity at 110% or above.
- **Persistent voltage drop:** at least three readings for a meter that
  are each at least 15 V below baseline in a 45-minute window, hopping
  every 15 minutes. A maximum drop of 18 V or more is `HIGH` severity.
- **Suspicious consumption:** current active power above the mean of
  prior readings plus three population standard deviations; require at
  least 12 valid prior readings.
- **Data quality:** emit for missing required fields, invalid physical
  ranges, or an unrecognized quality status.

The development source uses a zero-delay event-time watermark. Windowed
alerts are emitted only after later event timestamps advance the
watermark past the window end. The replay producer sends timestamp
groups in order; an idle or unfinished replay can therefore leave recent
windows open.

Start the streaming services (this starts Kafka, Flink, and the dashboard,
but does not submit the Flink SQL job):

``` powershell
docker compose --profile streaming `
  -f docker-compose.yml `
  -f docker-compose.streaming.override.yml `
  up --build -d kafka flink-jobmanager flink-taskmanager grid-alert-dashboard
```

Flink web interface: <http://localhost:8085>

Starting the containers launches the Flink runtime. Submit the SQL job
once, after checking the Flink Jobs page/API for an existing active job:

``` powershell
Invoke-RestMethod http://localhost:8085/jobs/overview
docker compose --profile streaming `
  -f docker-compose.yml `
  -f docker-compose.streaming.override.yml `
  run --rm flink-sql-runner
Invoke-RestMethod http://localhost:8085/jobs/overview
```

The source starts at `latest-offset`. Start Flink before producing a new
batch; this intentionally skips readings already present when the SQL job
starts and avoids replaying retained history into the alert sink. Each new
reading batch advances event time; windowed alerts can require later
timestamps before they are emitted.

After confirming the Flink Jobs page/API shows the SQL job as `RUNNING`,
produce the next dataset batch from a separate PowerShell window:

``` powershell
docker compose --profile producer `
  -f docker-compose.yml `
  -f docker-compose.streaming.override.yml `
  run --rm -e MAX_BATCHES=1 -e PUBLISH_INTERVAL_SECONDS=0 generator
```

The producer reads `generator/LastTime.txt`, publishes the next 15-minute
group, and advances the checkpoint only after successful publication. The
dashboard refreshes every two seconds; use its alert-type filter and the
optional browser-notification button to inspect newly emitted alerts.

The Flink-to-Kafka path has been verified by consuming alerts from
`grid-alerts` with Kafka's console consumer. This repository does not
include a Grafana dashboard for alert records; Grafana is currently
provisioned with Prometheus as its only data source. A separate
`grid-alert-dashboard` application consumes `grid-alerts` with its own
Kafka consumer group and provides a browser feed, counts by alert type
and severity, and optional browser notifications.

Start the dashboard without starting or resubmitting the Flink job:

``` powershell
docker compose --profile streaming `
  -f docker-compose.yml `
  -f docker-compose.streaming.override.yml `
  up --build -d grid-alert-dashboard
```

Open <http://localhost:8501>. The dashboard reads retained alert history
using its own group, `smart-energy-grid-alert-dashboard-v2`; it does not
change the Flink group's offsets or publish to Kafka. Its consumer does not
commit offsets so a service restart reconstructs the feed from retained
alerts. Counts are for alerts consumed since the dashboard process started.
Browser notifications are opt-in and are suppressed for records that were
already in the topic when the dashboard consumer started.

To check the dashboard's consumer and feed:

``` powershell
Invoke-RestMethod http://localhost:8501/healthz
Invoke-RestMethod "http://localhost:8501/api/alerts?limit=10"
```

To inspect emitted alerts manually instead:

``` bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic grid-alerts \
  --from-beginning
```

`EMIT_DQ_TEST_RECORD` is not a supported producer option. To exercise
the data-quality branch manually, publish a deliberately invalid record
to `meter-readings` with Kafka's console producer; it will create a
`POOR_DATA_QUALITY` alert.

### 3. Kafka-to-HDFS Integration --- Kafka Connect

Kafka Connect provides a framework for transferring Kafka records to
external systems through connectors.

The repository includes:

``` text
connect/
├── Dockerfile
├── hdfs3-config-reference.json
└── hdfs3-sink.json
```

Start the Connect profile:

``` bash
docker compose --profile connect up --build -d
```

Kafka Connect REST API: <http://localhost:8083>

The sink configuration describes the intended Kafka-to-HDFS integration.
Successful operation depends on the appropriate connector plugin being
installed, configured, and compatible with the deployed Hadoop
environment. Running the Connect worker alone does not establish that
records are being written to HDFS.

### 4. Distributed Storage --- Hadoop HDFS

Hadoop Distributed File System provides distributed storage for
downstream batch processing.

Start the batch services:

``` bash
docker compose --profile batch up --build -d
```

HDFS NameNode interface: <http://localhost:9870>

The Iceberg warehouse is configured at:

``` text
hdfs://namenode:8020/warehouse/iceberg
```

HDFS provides the underlying storage, while Apache Iceberg manages table
metadata and table-level operations for supported analytical datasets.

### 5. Batch Processing --- Apache Spark and Iceberg

Apache Spark provides the processing environment for batch workloads.
Apache Iceberg enables table management on top of the configured storage
layer.

Spark validation and integration scripts are maintained under:

``` text
spark/jobs/
```

These scripts cover selected Spark/HDFS operations, Iceberg operations,
and Snowflake connectivity and write validation.

The batch environment can be started with:

``` bash
docker compose --profile batch up --build -d
```

Spark master interface: <http://localhost:8082>

The batch flow is Kafka Connect JSON in HDFS, JSON-to-Parquet promotion,
Bronze-to-Silver, Silver-to-Gold, and an incremental Snowflake staging
load. The HDFS control logs track completed source files; the Snowflake
load uses stable-key MERGEs so reruns do not append duplicate history.
See [connect/README.md](./connect/README.md) for the repeatable commands.

### 6. Cloud Data Warehouse --- Snowflake

Snowflake serves as the cloud data warehouse for analytical storage and
downstream reporting. Unlike the local processing services, Snowflake is
an external cloud service; connection settings must be supplied through
local environment configuration.

The batch loader uses the existing local `.env` settings and creates or
updates only raw landing tables for detailed Silver readings, the three
Gold aggregates, and per-file pipeline quality metrics. It does not run
dbt or create dimensional warehouse tables. See
[connect/README.md](./connect/README.md) for the Spark 3.5 connector
command and staging-table behavior.

### 7. Analytical Transformations --- dbt

dbt provides the SQL transformation and modeling layer for data
available in Snowflake.

The dbt project organizes warehouse data into reusable analytical
models, including fact tables, dimensions, and data-quality tests as the
source contract is finalized.

The active dbt project is `smart_energy_dbt_workspace`. Its Compose service
builds dbt Core and the Snowflake adapter from that workspace's
`requirements.txt`. Compose maps the existing Snowflake values from the
repository `.env` into dbt's expected `DBT_SNOWFLAKE_*` variables; the
mounted example profile contains no credentials.

Validate the connection and project before building:

``` powershell
docker compose --profile analytics run --build --rm dbt debug
docker compose --profile analytics run --rm dbt parse
docker compose --profile analytics run --rm dbt ls --resource-type model
docker compose --profile analytics run --rm dbt show `
  --inline "select count(*) as ROW_COUNT from SNOWFLAKE_CLASSROOM.PUBLIC.STG_SILVER_METER_READINGS" `
  --limit 1
```

Run the full models and tests after the read-only checks pass:

``` powershell
docker compose --profile analytics run --rm dbt build
```

The initial build creates views and tables under `SNOWFLAKE_CLASSROOM.STAGING`,
`INTERMEDIATE`, and `DW`; incremental fact models use merge semantics.
The source `PUBLIC.STG_SILVER_METER_READINGS` is read-only to dbt.
To preview the resulting fact data:

``` powershell
docker compose --profile analytics run --rm dbt show `
  --inline "select * from SNOWFLAKE_CLASSROOM.DW.FACT_METER_READINGS order by READING_TS_UTC desc" `
  --limit 10
```

## Workflow Orchestration

### Apache Airflow

Apache Airflow provides the local workflow orchestration environment,
with PostgreSQL storing Airflow metadata and `LocalExecutor` handling
task execution.

Start the orchestration services:

``` bash
docker compose --profile orchestration up -d
```

Airflow web interface: <http://localhost:8080>

The included smoke-test DAG validates basic task execution. A complete
scheduled workflow linking batch processing, Snowflake loading, and dbt
transformations is a separate orchestration step.

## Monitoring

Prometheus and Grafana provide the monitoring foundation for the local
platform.

Start the monitoring services:

``` bash
docker compose --profile monitoring up -d
```

-   Prometheus: <http://localhost:9090>
-   Grafana: <http://localhost:3000>

Prometheus is configured to scrape its own metrics, and Grafana is
provisioned with Prometheus as a data source. Additional exporters and
service-specific dashboards are needed to expose detailed metrics from
Kafka, Flink, Kafka Connect, Airflow, and other components.

## Getting Started

### Prerequisites

-   Git
-   Docker Desktop with the WSL 2 engine on Windows, or Docker Engine on
    Linux
-   Docker Compose v2
-   An editor such as Visual Studio Code

### 1. Clone the repository

``` bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd smart-energy-platform
```

Replace the placeholder with the repository's actual GitHub URL.

### 2. Configure environment variables

On Windows PowerShell:

``` powershell
Copy-Item .env.example .env
```

On Linux, macOS, or Git Bash:

``` bash
cp .env.example .env
```

Configure the environment variables required by the services you intend
to run. Each developer should maintain their own local `.env` file.
Never commit credentials, access tokens, private keys, or other secrets.

### 3. Validate the Docker environment

``` bash
docker --version
docker compose version
docker compose config --quiet
```

### 4. Start the required services

Start Kafka first, then start the service profiles needed for the
workflow you want to run.

  Compose profile   Purpose
  ----------------- -------------------------------------------------
  `streaming`       Flink streaming runtime
  `connect`         Kafka Connect and HDFS sink integration
  `batch`           HDFS, Spark, and Iceberg
  `analytics`       dbt commands against Snowflake
  `orchestration`   Airflow and PostgreSQL
  `monitoring`      Prometheus and Grafana
  `producer`        Development CSV-to-Kafka producer
  `ml`              Optional CPU-based machine-learning environment

Some workflows require services from multiple profiles. Check the
Compose dependencies and required configuration when starting a profile
independently.

## Development Data Producer

The optional Python producer publishes records from a development CSV
file to Kafka.

The current starter expects a file at `data/raw/real_readings.csv` with
these columns:

``` text
id,timestamp,value_kwh
```

Run the producer:

``` bash
docker compose --profile producer run --build --rm generator
```

The producer adds an event identifier and event timestamp. The meaning
of the source `id`, the sampling interval, and the electrical
feature-generation contract should be confirmed before using the starter
as a full smart-meter simulator. Do not commit private or sensitive
source data.

## Repository Structure

``` text
smart-energy-platform/
├── connect/       # Kafka Connect image and HDFS sink configuration
├── smart_energy_dbt_workspace/ # dbt models, tests, and Snowflake profile template
├── flink/         # Flink Docker image and streaming SQL
├── spark/         # Spark configuration and validation/job scripts
├── generator/     # Development Kafka producer
├── data/          # Local data; private datasets should remain untracked
├── docker-compose.yml
├── .env.example
└── README.md
```

Other configuration files and directories support the services defined
in Docker Compose.

## Data and Environment Management

Local development data, generated artifacts, logs, credentials, and
container state should remain outside version control unless a specific
non-sensitive source file is intentionally part of the project.

The dbt profile template references environment variables rather than
embedding credentials. Keep local container state and secret-bearing
configuration files out of version control.

Docker volumes preserve service state across container restarts. To stop
the Compose environment while retaining named volumes:

``` bash
docker compose --profile "*" down
```

Using `docker compose down -v` removes named volumes and can delete
Kafka data, HDFS contents, Airflow metadata, and monitoring state. Use
it only when intentionally resetting the local environment.

## Development Workflow

Keep changes focused on individual components and review configuration
files before committing them. For collaborative development, use feature
branches and pull requests.

After modifying a service, validate the relevant component and its
interfaces with adjacent pipeline stages. A successful container startup
confirms that a service can start; it does not automatically prove that
data is flowing correctly through every downstream integration.

The platform is designed as a local development and integration
environment. A production deployment would require additional security
controls, resilient infrastructure, operational monitoring, and
deployment-specific configuration.
