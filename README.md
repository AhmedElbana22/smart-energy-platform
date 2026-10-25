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

**Low-voltage detection**

A low-voltage alert is generated when measured voltage falls below 95%
of its baseline:

``` text
voltage_v < voltage_v_baseline * 0.95
```

**High-transformer-loading detection**

A high-loading alert is generated when the transformer loading ratio
reaches or exceeds `0.80`:

``` text
loading_ratio >= 0.80
```

Alerts include the meter identifier, reading timestamp, transformer
identifier, alert type, relevant measurements, and a descriptive
message.

Start the streaming services:

``` bash
docker compose --profile streaming up --build -d
```

Flink web interface: <http://localhost:8085>

Starting the containers launches the Flink runtime. The SQL job must
also be submitted to the runtime for event processing to begin.

The Flink-to-Kafka alert flow and downstream `grid-alerts` consumer have
been tested in the local Docker environment.

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

This layer provides the foundation for transforming stored data into
analytical datasets. The complete Bronze-to-Silver-to-Gold
transformation workflow and reliable Iceberg-to-Snowflake loading are
separate implementation responsibilities.

### 6. Cloud Data Warehouse --- Snowflake

Snowflake serves as the cloud data warehouse for analytical storage and
downstream reporting. Unlike the local processing services, Snowflake is
an external cloud service; connection settings must be supplied through
local environment configuration.

The repository includes validation scripts for testing
Spark-to-Snowflake connectivity and write operations. These tests cover
the relevant connection and write paths but do not, by themselves,
establish that the complete HDFS-to-Iceberg-to-Snowflake workflow runs
automatically.

Configure the required Snowflake settings locally before executing the
relevant tests. Ensure that the configured user has the required access
to the account, warehouse, database, schema, and target objects.

### 7. Analytical Transformations --- dbt

dbt provides the SQL transformation and modeling layer for data
available in Snowflake.

The dbt project organizes warehouse data into reusable analytical
models, including fact tables, dimensions, and data-quality tests as the
source contract is finalized.

Validate the configured dbt connection:

``` bash
docker compose --profile analytics run --build --rm dbt debug
```

The dbt source definitions and models must match the tables and schemas
actually available in Snowflake. Credentials should be provided through
environment variables rather than hard-coded into configuration files.

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
├── dbt/           # dbt project and Snowflake profile configuration
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

The tracked `dbt/profiles.yml` should reference environment variables
for sensitive settings.

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
