# Smart Energy Data Platform — local team starter

This repository is a **shared development scaffold** for the Smart Energy project. It pins the main images/configuration and gives the team one place to collaborate. It is not a production deployment, and not every end-to-end connector/job is implemented yet.

## What is included

| Tool | Why it is in the repo | Compose profile |
|---|---|---|
| Apache Kafka | `meter-readings` and `grid-alerts` topics | Default (Kafka only) |
| Kafka Connect | Worker for the HDFS sink integration | `connect` |
| Apache Flink | Long-running streaming-alert runtime | `streaming` |
| HDFS NameNode + DataNode | Bronze/raw storage | `batch` or `connect` |
| Apache Spark + Iceberg runtime | PySpark batch work and Iceberg HadoopCatalog on HDFS | `batch` |
| Apache Airflow + PostgreSQL | Local DAG scheduling/orchestration | `orchestration` |
| dbt-snowflake | One-off dbt commands against external Snowflake | `analytics` |
| scikit-learn / XGBoost runner | Optional CPU baseline training/scoring environment | `ml` |
| Prometheus + Grafana | Monitoring foundation | `monitoring` |
| Python producer | Development CSV-to-Kafka starter | `producer` |
| GitHub Actions | Basic repository checks on pull requests/pushes | GitHub-hosted CI |

**Not containers:** Snowflake is an external cloud service, and Power BI is a desktop/reporting client. Their credentials/settings are local and must not be committed.

## Hardware recommendation

### Disk space

- **Absolute starting point:** around **60 GB free** on the drive used by Docker Desktop.
- **Recommended:** **80–100 GB free** before pulling the images and beginning regular development.
- **More comfortable for larger datasets, repeated builds, Parquet files, and model artifacts:** **120 GB or more**.

These are practical planning estimates, not hard product requirements. Docker images, build cache, named volumes, HDFS files, generated data, and ML artifacts all consume space. The local images and generated data can grow noticeably over time.

### RAM (your device has 16 GB)

- Start with about **8 GB available to Docker/WSL2**, leaving the rest for Windows, VS Code, browser tabs, and other apps.
- Do **not** run every profile concurrently on this laptop. The `batch` profile starts Spark and HDFS; the `streaming` profile starts Flink; Airflow and monitoring should be started when needed.
- Start Kafka by itself first. For an integrated test, start only the small set of services required for that test.
- 16 GB is enough to begin local development and small integration tests, but it is **not** a guarantee that the full stack plus 100,000-meter load tests will run comfortably at once. Scale gradually or use a shared Linux machine/cloud for heavier integration/performance runs.

In Docker Desktop on Windows, enable the WSL 2 engine. Check **Settings → Resources** and keep the memory/disk limits appropriate for your machine. If your setup uses WSL2's `.wslconfig`, make sure its memory limit agrees with your chosen budget.

## First-time setup (each teammate)

Prerequisites:

- Git
- Docker Desktop with the WSL2 engine enabled (Windows) or Docker Engine on Linux
- Docker Compose v2 (`docker compose`, with a space)
- VS Code or another editor

### 1. Clone the same repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd smart-energy-platform
```

### 2. Create a local environment file

PowerShell:

```powershell
Copy-Item .env.example .env
```

Git Bash / Linux / macOS:

```bash
cp .env.example .env
```

Each teammate owns their own `.env`. Change the default local passwords. Add approved Snowflake development credentials only on your own machine. **Never commit `.env`, passwords, access tokens, private keys, or private/real source data.**

### 3. Validate before starting containers

```bash
docker --version
docker compose version
docker compose config --quiet
```

### 4. Start only Kafka first

```bash
docker compose up -d kafka
docker compose ps
docker compose logs -f kafka
```

Create the project topics:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic meter-readings --partitions 3 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic grid-alerts --partitions 3 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

For local single-broker development, replication factor 1 is intentional. It is not a highly available production Kafka setup.

## Profiles: run only the tools you need

Start one profile at a time to keep RAM use manageable.

### Branch A: Flink streaming runtime

```bash
docker compose --profile streaming up -d
docker compose ps
```

Flink UI: <http://localhost:8085>

This starts the Flink runtime only. **The Flink job, Kafka source/sink connector JARs, and alert rules still need to be implemented and submitted.** Starting Flink alone does not process readings.

### Branch B: HDFS + Spark + Iceberg

```bash
docker compose --profile batch up --build -d
docker compose ps
```

- HDFS NameNode UI: <http://localhost:9870>
- Spark master UI: <http://localhost:8082>

The Spark image adds the Iceberg runtime JAR and configures an Iceberg HadoopCatalog at `hdfs://namenode:8020/warehouse/iceberg`. The team's actual Bronze → Silver → Gold transformation jobs still need to be written under `spark/jobs/`.

### Kafka Connect + HDFS

```bash
docker compose --profile connect up -d
docker compose ps
```

Kafka Connect REST API: <http://localhost:8083>

**Important integration item:** this profile runs the Kafka Connect worker, but the HDFS sink plugin is not bundled yet. Confluent's HDFS 2 sink is deprecated and targets HDFS 2.x; its HDFS 3 sink has separate licensing requirements. Before enabling this branch end to end, the team must choose and test a connector that matches our Hadoop version and project constraints. Do not assume the HDFS sink is already active just because Connect is running.

### Airflow orchestration

```bash
docker compose --profile orchestration up -d
docker compose ps
```

Airflow UI: <http://localhost:8080>

Login defaults are `admin` / `change_me_local_only` **only for local development**; set a different password in `.env`. The `compose_smoke_test` DAG just verifies that Airflow can run a task. It is not the final Spark → Snowflake → dbt workflow. Airflow uses `LocalExecutor` with a PostgreSQL metadata database in this starter setup.

### Monitoring

```bash
docker compose --profile monitoring up -d
```

- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000>

Prometheus currently scrapes itself and Grafana is provisioned with Prometheus as its data source. Kafka/Flink/Connect/Airflow exporters and service-specific dashboards still need to be configured, so their metrics will not appear automatically yet.

### Run the CSV producer (development starter)

Put a **permitted, non-sensitive development sample** at `data/raw/real_readings.csv` with the exact columns `id,timestamp,value_kwh`. Then run:

```bash
docker compose --profile producer run --build --rm generator
```

By default the starter producer waits 2 seconds between timestamp groups so that we can test quickly. To configure a 15-minute wait, set `PUBLISH_INTERVAL_SECONDS=900` in `.env`. The file must be sorted by timestamp for grouping to work correctly.

The script preserves the source ID as-is and adds a deterministic `event_id` plus `event_timestamp`. It **does not** decide whether `id` means a meter ID or reading ID, and it **does not fabricate the missing electrical features**. Confirm the meaning of `id`, the sampling interval, and the feature-generation contract before turning this starter into the 100,000-meter simulator. Do not commit the source CSV to Git.

### Optional CPU ML environment

```bash
docker compose --profile ml run --build --rm ml-runner
```

This just verifies that the small CPU ML image builds and runs. It is not the trained anomaly model. Add the actual feature engineering/training/scoring entrypoint when labels and the fine-tuned feature generator's dependencies are understood. Do not run ML training together with Spark/HDFS on the 16 GB laptop unless the workload is small.

### dbt against Snowflake

Fill the Snowflake fields in your local `.env`, then test the container:

```bash
docker compose --profile analytics run --build --rm dbt debug
```

The example dbt project is only a scaffold. Its source table/schema must match the actual Snowflake load. Build the fact/dimension models after the source contract and the meaning of `id` / `value_kwh` have been confirmed.

## Helpful commands

```bash
# Current services
docker compose ps

# Follow logs for one service
docker compose logs -f kafka

# Stop a specific service group without deleting persisted data
docker compose stop spark-master spark-worker namenode datanode

docker compose stop flink-jobmanager flink-taskmanager

docker compose stop airflow-webserver airflow-scheduler airflow-db

# Stop and remove containers/networks from all profiles but retain named volumes
docker compose --profile "*" down
```

**Data warning:** `docker compose down -v` (including `docker compose --profile "*" down -v`) deletes the named volumes, including Kafka data, HDFS contents, Airflow metadata, and Grafana/Prometheus state. Do not use `-v` unless you intentionally want to reset them.

## Git workflow for teammates

Recommended lightweight workflow:

1. Protect `main`; nobody pushes unfinished work directly to it.
2. Each teammate creates a branch for one responsibility, e.g. `feature/kafka-producer`, `feature/flink-alerts`, `feature/spark-silver-gold`, `feature/airflow-dag`, or `feature/dbt-models`.
3. Commit source code/configuration only. Do not commit `.env`, datasets, `logs/`, dbt artifacts, or container volumes.
4. Push the branch and open a pull request. GitHub Actions runs basic Python and Compose configuration checks.
5. Review and merge after the service starts and its acceptance test passes.

Example:

```bash
git checkout -b feature/kafka-producer
git add generator/
git commit -m "feat: add development Kafka producer"
git push -u origin feature/kafka-producer
```

### Important team limitation

Git makes the code and configuration shared, but Docker Compose runs a **separate local environment on each teammate's device**. It does not share live Kafka topics, HDFS data, or named volumes between laptops. If the team needs one common live environment, host the shared services on a Linux server or cloud VM and agree on access/security; do not expose unauthenticated development services to the public internet.

## Implementation status / next items

- [x] Starter repo and profiles for the main local tools
- [x] Persistent named volumes and `.gitignore` for local state/data
- [x] Airflow local smoke-test DAG
- [x] dbt/Snowflake profile template
- [x] Optional CPU ML runner image
- [ ] Confirm source `id` and `value_kwh` semantics and final event schema
- [ ] Finalize the fine-tuned feature-generation interface and anomaly ground-truth labels
- [ ] Select and validate an HDFS sink connector/plugin
- [ ] Implement/submit Flink Kafka job and persist/consume `grid-alerts`
- [ ] Implement Spark Bronze → Silver → Gold and Iceberg table creation
- [ ] Implement reliable Iceberg → Snowflake loading
- [ ] Complete dbt facts/dimensions, relationships, and tests
- [ ] Add real Prometheus exporters and dashboards
- [ ] Add pipeline integration tests and staged load testing
