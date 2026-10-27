# Smart Energy dbt Warehouse

This project builds the user's Galaxy Schema in Snowflake from the existing source:

`SNOWFLAKE_CLASSROOM.PUBLIC.STG_SILVER_METER_READINGS`

The source has 22 columns. `ANOMALY_LABEL` and `ANOMALY_FAMILY` are treated as **ground-truth dataset labels**, not as alerts emitted by Flink. There is no `grid-alerts` source configured in this workspace because the provided Snowflake staging table does not contain those detector events.

## Folder architecture

```text
smart_energy_dbt_workspace/
├── dbt_project.yml
├── requirements.txt
├── profiles.yml.example
├── macros/
│   └── generate_schema_name.sql
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── staging.yml
│   │   └── stg_silver_meter_readings.sql
│   ├── intermediate/
│   │   └── int_meter_readings_enriched.sql
│   └── marts/
│       ├── dimensions/
│       │   ├── dim_date.sql
│       │   ├── dim_time.sql
│       │   ├── dim_meter.sql
│       │   └── dim_grid_asset.sql
│       ├── facts/
│       │   ├── fact_meter_readings.sql
│       │   ├── fact_transformer_daily.sql
│       │   └── fact_anomaly_events.sql
│       └── marts.yml
└── tests/
    ├── unique_meter_reading_grain.sql
    ├── time_dimension_has_96_slots.sql
    └── readings_are_on_quarter_hour.sql
```

## Output schemas

The included `generate_schema_name` macro maps configured model schemas exactly to:

- `SNOWFLAKE_CLASSROOM.STAGING` — staging view
- `SNOWFLAKE_CLASSROOM.INTERMEDIATE` — intermediate view
- `SNOWFLAKE_CLASSROOM.DW` — dimensions and facts

It does not use `PUBLIC` for model outputs. The source remains in `SNOWFLAKE_CLASSROOM.PUBLIC`.

Your Snowflake role needs permission to create/use these schemas and objects. If you prefer different schema names, adjust `dbt_project.yml` and the macro deliberately.

## Set up a new workspace

1. Install a compatible dbt Core and Snowflake adapter in a virtual environment:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

2. Merge the `smart_energy_dw` profile block from `profiles.yml.example` into `~/.dbt/profiles.yml` (do not overwrite existing profiles). Configure it with the account, user, authentication method, role, and warehouse from your existing Snowflake connection. Do not put credentials in this repository. The Spark connector's `.env` is not automatically a dbt profile.

3. Run validation:

   ```powershell
   dbt debug --profiles-dir "$HOME\.dbt"
   dbt parse
   dbt ls
   ```

4. Check the source and grain test assumptions first. Then run the initial historical build:

   ```powershell
   dbt build --profiles-dir "$HOME\.dbt"
   ```

5. For subsequent daily execution, run the upstream Spark-to-Snowflake load first, then `dbt build`. The incremental facts use merge semantics and a configurable event-time lookback.

## Models

### Staging

`stg_silver_meter_readings` is a view over `PUBLIC.STG_SILVER_METER_READINGS`. It standardizes column names, trims identifiers, normalizes category strings, and casts fields.

### Intermediate

`int_meter_readings_enriched` derives:

- `date_key` as `YYYYMMDD`
- `time_key` as 0–95 for 15-minute slots
- stable numeric surrogate-key candidates using a truncated SHA-256 hash
- `reading_fact_key` using meter/source identity and event timestamp

A 60-bit truncated hash is practical for a project-scale surrogate key, but not a mathematical uniqueness guarantee. The uniqueness tests should pass before trusting these keys.

### Dimensions

- `dim_date`: calendar dates between the minimum and maximum source reading dates.
- `dim_time`: all 96 quarter-hour slots.
- `dim_meter`: distinct meter/source identities.
- `dim_grid_asset`: distinct transformer/feeder/substation combinations and their rating.

These four dimensions are rebuilt as tables on each dbt build. They have stable deterministic keys, but this simple model does **not** implement SCD Type 2 history. If asset/meter attributes change over time, expand the dimension design before depending on historical attribute tracking.

### Facts

- `fact_meter_readings`: incremental merge by `reading_fact_key`.
- `fact_transformer_daily`: daily transformer summaries, recomputed for affected dates using all detailed readings for those dates.
- `fact_anomaly_events`: rows where `ANOMALY_LABEL = TRUE`; these are ground-truth anomalies, not Flink detector alerts.

## Incremental-loading limitation

The current 22-column landing table has no ingestion timestamp or batch ID. Therefore the fact models use `READING_TS_UTC` as the available incremental watermark and reprocess a rolling lookback (`incremental_lookback_days`, default 3).

This works when event timestamps move forward as new batches arrive and late data is confined to the lookback period. It can miss arbitrarily late data older than that window. For stronger production-grade incremental loading, add `INGESTION_TS_UTC` and preferably `BATCH_ID`/`EVENT_ID` during the Spark-to-Snowflake load, then filter incrementals using ingestion metadata.

## Operational reminders

- The first run builds historical data; later runs merge newly arriving and lookback rows.
- `unique_key` does not itself filter incremental input; the SQL filters are present in the models.
- Snowflake `MERGE` requires the input unique key to be unique within each incremental batch. Run the duplicate-grain test and investigate any returned rows.
- `fact_transformer_daily` calculates transformer loading by summing apparent power across meters per timestamp, then comparing that load with transformer capacity. This is more meaningful than taking the maximum meter-level loading ratio.
- The source timestamps are `TIMESTAMP_NTZ`. The project treats their values as UTC by convention; Snowflake does not store timezone metadata in this type.
- This project does not yet model `grid-alerts` detector output or pipeline-run quality metrics. Add those as separate sources/models when their Snowflake landing tables exist.
