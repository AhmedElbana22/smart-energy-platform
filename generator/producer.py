"""Minimal publisher for timestamp batches from the AMI Parquet dataset.

This script publishes records to Kafka and checkpoints the last acknowledged
timestamp batch. A successful rerun starts at the next 15-minute timestamp.
Kafka and this local checkpoint cannot be committed atomically, so a crash after
publishing but before saving the checkpoint can replay a batch.
Bronze/silver transformations are intentionally out of scope.
"""
import datetime as dt
import json
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
from kafka import KafkaProducer

TOPIC = os.getenv("KAFKA_TOPIC", "meter-readings")
BROKERS = [x.strip() for x in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092").split(",") if x.strip()]
INPUT = Path(os.getenv("INPUT_PATH", "/data/raw/ami_readings_final"))
MAX_BATCHES = max(1, int(os.getenv("MAX_BATCHES", "1")))
INTERVAL = max(0.0, float(os.getenv("PUBLISH_INTERVAL_SECONDS", "0")))
START_TIMESTAMP = os.getenv("START_TIMESTAMP", "").strip()
CHUNK_SIZE = max(100, int(os.getenv("SEND_CHUNK_SIZE", "1000")))
CHECKPOINT = Path(os.getenv("CHECKPOINT_FILE", str(Path(__file__).with_name("LastTime.txt"))))

# Map only the source column names needed by the existing Flink SQL contract.
FIELDS = (
    "meter_id", "source_system", "source_meter_id", "reading_ts_utc",
    "interval_minutes", "active_energy_kwh_import", "active_power_kw",
    "reactive_energy_kvarh_import", "reactive_power_kvar",
    "voltage_v_baseline", "voltage_v", "apparent_power_kva",
    "transformer_id", "feeder_id", "substation_id",
    "transformer_rating_kva", "loading_ratio", "value_status", "quality_status",
)
ALIASES = {
    "reading_ts_utc": ("timestamp_utc", "event_ts_utc", "timestamp", "reading_timestamp"),
    "voltage_v_baseline": ("baseline_voltage_v", "voltage_baseline_v", "baseline_voltage"),
    "voltage_v": ("voltage", "voltage_value_v"),
    "active_power_kw": ("active_power",),
    "active_energy_kwh_import": ("active_energy_kwh", "energy_kwh_import"),
    "reactive_power_kvar": ("reactive_power",),
    "reactive_energy_kvarh_import": ("reactive_energy_kvarh",),
    "apparent_power_kva": ("apparent_load_kva", "apparent_power"),
    "transformer_rating_kva": ("rating_kva",),
    "interval_minutes": ("reading_interval_minutes",),
}


def utc(value):
    if isinstance(value, str):
        value = dt.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        value = dt.datetime.combine(value, dt.time.min)
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def timestamp_text(value):
    value = utc(value)
    value = value.replace(microsecond=(value.microsecond // 1000) * 1000)
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def read_checkpoint():
    if not CHECKPOINT.exists():
        return None
    value = CHECKPOINT.read_text(encoding="utf-8").strip()
    return utc(value) if value else None


def save_checkpoint(value):
    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    temp = CHECKPOINT.with_suffix(CHECKPOINT.suffix + ".tmp")
    temp.write_text(timestamp_text(value) + "\n", encoding="utf-8")
    temp.replace(CHECKPOINT)


def resolve_columns(names):
    names = set(names)
    resolved = {}
    for field in FIELDS:
        if field in names:
            resolved[field] = field
        else:
            for alias in ALIASES.get(field, ()):
                if alias in names:
                    resolved[field] = alias
                    break
    if "meter_id" not in resolved or "reading_ts_utc" not in resolved:
        raise ValueError("Dataset must contain meter_id and a recognized timestamp column.")
    return resolved


def json_default(value):
    if isinstance(value, (dt.datetime, dt.date)):
        return timestamp_text(value)
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Cannot serialize {type(value).__name__} to JSON")


def publish_batch(producer, dataset, timestamp_column, columns, timestamp):
    field_type = dataset.schema.field(timestamp_column).type
    filter_value = utc(timestamp)
    if pa.types.is_timestamp(field_type) and field_type.tz is None:
        filter_value = filter_value.replace(tzinfo=None)
    elif not pa.types.is_timestamp(field_type):
        filter_value = timestamp

    scanner = dataset.scanner(
        columns=sorted(set(columns.values())),
        filter=ds.field(timestamp_column) == filter_value,
        batch_size=5000,
        use_threads=True,
    )
    pending, count = [], 0
    for batch in scanner.to_batches():
        for row in batch.to_pylist():
            record = {target: row.get(source) for target, source in columns.items()}
            if record.get("reading_ts_utc") is not None:
                record["reading_ts_utc"] = timestamp_text(record["reading_ts_utc"])
            pending.append(producer.send(
                TOPIC,
                key=str(record.get("meter_id") or "__unknown_meter__"),
                value=record,
            ))
            count += 1
            if len(pending) >= CHUNK_SIZE:
                for future in pending:
                    future.get(timeout=60)
                pending.clear()
    for future in pending:
        future.get(timeout=60)
    producer.flush(timeout=60)
    if count == 0:
        raise ValueError(f"No records found for timestamp {timestamp!r}")
    return count




def main():
    if not INPUT.exists():
        raise FileNotFoundError(f"Dataset not found: {INPUT}")

    dataset = ds.dataset(
        str(INPUT),
        format="parquet",
        partitioning="hive" if INPUT.is_dir() else None,
    )
    columns = resolve_columns(dataset.schema.names)
    timestamp_column = columns["reading_ts_utc"]

    checkpoint = read_checkpoint()
    if checkpoint is not None:
        start = checkpoint + dt.timedelta(minutes=15)
    else:
        start = utc(START_TIMESTAMP or "2021-01-01T00:00:00Z")

    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            separators=(",", ":"),
            allow_nan=False,
            default=json_default,
        ).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        compression_type="gzip",
    )

    try:
        print(
            f"Input: {INPUT}; topic: {TOPIC}; "
            f"starting at {timestamp_text(start)}; batches: {MAX_BATCHES}",
            flush=True,
        )

        for index in range(MAX_BATCHES):
            timestamp = start + dt.timedelta(minutes=15 * index)
            day_path = INPUT / f"reading_date={timestamp.date().isoformat()}"

            if not day_path.is_dir():
                print(f"No partition found for {timestamp.date()}; stopping.",
                      flush=True)
                break

            day_dataset = ds.dataset(str(day_path), format="parquet")

            print(
                f"Publishing batch {index + 1}/{MAX_BATCHES}: "
                f"{timestamp_text(timestamp)}",
                flush=True,
            )

            count = publish_batch(
                producer,
                day_dataset,
                timestamp_column,
                columns,
                timestamp,
            )

            save_checkpoint(timestamp)
            next_timestamp = timestamp + dt.timedelta(minutes=15)
            print(
                f"Published {count:,} records; last completed checkpoint: "
                f"{timestamp_text(timestamp)}; next invocation starts: "
                f"{timestamp_text(next_timestamp)}",
                flush=True,
            )

            if index + 1 < MAX_BATCHES and INTERVAL:
                time.sleep(INTERVAL)

        print("Finished publishing.", flush=True)

    finally:
        producer.close(timeout=30)


if __name__ == "__main__":
    main()
