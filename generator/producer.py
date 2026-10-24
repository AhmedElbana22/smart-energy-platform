"""Development CSV-to-Kafka producer.

Input CSV is expected to be sorted by timestamp. It preserves source fields and does
not invent electrical features. Confirm what `id` represents before mapping it to
`meter_id`. Replace or extend this starter when the feature-generation module is ready.
"""
import csv
import json
import os
import time
import uuid
from itertools import groupby
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "meter-readings")
INPUT_CSV = Path(os.getenv("INPUT_CSV", "/data/raw/real_readings.csv"))
PUBLISH_INTERVAL_SECONDS = max(0, float(os.getenv("PUBLISH_INTERVAL_SECONDS", "2")))


def event_timestamp(row):
    return row.get("event_timestamp") or row.get("timestamp") or ""


def prepare_record(row):
    record = {key: value for key, value in row.items() if key is not None}
    timestamp = event_timestamp(record)
    source_id = record.get("id", "")
    value = record.get("value_kwh", "")
    record["event_timestamp"] = timestamp
    record["event_id"] = str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"smart-energy|{source_id}|{timestamp}|{value}")
    )
    return record


def main():
    if not INPUT_CSV.is_file():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_CSV}. Place your permitted development CSV at data/raw/real_readings.csv."
        )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda item: json.dumps(item, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        batch_size=32768,
        linger_ms=20,
        compression_type="gzip",
    )

    try:
        with INPUT_CSV.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            required = {"id", "timestamp", "value_kwh"}
            if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                raise ValueError(
                    f"Expected CSV columns {sorted(required)}; found {reader.fieldnames}. "
                    "Update this contract if the source file uses different column names."
                )

            count = 0
            # Publish all readings with the same timestamp together; source rows must be sorted.
            for timestamp, rows in groupby(reader, key=lambda row: row.get("timestamp", "")):
                batch_count = 0
                futures = []
                for row in rows:
                    record = prepare_record(row)
                    futures.append(producer.send(TOPIC, value=record))
                    count += 1
                    batch_count += 1
                producer.flush()
                for future in futures:
                    future.get(timeout=30)
                print(f"Published {batch_count} records for timestamp={timestamp!r}; total={count}")
                if PUBLISH_INTERVAL_SECONDS > 0:
                    time.sleep(PUBLISH_INTERVAL_SECONDS)
            print(f"Finished. Total records published: {count}")
    except KafkaError as exc:
        raise RuntimeError(f"Failed while publishing to Kafka topic {TOPIC!r}") from exc
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
