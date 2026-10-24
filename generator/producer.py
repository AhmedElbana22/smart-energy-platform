"""Publish validated smart-meter CSV rows to Kafka using the final 23-column schema."""

import csv
import json
import os
import time
from itertools import groupby
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "meter-readings")
INPUT_CSV = Path(
    os.getenv("INPUT_CSV", "/data/raw/sample_meter_readings.csv")
)
PUBLISH_INTERVAL_SECONDS = max(
    0.0, float(os.getenv("PUBLISH_INTERVAL_SECONDS", "2"))
)

EXPECTED_COLUMNS = [
    "meter_id",
    "source_system",
    "source_meter_id",
    "reading_ts_utc",
    "interval_minutes",
    "active_energy_kwh_import",
    "active_power_kw",
    "reactive_energy_kvarh_import",
    "reactive_power_kvar",
    "voltage_v_baseline",
    "voltage_v",
    "apparent_power_kva",
    "transformer_id",
    "feeder_id",
    "substation_id",
    "transformer_rating_kva",
    "loading_ratio",
    "anomaly_family",
    "anomaly_label",
    "value_status",
    "quality_status",
    "generator_seed",
    "reading_date",
]

INTEGER_COLUMNS = {
    "interval_minutes",
    "anomaly_label",
    "transformer_rating_kva",
    "generator_seed",
}

FLOAT_COLUMNS = {
    "active_energy_kwh_import",
    "active_power_kw",
    "reactive_energy_kvarh_import",
    "reactive_power_kvar",
    "voltage_v_baseline",
    "voltage_v",
    "apparent_power_kva",
    "loading_ratio",
}


def prepare_record(row):
    record = dict(row)

    for column in INTEGER_COLUMNS:
        record[column] = int(record[column])

    for column in FLOAT_COLUMNS:
        record[column] = float(record[column])

    return record


def main():
    if not INPUT_CSV.is_file():
        raise FileNotFoundError(
            f"CSV not found: {INPUT_CSV}. "
            "Check INPUT_CSV and data/raw/sample_meter_readings.csv."
        )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda item: json.dumps(
            item, separators=(",", ":"), allow_nan=False
        ).encode("utf-8"),
        acks="all",
        retries=5,
        batch_size=32768,
        linger_ms=20,
        compression_type="gzip",
    )

    count = 0
    try:
        with INPUT_CSV.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)

            if reader.fieldnames != EXPECTED_COLUMNS:
                raise ValueError(
                    "CSV header does not match the expected 23-column schema.\n"
                    f"Expected: {EXPECTED_COLUMNS}\n"
                    f"Found:    {reader.fieldnames}"
                )

            for timestamp, rows in groupby(
                reader, key=lambda row: row["reading_ts_utc"]
            ):
                futures = []
                batch_count = 0

                for row in rows:
                    if None in row or any(
                        value is None or value.strip() == ""
                        for value in row.values()
                    ):
                        raise ValueError(
                            f"Missing or extra CSV field at timestamp {timestamp!r}"
                        )

                    record = prepare_record(row)
                    futures.append(producer.send(TOPIC, value=record))
                    batch_count += 1

                producer.flush()
                for future in futures:
                    future.get(timeout=30)

                count += batch_count
                print(
                    f"Published {batch_count} records for "
                    f"timestamp={timestamp!r}; total={count}"
                )

                if PUBLISH_INTERVAL_SECONDS > 0:
                    time.sleep(PUBLISH_INTERVAL_SECONDS)

        print(f"Finished. Total records published: {count}")

    except (KafkaError, ValueError, OSError) as exc:
        raise RuntimeError(f"Producer failed for topic {TOPIC!r}") from exc
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
