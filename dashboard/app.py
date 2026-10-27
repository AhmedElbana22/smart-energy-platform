import json
import logging
import os
import threading
from collections import Counter, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError


LOG = logging.getLogger("grid_alert_dashboard")
APP_DIR = Path(__file__).resolve().parent
HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
PORT = int(os.getenv("DASHBOARD_PORT", "8501"))
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "grid-alerts")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "smart-energy-grid-alert-dashboard-v1")
BUFFER_SIZE = max(1, int(os.getenv("ALERT_BUFFER_SIZE", "500")))
DEDUPLICATION_WINDOW = max(BUFFER_SIZE, int(os.getenv("ALERT_DEDUPLICATION_WINDOW", "5000")))


class AlertState:
    def __init__(self):
        self.lock = threading.Lock()
        self.alerts = deque(maxlen=BUFFER_SIZE)
        self.alert_ids = set()
        self.alert_id_order = deque()
        self.counts_by_type = Counter()
        self.counts_by_severity = Counter()
        self.connected = False
        self.last_error = None
        self.invalid_messages = 0
        self.last_alert_at = None

    def set_connection(self, connected, error=None):
        with self.lock:
            self.connected = connected
            self.last_error = error

    def add_alert(self, raw_value, partition, offset, historical):
        try:
            alert = json.loads(raw_value.decode("utf-8"))
            if not isinstance(alert, dict):
                raise ValueError("alert JSON must be an object")
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            with self.lock:
                self.invalid_messages += 1
                self.last_error = f"Invalid alert at partition {partition}, offset {offset}: {exc}"
            LOG.warning("Skipping malformed grid-alerts record at %s:%s: %s", partition, offset, exc)
            return

        alert_id = str(alert.get("alert_id") or f"{partition}:{offset}")
        alert_type = str(alert.get("alert_type") or "UNKNOWN")
        severity = str(alert.get("severity") or "UNKNOWN")
        alert["_dashboard_id"] = alert_id
        alert["_dashboard_historical"] = historical
        alert["_dashboard_received_at"] = datetime.now(timezone.utc).isoformat()

        with self.lock:
            if alert_id in self.alert_ids:
                return

            if len(self.alert_id_order) >= DEDUPLICATION_WINDOW:
                self.alert_ids.discard(self.alert_id_order.popleft())
            self.alert_id_order.append(alert_id)
            self.alert_ids.add(alert_id)
            self.alerts.appendleft(alert)
            self.counts_by_type[alert_type] += 1
            self.counts_by_severity[severity] += 1
            self.last_alert_at = alert["_dashboard_received_at"]

    def snapshot(self, limit):
        with self.lock:
            return {
                "connected": self.connected,
                "topic": TOPIC,
                "group_id": GROUP_ID,
                "alerts": list(self.alerts)[:limit],
                "counts_by_type": dict(self.counts_by_type),
                "counts_by_severity": dict(self.counts_by_severity),
                "alerts_counted_since_start": sum(self.counts_by_type.values()),
                "invalid_messages": self.invalid_messages,
                "last_alert_at": self.last_alert_at,
                "last_error": self.last_error,
            }


STATE = AlertState()
STOP_EVENT = threading.Event()


def consume_alerts():
    while not STOP_EVENT.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
                group_id=GROUP_ID,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=10000,
                max_poll_records=500,
            )
            consumer.subscribe([TOPIC])
            initial_end_offsets = None
            STATE.set_connection(True)
            LOG.info(
                "Consuming topic %s from Kafka group %s at %s",
                TOPIC,
                GROUP_ID,
                BOOTSTRAP_SERVERS,
            )

            while not STOP_EVENT.is_set():
                records_by_partition = consumer.poll(timeout_ms=1000)
                assignments = consumer.assignment()
                if initial_end_offsets is None and assignments:
                    initial_end_offsets = consumer.end_offsets(assignments)

                STATE.set_connection(True)
                for records in records_by_partition.values():
                    for record in records:
                        historical = (
                            initial_end_offsets is not None
                            and record.offset
                            < initial_end_offsets.get(TopicPartition(record.topic, record.partition), 0)
                        )
                        STATE.add_alert(
                            record.value,
                            record.partition,
                            record.offset,
                            historical,
                        )
        except KafkaError as exc:
            message = f"Kafka consumer error: {exc}"
            STATE.set_connection(False, message)
            LOG.exception(message)
            STOP_EVENT.wait(5)
        except Exception as exc:
            message = f"Unexpected consumer error: {exc}"
            STATE.set_connection(False, message)
            LOG.exception(message)
            STOP_EVENT.wait(5)
        finally:
            if consumer is not None:
                consumer.close()


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            with STATE.lock:
                connected = STATE.connected
            self.send_json({"healthy": connected}, 200 if connected else 503)
            return

        if parsed.path == "/api/alerts":
            try:
                limit = int(parse_qs(parsed.query).get("limit", ["100"])[0])
            except ValueError:
                self.send_json({"error": "limit must be an integer"}, 400)
                return
            limit = max(1, min(limit, BUFFER_SIZE))
            self.send_json(STATE.snapshot(limit))
            return

        if parsed.path not in ("/", "/index.html"):
            self.send_error(404)
            return

        content = (APP_DIR / "index.html").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def send_json(self, payload, status=200):
        content = json.dumps(payload, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format_string, *args):
        LOG.info("%s - %s", self.address_string(), format_string % args)


def main():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    consumer_thread = threading.Thread(target=consume_alerts, name="grid-alert-consumer", daemon=True)
    consumer_thread.start()

    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)
    server.daemon_threads = True
    LOG.info("Dashboard listening on http://%s:%s", HOST, PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOG.info("Stopping dashboard")
    finally:
        STOP_EVENT.set()
        server.server_close()
        consumer_thread.join(timeout=10)


if __name__ == "__main__":
    main()
