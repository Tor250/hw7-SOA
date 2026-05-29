from __future__ import annotations

import logging
import os
import threading
import time

import clickhouse_connect
from prometheus_client import Gauge, start_http_server

from src.common.logging_utils import configure_logging


configure_logging()
LOGGER = logging.getLogger(__name__)

CLICKHOUSE_UP = Gauge("clickhouse_up", "Whether the ClickHouse exporter can reach the database.")
CLICKHOUSE_SYSTEM_METRIC = Gauge(
    "clickhouse_system_metric",
    "Raw ClickHouse system.metrics values.",
    ["metric"],
)
CLICKHOUSE_SYSTEM_EVENT = Gauge(
    "clickhouse_system_event",
    "Raw ClickHouse system.events values.",
    ["event"],
)

SYSTEM_METRICS = (
    "MemoryTracking",
    "Query",
    "DelayedInserts",
)
SYSTEM_EVENTS = (
    "Query",
    "SelectQuery",
    "InsertedRows",
    "InsertedBytes",
)


def build_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "analytics"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "analytics"),
        database=os.getenv("CLICKHOUSE_DATABASE", "analytics"),
    )


def refresh_metrics(client: clickhouse_connect.driver.Client) -> None:
    metric_list = ", ".join(f"'{metric}'" for metric in SYSTEM_METRICS)
    event_list = ", ".join(f"'{event}'" for event in SYSTEM_EVENTS)
    metric_rows = client.query(
        """
        SELECT metric, value
        FROM system.metrics
        WHERE metric IN ({metric_list})
        """.format(metric_list=metric_list),
    ).result_rows
    event_rows = client.query(
        """
        SELECT event, value
        FROM system.events
        WHERE event IN ({event_list})
        """.format(event_list=event_list),
    ).result_rows

    CLICKHOUSE_UP.set(1)
    for metric, value in metric_rows:
        CLICKHOUSE_SYSTEM_METRIC.labels(metric=metric).set(float(value))
    for event, value in event_rows:
        CLICKHOUSE_SYSTEM_EVENT.labels(event=event).set(float(value))


def collector_loop(refresh_interval_seconds: int) -> None:
    client = build_client()
    while True:
        try:
            refresh_metrics(client)
        except Exception:
            LOGGER.exception("Failed to refresh ClickHouse metrics")
            CLICKHOUSE_UP.set(0)
        time.sleep(refresh_interval_seconds)


def main() -> None:
    exporter_port = int(os.getenv("EXPORTER_PORT", "9108"))
    refresh_interval_seconds = int(os.getenv("REFRESH_INTERVAL_SECONDS", "15"))
    start_http_server(exporter_port)
    LOGGER.info("ClickHouse exporter is listening on %s", exporter_port)

    thread = threading.Thread(target=collector_loop, args=(refresh_interval_seconds,), daemon=True)
    thread.start()
    thread.join()


if __name__ == "__main__":
    main()
