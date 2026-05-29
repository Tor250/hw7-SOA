from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import clickhouse_connect

from src.common.logging_utils import configure_logging


configure_logging()
LOGGER = logging.getLogger(__name__)


def wait_for_clickhouse(attempts: int = 60) -> clickhouse_connect.driver.client.Client:
    username = os.getenv("CLICKHOUSE_USER", "analytics")
    password = os.getenv("CLICKHOUSE_PASSWORD", "analytics")
    database = os.getenv("CLICKHOUSE_DATABASE", "default")
    for attempt in range(1, attempts + 1):
        try:
            client = clickhouse_connect.get_client(
                host="clickhouse",
                port=8123,
                username=username,
                password=password,
                database=database,
            )
            client.command("SELECT 1")
            LOGGER.info("ClickHouse is ready after %s attempts", attempt)
            return client
        except Exception as exc:
            LOGGER.info("Waiting for ClickHouse (%s/%s): %s", attempt, attempts, exc)
            time.sleep(2)
    raise TimeoutError("ClickHouse is not ready")


def main() -> None:
    client = wait_for_clickhouse()
    sql_dir = Path("/app/infra/clickhouse/sql")
    for path in sorted(sql_dir.glob("*.sql")):
        statement = path.read_text(encoding="utf-8").strip()
        if not statement:
            continue
        client.command(statement)
        LOGGER.info("Applied ClickHouse SQL: %s", path.name)
    client.close()


if __name__ == "__main__":
    main()
