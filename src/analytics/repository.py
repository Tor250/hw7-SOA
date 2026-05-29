from __future__ import annotations

import json
from datetime import date
import logging
from pathlib import Path
from typing import Any

import clickhouse_connect
import psycopg
from psycopg.rows import dict_row
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.analytics.settings import AnalyticsSettings


LOGGER = logging.getLogger(__name__)


class AnalyticsRepository:
    def __init__(self, settings: AnalyticsSettings) -> None:
        self.settings = settings
        self.clickhouse_client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_username,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    def close(self) -> None:
        self.clickhouse_client.close()

    def apply_postgres_migrations(self) -> None:
        migrations_dir = Path("/app/infra/postgres/migrations")
        with psycopg.connect(self.settings.postgres_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version TEXT PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                for path in sorted(migrations_dir.glob("*.sql")):
                    cur.execute(
                        "SELECT 1 FROM schema_migrations WHERE version = %s",
                        (path.name,),
                    )
                    if cur.fetchone():
                        continue
                    cur.execute(path.read_text(encoding="utf-8"))
                    cur.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s)",
                        (path.name,),
                    )

    def raw_event_count(self) -> int:
        result = self.clickhouse_client.query(
            "SELECT count() FROM analytics.movie_events",
        )
        return int(result.first_row[0]) if result.first_row else 0

    def rebuild_clickhouse_aggregates(self) -> list[date]:
        self.clickhouse_client.command("TRUNCATE TABLE analytics.daily_metrics")
        self.clickhouse_client.command("TRUNCATE TABLE analytics.top_movies_daily")
        self.clickhouse_client.command("TRUNCATE TABLE analytics.retention_cohort_daily")
        self.clickhouse_client.command("TRUNCATE TABLE analytics.device_distribution_daily")

        self.clickhouse_client.command(
            """
            INSERT INTO analytics.top_movies_daily
            SELECT
                metric_date,
                rank,
                movie_id,
                views_count,
                now64(3) AS computed_at
            FROM
            (
                SELECT
                    metric_date,
                    movie_id,
                    views_count,
                    row_number() OVER (
                        PARTITION BY metric_date
                        ORDER BY views_count DESC, movie_id ASC
                    ) AS rank
                FROM
                (
                    SELECT
                        event_date AS metric_date,
                        movie_id,
                        countIf(event_type = 'VIEW_STARTED') AS views_count
                    FROM analytics.movie_events
                    WHERE movie_id != ''
                    GROUP BY event_date, movie_id
                )
            )
            WHERE rank <= 10
            """
        )

        self.clickhouse_client.command(
            """
            INSERT INTO analytics.device_distribution_daily
            SELECT
                event_date AS metric_date,
                device_type,
                uniqExact(user_id) AS active_users,
                now64(3) AS computed_at
            FROM analytics.movie_events
            GROUP BY metric_date, device_type
            """
        )

        self.clickhouse_client.command(
            """
            INSERT INTO analytics.retention_cohort_daily
            WITH first_views AS
            (
                SELECT
                    user_id,
                    min(event_date) AS cohort_date
                FROM analytics.movie_events
                WHERE event_type = 'VIEW_STARTED'
                GROUP BY user_id
            ),
            cohort_sizes AS
            (
                SELECT
                    cohort_date,
                    uniqExact(user_id) AS cohort_size
                FROM first_views
                GROUP BY cohort_date
            ),
            daily_activity AS
            (
                SELECT DISTINCT
                    user_id,
                    event_date AS activity_date
                FROM analytics.movie_events
                WHERE event_type IN ('VIEW_STARTED', 'VIEW_FINISHED', 'VIEW_PAUSED', 'VIEW_RESUMED', 'LIKED', 'SEARCHED')
            )
            SELECT
                fv.cohort_date,
                da.activity_date,
                toUInt8(dateDiff('day', fv.cohort_date, da.activity_date)) AS day_number,
                uniqExact(fv.user_id) AS retained_users,
                cs.cohort_size,
                round(retained_users / nullIf(toFloat64(cs.cohort_size), 0), 4) AS retention_rate,
                now64(3) AS computed_at
            FROM first_views AS fv
            INNER JOIN daily_activity AS da USING (user_id)
            INNER JOIN cohort_sizes AS cs USING (cohort_date)
            WHERE dateDiff('day', fv.cohort_date, da.activity_date) BETWEEN 0 AND 7
            GROUP BY
                fv.cohort_date,
                da.activity_date,
                day_number,
                cs.cohort_size
            ORDER BY fv.cohort_date, day_number
            """
        )

        self.clickhouse_client.command(
            """
            INSERT INTO analytics.daily_metrics
            SELECT
                metric_date,
                metric_name,
                metric_value,
                now64(3) AS computed_at
            FROM
            (
                SELECT
                    event_date AS metric_date,
                    'dau' AS metric_name,
                    toFloat64(uniqExact(user_id)) AS metric_value
                FROM analytics.movie_events
                GROUP BY metric_date

                UNION ALL

                SELECT
                    event_date AS metric_date,
                    'avg_watch_time_seconds' AS metric_name,
                    if(
                        countIf(event_type = 'VIEW_FINISHED') = 0,
                        0.0,
                        round(avgIf(toFloat64(progress_seconds), event_type = 'VIEW_FINISHED'), 2)
                    ) AS metric_value
                FROM analytics.movie_events
                GROUP BY metric_date

                UNION ALL

                SELECT
                    event_date AS metric_date,
                    'view_finish_conversion' AS metric_name,
                    if(
                        countIf(event_type = 'VIEW_STARTED') = 0,
                        0.0,
                        round(
                            countIf(event_type = 'VIEW_FINISHED') / toFloat64(countIf(event_type = 'VIEW_STARTED')),
                            4
                        )
                    ) AS metric_value
                FROM analytics.movie_events
                GROUP BY metric_date

                UNION ALL

                SELECT
                    cohort_date AS metric_date,
                    'retention_d1' AS metric_name,
                    coalesce(maxIf(retention_rate, day_number = 1), 0.0) AS metric_value
                FROM analytics.retention_cohort_daily
                GROUP BY cohort_date

                UNION ALL

                SELECT
                    cohort_date AS metric_date,
                    'retention_d7' AS metric_name,
                    coalesce(maxIf(retention_rate, day_number = 7), 0.0) AS metric_value
                FROM analytics.retention_cohort_daily
                GROUP BY cohort_date
            )
            ORDER BY metric_date, metric_name
            """
        )

        dates_result = self.clickhouse_client.query(
            "SELECT DISTINCT metric_date FROM analytics.daily_metrics ORDER BY metric_date",
        )
        return [row[0] for row in dates_result.result_rows]

    @retry(
        retry=retry_if_exception_type(psycopg.Error),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def sync_postgres_from_clickhouse(self) -> None:
        daily_metrics = self.clickhouse_client.query(
            """
            SELECT metric_date, metric_name, metric_value, computed_at
            FROM analytics.daily_metrics
            ORDER BY metric_date, metric_name
            """
        ).result_rows
        top_movies = self.clickhouse_client.query(
            """
            SELECT metric_date, rank, movie_id, views_count, computed_at
            FROM analytics.top_movies_daily
            ORDER BY metric_date, rank
            """
        ).result_rows
        retention_rows = self.clickhouse_client.query(
            """
            SELECT
                cohort_date,
                activity_date,
                day_number,
                retained_users,
                cohort_size,
                retention_rate,
                computed_at
            FROM analytics.retention_cohort_daily
            ORDER BY cohort_date, day_number
            """
        ).result_rows
        device_rows = self.clickhouse_client.query(
            """
            SELECT metric_date, device_type, active_users, computed_at
            FROM analytics.device_distribution_daily
            ORDER BY metric_date, device_type
            """
        ).result_rows

        with psycopg.connect(self.settings.postgres_dsn) as conn:
            with conn.cursor() as cur:
                metric_dates = sorted({row[0] for row in daily_metrics})
                top_movie_dates = sorted({row[0] for row in top_movies})
                retention_dates = sorted({row[0] for row in retention_rows})
                device_dates = sorted({row[0] for row in device_rows})

                if metric_dates:
                    cur.execute(
                        "DELETE FROM metric_values WHERE metric_date = ANY(%s)",
                        (metric_dates,),
                    )
                if top_movie_dates:
                    cur.execute(
                        "DELETE FROM top_movies WHERE metric_date = ANY(%s)",
                        (top_movie_dates,),
                    )
                if retention_dates:
                    cur.execute(
                        "DELETE FROM retention_cohort WHERE cohort_date = ANY(%s)",
                        (retention_dates,),
                    )
                if device_dates:
                    cur.execute(
                        "DELETE FROM device_distribution WHERE metric_date = ANY(%s)",
                        (device_dates,),
                    )

                for row in daily_metrics:
                    cur.execute(
                        """
                        INSERT INTO metric_values (metric_date, metric_name, metric_value, computed_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (metric_date, metric_name)
                        DO UPDATE SET
                            metric_value = EXCLUDED.metric_value,
                            computed_at = EXCLUDED.computed_at
                        """,
                        row,
                    )

                for row in top_movies:
                    cur.execute(
                        """
                        INSERT INTO top_movies (metric_date, rank, movie_id, views_count, computed_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (metric_date, rank)
                        DO UPDATE SET
                            movie_id = EXCLUDED.movie_id,
                            views_count = EXCLUDED.views_count,
                            computed_at = EXCLUDED.computed_at
                        """,
                        row,
                    )

                for row in retention_rows:
                    cur.execute(
                        """
                        INSERT INTO retention_cohort (cohort_date, activity_date, day_number, retained_users, cohort_size, retention_rate, computed_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (cohort_date, day_number)
                        DO UPDATE SET
                            activity_date = EXCLUDED.activity_date,
                            retained_users = EXCLUDED.retained_users,
                            cohort_size = EXCLUDED.cohort_size,
                            retention_rate = EXCLUDED.retention_rate,
                            computed_at = EXCLUDED.computed_at
                        """,
                        row,
                    )

                for row in device_rows:
                    cur.execute(
                        """
                        INSERT INTO device_distribution (metric_date, device_type, active_users, computed_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (metric_date, device_type)
                        DO UPDATE SET
                            active_users = EXCLUDED.active_users,
                            computed_at = EXCLUDED.computed_at
                        """,
                        row,
                    )
            conn.commit()
        LOGGER.info(
            "Synced aggregates to PostgreSQL: metrics=%s top_movies=%s retention=%s devices=%s",
            len(daily_metrics),
            len(top_movies),
            len(retention_rows),
            len(device_rows),
        )

    def metric_snapshot_for_date(self, target_date: date) -> dict[str, Any]:
        with psycopg.connect(self.settings.postgres_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT metric_name, metric_value, computed_at
                    FROM metric_values
                    WHERE metric_date = %s
                    ORDER BY metric_name
                    """,
                    (target_date,),
                )
                metrics = cur.fetchall()
                cur.execute(
                    """
                    SELECT rank, movie_id, views_count, computed_at
                    FROM top_movies
                    WHERE metric_date = %s
                    ORDER BY rank
                    """,
                    (target_date,),
                )
                top_movies = cur.fetchall()
                cur.execute(
                    """
                    SELECT day_number, retention_rate, retained_users, cohort_size, computed_at
                    FROM retention_cohort
                    WHERE cohort_date = %s
                    ORDER BY day_number
                    """,
                    (target_date,),
                )
                retention = cur.fetchall()
        return {
            "metric_date": target_date.isoformat(),
            "metrics": metrics,
            "top_movies": top_movies,
            "retention": retention,
        }

    def export_payload_for_date(self, target_date: date) -> dict[str, Any]:
        snapshot = self.metric_snapshot_for_date(target_date)
        with psycopg.connect(self.settings.postgres_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT device_type, active_users, computed_at
                    FROM device_distribution
                    WHERE metric_date = %s
                    ORDER BY device_type
                    """,
                    (target_date,),
                )
                snapshot["device_distribution"] = cur.fetchall()
        return json.loads(json.dumps(snapshot, default=str))
