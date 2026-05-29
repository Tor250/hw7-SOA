from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx


QUERIES: dict[str, str] = {
    "producer_error_rate": (
        'sum(rate(http_request_errors_total{job="producer",endpoint="/events"}[5m])) '
        '/ clamp_min(sum(rate(http_requests_total{job="producer",endpoint="/events"}[5m])), 1)'
    ),
    "producer_p95_latency_seconds": (
        'histogram_quantile(0.95, '
        'sum by (le) (rate(http_request_duration_seconds_bucket{job="producer",endpoint="/events"}[5m])))'
    ),
    "producer_availability": (
        'sum(rate(http_requests_total{job="producer",endpoint="/events",status=~"2.."}[5m])) '
        '/ clamp_min(sum(rate(http_requests_total{job="producer",endpoint="/events"}[5m])), 1)'
    ),
    "analytics_p95_latency_seconds": (
        'histogram_quantile(0.95, '
        'sum by (le) (rate(http_request_duration_seconds_bucket{job="analytics-service",endpoint="/aggregation/run"}[5m])))'
    ),
}


def query_value(client: httpx.Client, expression: str) -> float:
    response = client.get("/api/v1/query", params={"query": expression}, timeout=10.0)
    response.raise_for_status()
    payload = response.json()["data"]["result"]
    if not payload:
        return 0.0
    return float(payload[0]["value"][1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--output", required=True)
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--duration", type=int, default=45)
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(tz=timezone.utc)
    deadline = time.time() + args.duration

    samples: list[dict[str, object]] = []
    with httpx.Client(base_url=args.prometheus_url) as client:
        while True:
            sampled_at = datetime.now(tz=timezone.utc)
            sample = {"sampled_at": sampled_at.isoformat()}
            for metric_name, expression in QUERIES.items():
                try:
                    sample[metric_name] = query_value(client, expression)
                except Exception as exc:  # pragma: no cover - defensive network guard
                    sample[metric_name] = None
                    sample[f"{metric_name}_error"] = str(exc)
            samples.append(sample)
            if time.time() >= deadline:
                break
            time.sleep(args.interval)

    output_path.write_text(
        json.dumps(
            {
                "started_at": started_at.isoformat(),
                "finished_at": datetime.now(tz=timezone.utc).isoformat(),
                "interval_seconds": args.interval,
                "duration_seconds": args.duration,
                "samples": samples,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
