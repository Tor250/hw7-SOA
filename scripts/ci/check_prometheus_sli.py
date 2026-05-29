from __future__ import annotations

import argparse
import json
from pathlib import Path

import httpx


CHECKS: dict[str, tuple[str, float, str]] = {
    "producer_error_rate": (
        'sum(rate(http_request_errors_total{job="producer",endpoint="/events"}[5m])) '
        '/ clamp_min(sum(rate(http_requests_total{job="producer",endpoint="/events"}[5m])), 1)',
        0.01,
        "<=",
    ),
    "producer_p95_latency_seconds": (
        'histogram_quantile(0.95, '
        'sum by (le) (rate(http_request_duration_seconds_bucket{job="producer",endpoint="/events"}[5m])))',
        0.5,
        "<=",
    ),
    "producer_availability": (
        'sum(rate(http_requests_total{job="producer",endpoint="/events",status=~"2.."}[5m])) '
        '/ clamp_min(sum(rate(http_requests_total{job="producer",endpoint="/events"}[5m])), 1)',
        0.99,
        ">=",
    ),
    "analytics_p95_latency_seconds": (
        'histogram_quantile(0.95, '
        'sum by (le) (rate(http_request_duration_seconds_bucket{job="analytics-service",endpoint="/aggregation/run"}[5m])))',
        1.0,
        "<=",
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
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    results: dict[str, object] = {}
    failures: list[str] = []

    with httpx.Client(base_url=args.prometheus_url) as client:
        for metric_name, (expression, threshold, relation) in CHECKS.items():
            value = query_value(client, expression)
            results[metric_name] = value
            results[f"{metric_name}_threshold"] = threshold
            results[f"{metric_name}_relation"] = relation

            if relation == "<=" and value > threshold:
                failures.append(f"{metric_name}={value:.4f} is above {threshold:.4f}")
            elif relation == ">=" and value < threshold:
                failures.append(f"{metric_name}={value:.4f} is below {threshold:.4f}")

    output_path.write_text(
        json.dumps(results, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if failures:
        raise SystemExit("; ".join(failures))


if __name__ == "__main__":
    main()
