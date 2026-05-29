from __future__ import annotations

import argparse
import json
from pathlib import Path


def metric_value(metrics: dict[str, object], name: str, field: str) -> float:
    metric = metrics.get(name)
    if not isinstance(metric, dict):
        raise KeyError(name)
    value = metric.get(field)
    if value is None:
        raise KeyError(f"{name}.{field}")
    return float(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    summary_path = Path(args.summary)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    metrics = payload["metrics"]

    checks = {
        "http_req_failed_rate": {
            "value": metric_value(metrics, "http_req_failed", "value"),
            "threshold": 0.01,
            "relation": "<=",
        },
        "producer_health_rate": {
            "value": metric_value(metrics, 'checks{check:"producer health is 200"}', "value"),
            "threshold": 0.99,
            "relation": ">=",
        },
        "event_accepted_rate": {
            "value": metric_value(metrics, 'checks{check:"event accepted"}', "value"),
            "threshold": 0.99,
            "relation": ">=",
        },
        "producer_events_p95_ms": {
            "value": metric_value(metrics, "http_req_duration{endpoint:events}", "p(95)"),
            "threshold": 500.0,
            "relation": "<=",
        },
    }

    failures: list[str] = []
    for name, item in checks.items():
        value = item["value"]
        threshold = item["threshold"]
        relation = item["relation"]
        if relation == "<=" and value > threshold:
            failures.append(f"{name}={value:.3f} > {threshold:.3f}")
        elif relation == ">=" and value < threshold:
            failures.append(f"{name}={value:.3f} < {threshold:.3f}")

    output_path.write_text(
        json.dumps(
            {
                "source_summary": str(summary_path),
                "checks": checks,
                "passed": not failures,
                "failures": failures,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    if failures:
        raise SystemExit("; ".join(failures))


if __name__ == "__main__":
    main()
