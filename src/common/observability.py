from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest


HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total number of HTTP requests.",
    ["method", "endpoint", "status"],
)
HTTP_REQUEST_ERRORS_TOTAL = Counter(
    "http_request_errors_total",
    "Total number of HTTP request errors.",
    ["method", "endpoint", "error_type"],
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds.",
    ["method", "endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)


def install_http_metrics(app: FastAPI) -> None:
    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        if request.url.path == "/metrics":
            return await call_next(request)

        started_at = time.perf_counter()
        endpoint = request.url.path
        method = request.method
        status = "500"
        error_type: str | None = None

        try:
            response = await call_next(request)
        except Exception as exc:
            error_type = exc.__class__.__name__
            raise
        else:
            status = str(response.status_code)
            if response.status_code >= 500:
                error_type = "server_error"
            elif response.status_code >= 400:
                error_type = "client_error"
            return response
        finally:
            duration = time.perf_counter() - started_at
            HTTP_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status=status).inc()
            HTTP_REQUEST_DURATION_SECONDS.labels(method=method, endpoint=endpoint).observe(duration)
            if error_type is not None:
                HTTP_REQUEST_ERRORS_TOTAL.labels(
                    method=method,
                    endpoint=endpoint,
                    error_type=error_type,
                ).inc()


def metrics_response() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
