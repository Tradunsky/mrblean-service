"""Mock checkout service with Prometheus metrics, Loki logs, and lab fault injection."""

from __future__ import annotations

import os
import random
import threading
import time
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    Info,
    generate_latest,
)
from pydantic import BaseModel, Field

SERVICE = os.environ.get("SERVICE_NAME", "mrblean-service")
LOKI_URL = os.environ.get("LOKI_URL", "http://127.0.0.1:3100").rstrip("/")
DEPLOY_VERSION = os.environ.get("DEPLOY_VERSION", "1.0.0")
DEPLOY_SHA = os.environ.get("DEPLOY_SHA", "abc1234")

# Intentional deploy regression flag (coding-agent RCA evidence).
# Healthy baseline keeps this False. E2E commits flip it True so checkout
# returns elevated 500s that are visible in the tree at the deploy SHA.
CHECKOUT_PAYMENT_VALIDATION_BROKEN = False
# Default memory limit used for OOM simulation (bytes).
DEFAULT_MEMORY_LIMIT = int(os.environ.get("MEMORY_LIMIT_BYTES", str(256 * 1024 * 1024)))

REQUESTS = Counter(
    "http_requests_total",
    "HTTP requests",
    ["service", "route", "status"],
)
ERRORS = Counter(
    "checkout_errors_total",
    "Checkout failures",
    ["service"],
)
LATENCY = Histogram(
    "http_request_duration_seconds",
    "Request latency",
    ["service", "route"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0),
)
MEMORY_USED = Gauge(
    "service_memory_used_bytes",
    "Simulated resident memory used by the service",
    ["service"],
)
MEMORY_LIMIT = Gauge(
    "service_memory_limit_bytes",
    "Simulated memory limit for the service",
    ["service"],
)
DEPLOY_INFO = Info(
    "deploy",
    "Current deployment metadata",
)

app = FastAPI(title=SERVICE)

_lock = threading.Lock()
_state: dict[str, Any] = {
    "mode": "clear",
    "error_rate": 0.02,
    "checkout_error_rate": 0.02,
    "pay_error_rate": 0.02,
    "checkout_latency_seconds": 0.05,
    "search_latency_seconds": 0.08,
    "memory_used_bytes": 64 * 1024 * 1024,
    "memory_limit_bytes": DEFAULT_MEMORY_LIMIT,
    "deploy_version": DEPLOY_VERSION,
    "deploy_sha": DEPLOY_SHA,
    "extra": {},
}


def _apply_metrics_from_state() -> None:
    used = float(_state["memory_used_bytes"])
    limit = float(_state["memory_limit_bytes"])
    MEMORY_USED.labels(service=SERVICE).set(used)
    MEMORY_LIMIT.labels(service=SERVICE).set(limit)
    DEPLOY_INFO.info(
        {
            "service": SERVICE,
            "version": str(_state["deploy_version"]),
            "sha": str(_state["deploy_sha"]),
        }
    )


def push_log(level: str, message: str) -> None:
    now_ns = str(int(time.time() * 1e9))
    body = {
        "streams": [
            {
                "stream": {
                    "service": SERVICE,
                    "job": SERVICE,
                    "level": level,
                },
                "values": [[now_ns, message]],
            }
        ]
    }
    try:
        httpx.post(f"{LOKI_URL}/loki/api/v1/push", json=body, timeout=2.0)
    except Exception:
        # Lab must keep serving even if Loki is briefly down.
        pass


def _record(route: str, status: str, duration: float, level: str, message: str) -> None:
    REQUESTS.labels(service=SERVICE, route=route, status=status).inc()
    if route == "/checkout" and status == "500":
        ERRORS.labels(service=SERVICE).inc()
    LATENCY.labels(service=SERVICE, route=route).observe(duration)
    push_log(level, message)


def _simulate_request(route: str) -> tuple[str, float, str, str]:
    """Return status, duration, log level, log message for a synthetic request."""
    with _lock:
        mode = _state["mode"]
        checkout_err = float(_state["checkout_error_rate"])
        pay_err = float(_state["pay_error_rate"])
        checkout_lat = float(_state["checkout_latency_seconds"])
        search_lat = float(_state["search_latency_seconds"])
        version = str(_state["deploy_version"])
        sha = str(_state["deploy_sha"])

    if route == "/checkout":
        # Artificial latency for high_latency / baseline.
        base = checkout_lat if mode == "high_latency" else max(0.02, min(checkout_lat, 0.08))
        # Add small jitter.
        duration = max(0.01, random.gauss(base, base * 0.1))
        if mode == "high_latency":
            duration = max(duration, checkout_lat * 0.9)

        # Broken deploy: payment validation regresses and checkout fails often.
        effective_err = 0.75 if CHECKOUT_PAYMENT_VALIDATION_BROKEN else checkout_err
        fail = random.random() < effective_err
        if fail:
            if CHECKOUT_PAYMENT_VALIDATION_BROKEN or mode == "deploy_500s":
                msg = (
                    f"checkout failed status=500 service={SERVICE} "
                    f"after recent deploy version={version} sha={sha} "
                    f"cause=CHECKOUT_PAYMENT_VALIDATION_BROKEN"
                )
            else:
                msg = f"checkout failed status=500 service={SERVICE}"
            return "500", duration, "error", msg
        return (
            "200",
            duration,
            "info",
            f"checkout ok status=200 service={SERVICE} version={version}",
        )

    if route == "/pay":
        duration = max(0.01, random.gauss(0.04, 0.01))
        fail = random.random() < pay_err
        if fail:
            return (
                "500",
                duration,
                "error",
                f"pay endpoint error status=500 route=/pay service={SERVICE} "
                f"api_orders_misbehaving=true",
            )
        return "200", duration, "info", f"pay ok status=200 service={SERVICE}"

    if route == "/api/search":
        base = search_lat if mode == "high_latency" else 0.06
        duration = max(0.01, random.gauss(base, base * 0.1))
        if mode == "high_latency":
            duration = max(duration, search_lat * 0.9)
        return (
            "200",
            duration,
            "info",
            f"search ok status=200 service={SERVICE} latency_ms={int(duration * 1000)}",
        )

    # /api/orders — healthier companion route except under endpoint_errors
    duration = max(0.01, random.gauss(0.03, 0.005))
    if mode == "endpoint_errors" and random.random() < pay_err:
        return (
            "500",
            duration,
            "error",
            f"api/orders misbehaving status=500 route=/api/orders service={SERVICE}",
        )
    return "200", duration, "info", f"orders ok status=200 service={SERVICE}"


def traffic_loop() -> None:
    routes_by_mode = {
        "clear": ["/checkout", "/pay", "/api/search", "/api/orders"],
        "deploy_500s": ["/checkout", "/checkout", "/pay", "/api/search"],
        "oom": ["/checkout", "/pay", "/api/search"],
        "endpoint_errors": ["/pay", "/pay", "/api/orders", "/checkout", "/api/search"],
        "high_latency": ["/checkout", "/api/search", "/api/search", "/pay"],
    }
    while True:
        with _lock:
            mode = _state["mode"]
            mem_used = int(_state["memory_used_bytes"])
            mem_limit = int(_state["memory_limit_bytes"])
        routes = routes_by_mode.get(mode, routes_by_mode["clear"])
        route = random.choice(routes)
        status, duration, level, message = _simulate_request(route)
        _record(route, status, duration, level, message)

        if mode == "oom":
            # Periodically emit memory pressure / OOM-style logs.
            if random.random() < 0.35:
                pct = (mem_used / mem_limit * 100.0) if mem_limit else 0.0
                push_log(
                    "error",
                    f"OOM risk: memory pressure service={SERVICE} "
                    f"used_bytes={mem_used} limit_bytes={mem_limit} "
                    f"usage_pct={pct:.1f} message=out of memory killer impending",
                )

        # Keep gauges fresh.
        with _lock:
            _apply_metrics_from_state()

        time.sleep(0.5)


class FaultRequest(BaseModel):
    mode: str = Field(..., description="clear|deploy_500s|oom|endpoint_errors|high_latency")
    error_rate: float | None = None
    checkout_error_rate: float | None = None
    pay_error_rate: float | None = None
    checkout_latency_seconds: float | None = None
    search_latency_seconds: float | None = None
    memory_used_bytes: int | None = None
    memory_limit_bytes: int | None = None
    deploy_version: str | None = None
    deploy_sha: str | None = None


def _set_mode(req: FaultRequest) -> dict[str, Any]:
    mode = req.mode.strip().lower()
    with _lock:
        if mode == "clear":
            _state.update(
                {
                    "mode": "clear",
                    "error_rate": 0.02,
                    "checkout_error_rate": 0.02,
                    "pay_error_rate": 0.02,
                    "checkout_latency_seconds": 0.05,
                    "search_latency_seconds": 0.06,
                    "memory_used_bytes": 64 * 1024 * 1024,
                    "memory_limit_bytes": DEFAULT_MEMORY_LIMIT,
                }
            )
        elif mode == "deploy_500s":
            _state.update(
                {
                    "mode": "deploy_500s",
                    "checkout_error_rate": req.checkout_error_rate
                    if req.checkout_error_rate is not None
                    else (req.error_rate if req.error_rate is not None else 0.75),
                    "pay_error_rate": 0.02,
                    "checkout_latency_seconds": 0.08,
                    "search_latency_seconds": 0.06,
                    "memory_used_bytes": 64 * 1024 * 1024,
                    "memory_limit_bytes": DEFAULT_MEMORY_LIMIT,
                    "deploy_version": req.deploy_version or f"1.{int(time.time()) % 1000}.0",
                    "deploy_sha": req.deploy_sha or f"dead{int(time.time()) % 10000:04x}",
                }
            )
        elif mode == "oom":
            limit = req.memory_limit_bytes or DEFAULT_MEMORY_LIMIT
            used = req.memory_used_bytes or int(limit * 1.05)
            _state.update(
                {
                    "mode": "oom",
                    "checkout_error_rate": 0.05,
                    "pay_error_rate": 0.05,
                    "checkout_latency_seconds": 0.1,
                    "search_latency_seconds": 0.08,
                    "memory_used_bytes": used,
                    "memory_limit_bytes": limit,
                }
            )
        elif mode == "endpoint_errors":
            _state.update(
                {
                    "mode": "endpoint_errors",
                    "checkout_error_rate": 0.02,
                    "pay_error_rate": req.pay_error_rate
                    if req.pay_error_rate is not None
                    else (req.error_rate if req.error_rate is not None else 0.8),
                    "checkout_latency_seconds": 0.05,
                    "search_latency_seconds": 0.06,
                    "memory_used_bytes": 64 * 1024 * 1024,
                    "memory_limit_bytes": DEFAULT_MEMORY_LIMIT,
                }
            )
        elif mode == "high_latency":
            _state.update(
                {
                    "mode": "high_latency",
                    "checkout_error_rate": 0.02,
                    "pay_error_rate": 0.02,
                    "checkout_latency_seconds": req.checkout_latency_seconds
                    if req.checkout_latency_seconds is not None
                    else 2.5,
                    "search_latency_seconds": req.search_latency_seconds
                    if req.search_latency_seconds is not None
                    else 2.0,
                    "memory_used_bytes": 64 * 1024 * 1024,
                    "memory_limit_bytes": DEFAULT_MEMORY_LIMIT,
                }
            )
        else:
            raise ValueError(
                f"unknown mode {mode!r}; expected clear|deploy_500s|oom|endpoint_errors|high_latency"
            )

        # Optional overrides for any mode.
        if req.deploy_version is not None and mode != "clear":
            _state["deploy_version"] = req.deploy_version
        if req.deploy_sha is not None and mode != "clear":
            _state["deploy_sha"] = req.deploy_sha
        if req.memory_used_bytes is not None and mode not in ("oom",):
            _state["memory_used_bytes"] = req.memory_used_bytes
        if req.memory_limit_bytes is not None and mode not in ("oom",):
            _state["memory_limit_bytes"] = req.memory_limit_bytes

        _apply_metrics_from_state()
        snapshot = dict(_state)

    push_log("info", f"lab fault mode set to {snapshot['mode']} service={SERVICE}")
    if snapshot["mode"] == "deploy_500s":
        push_log(
            "warn",
            f"deployment rolled out version={snapshot['deploy_version']} "
            f"sha={snapshot['deploy_sha']} service={SERVICE}",
        )
    if snapshot["mode"] == "oom":
        push_log(
            "error",
            f"memory limit approached used_bytes={snapshot['memory_used_bytes']} "
            f"limit_bytes={snapshot['memory_limit_bytes']} service={SERVICE}",
        )
    return snapshot


@app.on_event("startup")
def _start_background() -> None:
    with _lock:
        _apply_metrics_from_state()
    thread = threading.Thread(target=traffic_loop, name="traffic", daemon=True)
    thread.start()
    push_log("info", f"{SERVICE} started mode={_state['mode']}")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"service": SERVICE, "ok": "true"}


@app.get("/lab/status")
def lab_status() -> dict[str, Any]:
    with _lock:
        snap = dict(_state)
    return {
        "service": SERVICE,
        "mode": snap["mode"],
        "gauges": {
            "checkout_error_rate": snap["checkout_error_rate"],
            "pay_error_rate": snap["pay_error_rate"],
            "checkout_latency_seconds": snap["checkout_latency_seconds"],
            "search_latency_seconds": snap["search_latency_seconds"],
            "memory_used_bytes": snap["memory_used_bytes"],
            "memory_limit_bytes": snap["memory_limit_bytes"],
            "deploy_version": snap["deploy_version"],
            "deploy_sha": snap["deploy_sha"],
        },
    }


@app.post("/lab/fault")
def lab_fault(body: FaultRequest) -> dict[str, Any]:
    try:
        snap = _set_mode(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "mode": snap["mode"], "gauges": lab_status()["gauges"]}


@app.post("/checkout")
def checkout() -> Response:
    status, duration, level, message = _simulate_request("/checkout")
    _record("/checkout", status, duration, level, message)
    if status == "500":
        return Response(content='{"ok":false}', media_type="application/json", status_code=500)
    return Response(content='{"ok":true}', media_type="application/json")


@app.post("/pay")
def pay() -> Response:
    status, duration, level, message = _simulate_request("/pay")
    _record("/pay", status, duration, level, message)
    if status == "500":
        return Response(content='{"ok":false}', media_type="application/json", status_code=500)
    return Response(content='{"ok":true}', media_type="application/json")


@app.get("/api/search")
def api_search() -> Response:
    status, duration, level, message = _simulate_request("/api/search")
    _record("/api/search", status, duration, level, message)
    return Response(
        content='{"ok":true,"results":[]}',
        media_type="application/json",
        status_code=200 if status == "200" else 500,
    )


@app.get("/api/orders")
def api_orders() -> Response:
    status, duration, level, message = _simulate_request("/api/orders")
    _record("/api/orders", status, duration, level, message)
    if status == "500":
        return Response(content='{"ok":false}', media_type="application/json", status_code=500)
    return Response(content='{"ok":true,"orders":[]}', media_type="application/json")


@app.get("/metrics")
def metrics() -> Response:
    with _lock:
        _apply_metrics_from_state()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
