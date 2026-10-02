"""Service health prober: polls HTTP health endpoints and publishes `up` and `latency_ms` as telemetry, so
service monitoring uses the same pipeline, storage, rules and dashboard as asset and host monitoring."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime

import aiomqtt
import httpx

from monitoring.config import Settings


def parse_targets(spec: str) -> dict[str, str]:
    targets = {}
    for item in filter(None, (s.strip() for s in spec.split(","))):
        name, _, url = item.partition("=")
        if not name or not url.startswith(("http://", "https://")):
            raise ValueError(f"invalid probe target '{item}', expected name=http(s)://...")
        targets[name] = url
    return targets


async def probe_once(client: httpx.AsyncClient, url: str) -> tuple[float, float]:
    started = time.perf_counter()
    try:
        response = await client.get(url, timeout=5.0)
        up = 1.0 if response.status_code < 500 else 0.0
    except httpx.HTTPError:
        up = 0.0
    return up, (time.perf_counter() - started) * 1000


async def run(settings: Settings) -> None:
    targets = parse_targets(settings.probe_targets)
    async with (
        httpx.AsyncClient() as http,
        aiomqtt.Client(settings.mqtt_host, settings.mqtt_port, identifier="prober") as mqtt,
    ):
        while True:
            for name, url in targets.items():
                up, latency = await probe_once(http, url)
                payload = {"ts": datetime.now(UTC).isoformat(), "metrics": {"up": up, "latency_ms": latency}}
                await mqtt.publish(f"telemetry/platform/service/{name}", json.dumps(payload), qos=1)
            await asyncio.sleep(settings.probe_interval_seconds)
