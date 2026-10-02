"""Publishes realistic telemetry over MQTT for demos and load tests.

Assets report vibration and bearing temperature (random walk); hosts report CPU, memory and disk. Use
`--anomaly SOURCE` to drive one asset's vibration above the alert threshold.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
from datetime import UTC, datetime

import aiomqtt


async def simulate(
    host: str, port: int, assets: int, hosts: int, interval: float, anomaly: str | None, seconds: float | None
) -> None:
    state: dict[str, dict[str, float]] = {}
    started = asyncio.get_running_loop().time()
    async with aiomqtt.Client(host, port, identifier="simulator") as client:
        while seconds is None or asyncio.get_running_loop().time() - started < seconds:
            now = datetime.now(UTC).isoformat()
            for i in range(1, assets + 1):
                sid = f"PUMP-{100 + i}"
                s = state.setdefault(sid, {"vibration_mm_s": 2.5, "bearing_temp_c": 55.0})
                s["vibration_mm_s"] = max(0.5, s["vibration_mm_s"] + random.uniform(-0.3, 0.3))
                s["bearing_temp_c"] = max(20.0, s["bearing_temp_c"] + random.uniform(-0.5, 0.5))
                if sid == anomaly:
                    s["vibration_mm_s"] = min(12.0, s["vibration_mm_s"] + 0.8)
                payload = {
                    "ts": now,
                    "metrics": s,
                    "units": {"vibration_mm_s": "mm/s", "bearing_temp_c": "C"},
                }
                await client.publish(f"telemetry/SITE01/asset/{sid}", json.dumps(payload), qos=1)
            for i in range(1, hosts + 1):
                metrics = {
                    "cpu_pct": random.uniform(10, 70),
                    "mem_pct": random.uniform(30, 80),
                    "disk_free_pct": random.uniform(20, 60),
                }
                payload = {"ts": now, "metrics": metrics, "units": {k: "%" for k in metrics}}
                await client.publish(f"telemetry/DC1/host/host-{i:02d}", json.dumps(payload), qos=1)
            await asyncio.sleep(interval)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--assets", type=int, default=5)
    p.add_argument("--hosts", type=int, default=3)
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--anomaly", default=None, help="source id whose vibration should rise, e.g. PUMP-101")
    p.add_argument("--seconds", type=float, default=None)
    a = p.parse_args(argv)
    asyncio.run(simulate(a.host, a.port, a.assets, a.hosts, a.interval, a.anomaly, a.seconds))
