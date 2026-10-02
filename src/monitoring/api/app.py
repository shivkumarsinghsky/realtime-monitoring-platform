"""Query API, live WebSocket feed and dashboard."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from monitoring import db
from monitoring.logs import correlation_id
from monitoring.models import AlertState
from monitoring.processing.state import ALERT_CHANNEL, LIVE_CHANNEL, LatestState

STATIC = Path(__file__).resolve().parent / "static"
log = logging.getLogger("monitoring.api")


class AckRequest(BaseModel):
    by: str = Field(min_length=1, max_length=100)


def create_app(pool: asyncpg.Pool, redis: Redis) -> FastAPI:
    state = LatestState(redis)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield

    app = FastAPI(title="Real-Time Monitoring API", version="1.0.0", lifespan=lifespan)

    @app.middleware("http")
    async def correlation(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        cid = request.headers.get("x-correlation-id") or str(uuid.uuid4())
        token = correlation_id.set(cid)
        try:
            response = await call_next(request)
        finally:
            correlation_id.reset(token)
        response.headers["x-correlation-id"] = cid
        return response

    @app.get("/", include_in_schema=False)
    async def dashboard() -> FileResponse:
        return FileResponse(STATIC / "index.html")

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready() -> JSONResponse:
        checks = {"database": False, "redis": False}
        with contextlib.suppress(Exception):
            checks["database"] = await pool.fetchval("SELECT 1") == 1
        with contextlib.suppress(Exception):
            checks["redis"] = bool(await redis.ping())
        ok = all(checks.values())
        return JSONResponse(
            {"status": "ready" if ok else "not-ready", "checks": checks}, status_code=200 if ok else 503
        )

    @app.get("/metrics", include_in_schema=False)
    async def prometheus() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/sources")
    async def sources() -> list[dict[str, Any]]:
        return await state.sources()

    @app.get("/api/sources/{source_id}/latest")
    async def latest(source_id: str) -> dict[str, Any]:
        value = await state.latest(source_id)
        if value is None:
            raise HTTPException(404, "unknown source")
        return value

    @app.get("/api/sources/{source_id}/series")
    async def series(
        source_id: str,
        metric: Annotated[str, Query(pattern=r"^[a-z][a-z0-9_]{0,63}$")],
        start: datetime | None = None,
        end: datetime | None = None,
        resolution: Literal["raw", "1m"] = "1m",
    ) -> list[dict[str, Any]]:
        end = end or datetime.now(UTC)
        start = start or end - timedelta(hours=1)
        if end <= start or end - start > timedelta(days=31):
            raise HTTPException(400, "range must be positive and at most 31 days")
        if resolution == "raw" and end - start > timedelta(days=1):
            raise HTTPException(400, "raw resolution is limited to 1 day; use 1m")
        return await db.series(pool, source_id, metric, start, end, resolution)

    @app.get("/api/alerts")
    async def alerts(
        state_: Annotated[AlertState | None, Query(alias="state")] = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        return await db.list_alerts(pool, state_, min(max(limit, 1), 500))

    @app.post("/api/alerts/{alert_id}/ack")
    async def ack(alert_id: int, body: AckRequest) -> dict[str, Any]:
        result = await db.acknowledge_alert(pool, alert_id, body.by)
        if result is None:
            raise HTTPException(409, "alert is not open (already acknowledged, resolved, or unknown)")
        return result

    @app.websocket("/ws/live")
    async def ws_live(ws: WebSocket) -> None:
        """Pushes {"type": "readings"|"alert", "data": ...} as the processor publishes them."""
        await ws.accept()
        pubsub = redis.pubsub()
        await pubsub.subscribe(LIVE_CHANNEL, ALERT_CHANNEL)
        try:
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg is None:
                    await asyncio.sleep(0)
                    continue
                channel = msg["channel"].decode()
                kind = "alert" if channel == ALERT_CHANNEL else "readings"
                await ws.send_text(f'{{"type":"{kind}","data":{msg["data"].decode()}}}')
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            await pubsub.unsubscribe()
            await pubsub.aclose()  # type: ignore[no-untyped-call]

    return app
