"""Alert rule engine.

Two rule families:
- ThresholdRule: value above/below a trigger level, sustained for a duration, with a hysteresis reset level.
  One alert episode until the value returns past the reset level.
- Staleness: a source that stops reporting (device offline, agent dead, service unreachable) raises an alert
  from the *absence* of data — essential for host and service monitoring.

The engine holds per-(rule, source) state in memory. It is correct because each partition, and therefore each
source, is processed by exactly one processor at a time (ADR-003).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

from monitoring.models import AlertEvent, Reading, Severity, SourceKind


@dataclass(frozen=True)
class ThresholdRule:
    id: str
    metric: str
    op: Literal[">", "<"]
    trigger: float
    reset: float
    sustain_seconds: float
    severity: Severity
    kinds: frozenset[SourceKind] = frozenset(SourceKind)

    def __post_init__(self) -> None:
        if self.op == ">" and not self.reset < self.trigger:
            raise ValueError(f"{self.id}: for '>' rules reset must be below trigger")
        if self.op == "<" and not self.reset > self.trigger:
            raise ValueError(f"{self.id}: for '<' rules reset must be above trigger")

    def breached(self, value: float) -> bool:
        return value > self.trigger if self.op == ">" else value < self.trigger

    def cleared(self, value: float) -> bool:
        return value < self.reset if self.op == ">" else value > self.reset


@dataclass
class _EpisodeState:
    breach_started: datetime | None = None
    open: bool = False


@dataclass
class RuleEngine:
    rules: list[ThresholdRule]
    stale_after: timedelta
    _state: dict[tuple[str, str], _EpisodeState] = field(default_factory=dict)
    _last_seen: dict[str, tuple[str, SourceKind, datetime]] = field(default_factory=dict)
    _stale_open: set[str] = field(default_factory=set)

    def evaluate(self, reading: Reading) -> list[AlertEvent]:
        events: list[AlertEvent] = []
        prev = self._last_seen.get(reading.source_id)
        if prev is None or reading.ts > prev[2]:
            self._last_seen[reading.source_id] = (reading.site, reading.kind, reading.ts)
        if reading.source_id in self._stale_open:
            self._stale_open.discard(reading.source_id)
            events.append(self._stale_event(reading.site, reading.source_id, reading.kind, "resolved", reading.ts))
        for rule in self.rules:
            if rule.metric != reading.metric or reading.kind not in rule.kinds:
                continue
            st = self._state.setdefault((rule.id, reading.source_id), _EpisodeState())
            if st.open:
                if rule.cleared(reading.value):
                    st.open, st.breach_started = False, None
                    events.append(self._event(rule, reading, "resolved"))
                continue
            if not rule.breached(reading.value):
                st.breach_started = None
                continue
            st.breach_started = st.breach_started or reading.ts
            if (reading.ts - st.breach_started).total_seconds() >= rule.sustain_seconds:
                st.open = True
                events.append(self._event(rule, reading, "raised"))
        return events

    def check_stale(self, now: datetime) -> list[AlertEvent]:
        events = []
        for source_id, (site, kind, last) in self._last_seen.items():
            if source_id not in self._stale_open and now - last > self.stale_after:
                self._stale_open.add(source_id)
                events.append(self._stale_event(site, source_id, kind, "raised", now))
        return events

    def _event(self, rule: ThresholdRule, r: Reading, transition: Literal["raised", "resolved"]) -> AlertEvent:
        verb = "exceeded" if rule.op == ">" else "fell below"
        message = (
            f"{r.metric} {verb} {rule.trigger} for {rule.sustain_seconds:g}s (value {r.value:g})"
            if transition == "raised"
            else f"{r.metric} back to normal (value {r.value:g})"
        )
        return AlertEvent(
            rule_id=rule.id,
            site=r.site,
            source_id=r.source_id,
            metric=r.metric,
            severity=rule.severity,
            transition=transition,
            value=r.value,
            message=message,
            at=r.ts,
        )

    def _stale_event(
        self,
        site: str,
        source_id: str,
        kind: SourceKind,
        transition: Literal["raised", "resolved"],
        at: datetime,
    ) -> AlertEvent:
        severity = Severity.WARNING if kind is SourceKind.ASSET else Severity.CRITICAL
        message = (
            f"no data for more than {self.stale_after.total_seconds():g}s"
            if transition == "raised"
            else "data received again"
        )
        return AlertEvent(
            rule_id="source-stale",
            site=site,
            source_id=source_id,
            metric="heartbeat",
            severity=severity,
            transition=transition,
            value=None,
            message=message,
            at=at,
        )


def load_rules(path: str | Path) -> list[ThresholdRule]:
    """Load rules from a JSON file (see config/rules.json)."""
    data = json.loads(Path(path).read_text())
    return list(parse_rules(data))


def parse_rules(data: Iterable[dict[str, object]]) -> Iterable[ThresholdRule]:
    for r in data:
        kinds = r.get("kinds")
        yield ThresholdRule(
            id=str(r["id"]),
            metric=str(r["metric"]),
            op=">" if r["op"] == ">" else "<",
            trigger=float(r["trigger"]),  # type: ignore[arg-type]
            reset=float(r["reset"]),  # type: ignore[arg-type]
            sustain_seconds=float(r.get("sustain_seconds", 0)),  # type: ignore[arg-type]
            severity=Severity(str(r.get("severity", "warning"))),
            kinds=frozenset(SourceKind(k) for k in kinds) if isinstance(kinds, list) else frozenset(SourceKind),
        )
