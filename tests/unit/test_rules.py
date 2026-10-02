from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from monitoring.models import Reading, Severity, SourceKind
from monitoring.processing.rules import RuleEngine, ThresholdRule, load_rules

T0 = datetime(2026, 10, 1, 10, 0, tzinfo=UTC)


def reading(seconds: float, value: float, metric="vibration_mm_s", source="PUMP-101", kind=SourceKind.ASSET):
    return Reading(
        site="S1", source_id=source, kind=kind, metric=metric, value=value, ts=T0 + timedelta(seconds=seconds)
    )


VIB = ThresholdRule("vib", "vibration_mm_s", ">", 7.1, 4.5, 30, Severity.WARNING, frozenset({SourceKind.ASSET}))
DISK = ThresholdRule("disk", "disk_free_pct", "<", 10, 15, 0, Severity.CRITICAL, frozenset({SourceKind.HOST}))


def transitions(engine: RuleEngine, *readings: Reading) -> list[tuple[str, str]]:
    out = []
    for r in readings:
        out += [(e.rule_id, e.transition) for e in engine.evaluate(r)]
    return out


def test_threshold_requires_sustained_breach_and_resolves_below_reset():
    e = RuleEngine([VIB], timedelta(minutes=5))
    assert transitions(e, reading(0, 8), reading(20, 6), reading(25, 8), reading(50, 8.5)) == []
    assert transitions(e, reading(56, 9)) == [("vib", "raised")]
    assert transitions(e, reading(60, 9.5), reading(70, 5)) == []  # same episode; inside hysteresis band
    assert transitions(e, reading(80, 4)) == [("vib", "resolved")]


def test_below_rules_and_kind_filtering():
    e = RuleEngine([VIB, DISK], timedelta(minutes=5))
    host = SourceKind.HOST
    assert transitions(e, reading(0, 8, source="h1", kind=host)) == []  # vibration rule is asset-only
    assert transitions(e, reading(0, 9, metric="disk_free_pct", source="h1", kind=host)) == [("disk", "raised")]
    assert transitions(e, reading(10, 12, metric="disk_free_pct", source="h1", kind=host)) == []
    assert transitions(e, reading(20, 16, metric="disk_free_pct", source="h1", kind=host)) == [("disk", "resolved")]


def test_episodes_are_tracked_per_source():
    e = RuleEngine([DISK], timedelta(minutes=5))
    h = SourceKind.HOST
    assert transitions(e, reading(0, 5, "disk_free_pct", "h1", h), reading(0, 5, "disk_free_pct", "h2", h)) == [
        ("disk", "raised"),
        ("disk", "raised"),
    ]


def test_staleness_raises_once_and_resolves_when_data_returns():
    e = RuleEngine([], timedelta(seconds=60))
    e.evaluate(reading(0, 1, metric="up", source="api", kind=SourceKind.SERVICE))
    assert e.check_stale(T0 + timedelta(seconds=30)) == []
    stale = e.check_stale(T0 + timedelta(seconds=61))
    assert [(x.rule_id, x.transition, x.severity) for x in stale] == [("source-stale", "raised", Severity.CRITICAL)]
    assert e.check_stale(T0 + timedelta(seconds=120)) == []
    back = e.evaluate(reading(130, 1, metric="up", source="api", kind=SourceKind.SERVICE))
    assert [(x.rule_id, x.transition) for x in back] == [("source-stale", "resolved")]


def test_out_of_order_readings_do_not_move_last_seen_backwards():
    e = RuleEngine([], timedelta(seconds=60))
    e.evaluate(reading(100, 1))
    e.evaluate(reading(10, 1))
    assert e.check_stale(T0 + timedelta(seconds=150)) == []


def test_invalid_hysteresis_is_rejected():
    with pytest.raises(ValueError):
        ThresholdRule("x", "m", ">", 5, 6, 0, Severity.INFO)
    with pytest.raises(ValueError):
        ThresholdRule("x", "m", "<", 5, 4, 0, Severity.INFO)


def test_shipped_rule_file_loads():
    rules = load_rules(Path(__file__).resolve().parents[2] / "config" / "rules.json")
    assert {r.id for r in rules} >= {"asset-vibration-high", "service-down", "host-disk-free-low"}
