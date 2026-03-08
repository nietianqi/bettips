"""Unit tests for first-time late upgrade logic."""

from datetime import datetime, timedelta

from src.scanner import has_first_time_late_upgrade


KICKOFF = datetime(2026, 3, 8, 20, 0, 0)


def _rec(depth: float, minutes_before_kickoff: int) -> dict:
    return {"line_depth": depth, "ts": KICKOFF - timedelta(minutes=minutes_before_kickoff)}


def test_first_time_upgrade_inside_15m():
    history = [_rec(1.0, 180), _rec(1.0, 40), _rec(1.25, 10)]
    triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
    assert triggered is True
    assert rec is not None
    assert rec["line_depth"] == 1.25
    assert rec["prev_depth"] == 1.0


def test_upgrade_not_first_time():
    history = [_rec(1.0, 200), _rec(1.25, 120), _rec(1.0, 20), _rec(1.25, 10)]
    triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
    assert triggered is False
    assert rec is None


def test_upgrade_outside_window():
    history = [_rec(1.0, 180), _rec(1.25, 25)]
    triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
    assert triggered is False
    assert rec is None


def test_support_iso_string_ts():
    history = [
        {"line_depth": 1.0, "ts": "2026-03-08 19:30:00"},
        {"line_depth": 1.25, "ts": "2026-03-08T19:50:00Z"},
    ]
    triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
    assert triggered is True
    assert rec is not None
    assert rec["prev_depth"] == 1.0
