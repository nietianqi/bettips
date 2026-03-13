"""Unit tests for first-time late upgrade logic."""

from datetime import datetime, timedelta
from pathlib import Path

from src import storage
from src.scanner import has_first_time_late_upgrade, scan_match


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


def _setup_match(db_path: Path, match_id: str) -> dict:
    storage.init_db(str(db_path))
    match = {
        "id": match_id,
        "league": "Test League",
        "home_team": "Home",
        "away_team": "Away",
        "kickoff_time": "2026-03-08 20:00:00",
        "status": "scheduled",
    }
    storage.upsert_match(str(db_path), match)
    return match


def test_scan_match_accepts_away_gives_when_rules_match(tmp_path: Path):
    db_path = tmp_path / "scan.db"
    match = _setup_match(db_path, "1001")

    storage.insert_odds(
        str(db_path),
        {
            "match_id": "1001",
            "bookmaker": "bet365_main",
            "line_depth": 1.0,
            "home_gives": 0,
            "ts": "2026-03-08 19:30:00",
        },
    )
    storage.insert_odds(
        str(db_path),
        {
            "match_id": "1001",
            "bookmaker": "bet365_pan4",
            "line_depth": 0.75,
            "home_gives": 0,
            "ts": "2026-03-08 19:20:00",
        },
    )
    storage.insert_odds(
        str(db_path),
        {
            "match_id": "1001",
            "bookmaker": "bet365_pan4",
            "line_depth": 1.25,
            "home_gives": 0,
            "ts": "2026-03-08 19:50:00",
        },
    )

    result = scan_match(str(db_path), match, min_depth=1.0, window_minutes=15)
    assert result is not None
    assert result["trigger_depth"] == 1.25
    assert result["prev_depth"] == 0.75
    assert result["upgrade_ts"] == "2026-03-08 19:50:00"


def test_scan_match_requires_deep_main_line(tmp_path: Path):
    db_path = tmp_path / "scan2.db"
    match = _setup_match(db_path, "1002")

    storage.insert_odds(
        str(db_path),
        {
            "match_id": "1002",
            "bookmaker": "bet365_main",
            "line_depth": 0.75,
            "home_gives": 1,
            "ts": "2026-03-08 19:20:00",
        },
    )
    storage.insert_odds(
        str(db_path),
        {
            "match_id": "1002",
            "bookmaker": "bet365_pan4",
            "line_depth": 0.5,
            "home_gives": 1,
            "ts": "2026-03-08 19:20:00",
        },
    )
    storage.insert_odds(
        str(db_path),
        {
            "match_id": "1002",
            "bookmaker": "bet365_pan4",
            "line_depth": 0.75,
            "home_gives": 1,
            "ts": "2026-03-08 19:50:00",
        },
    )

    result = scan_match(str(db_path), match, min_depth=1.0, window_minutes=15)
    assert result is None


def test_scan_match_requires_first_time_upgrade(tmp_path: Path):
    db_path = tmp_path / "scan3.db"
    match = _setup_match(db_path, "1003")

    storage.insert_odds(
        str(db_path),
        {
            "match_id": "1003",
            "bookmaker": "bet365_main",
            "line_depth": 1.25,
            "home_gives": 1,
            "ts": "2026-03-08 19:10:00",
        },
    )
    storage.insert_odds(
        str(db_path),
        {
            "match_id": "1003",
            "bookmaker": "bet365_pan4",
            "line_depth": 1.0,
            "home_gives": 1,
            "ts": "2026-03-08 18:00:00",
        },
    )
    storage.insert_odds(
        str(db_path),
        {
            "match_id": "1003",
            "bookmaker": "bet365_pan4",
            "line_depth": 0.75,
            "home_gives": 1,
            "ts": "2026-03-08 19:40:00",
        },
    )
    storage.insert_odds(
        str(db_path),
        {
            "match_id": "1003",
            "bookmaker": "bet365_pan4",
            "line_depth": 1.0,
            "home_gives": 1,
            "ts": "2026-03-08 19:55:00",
        },
    )

    result = scan_match(str(db_path), match, min_depth=1.0, window_minutes=15)
    assert result is None
