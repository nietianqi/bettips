"""Tests for halftime fallback behavior."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from src import halftime, storage


def _candidate_status(db_path: Path, match_id: str) -> str | None:
    con = sqlite3.connect(str(db_path))
    try:
        row = con.execute("SELECT status FROM candidates WHERE match_id=?", (match_id,)).fetchone()
        return row[0] if row else None
    finally:
        con.close()


def _setup_candidate(db_path: Path, match_id: str = "2001") -> None:
    storage.init_db(str(db_path))
    storage.upsert_match(
        str(db_path),
        {
            "id": match_id,
            "league": "Test League",
            "home_team": "Home",
            "away_team": "Away",
            "kickoff_time": "2026-03-08 20:00:00",
            "status": "halftime",
            "ht_home": 1,
            "ht_away": 1,
        },
    )
    storage.add_candidate(
        str(db_path),
        {
            "match_id": match_id,
            "trigger_depth": 1.25,
            "prev_depth": 1.0,
            "upgrade_ts": "2026-03-08 19:50:00",
        },
    )
    storage.insert_event(
        str(db_path),
        {
            "match_id": match_id,
            "minute": 10,
            "event_type": "red_card",
            "team": "home",
            "ts": "2026-03-08 19:10:00",
        },
    )


def test_halftime_alert_ignores_score_and_red_card(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "halftime.db"
    _setup_candidate(db_path, "2001")

    called = {"count": 0}

    def fake_send_ht_alert(_match, _cfg):  # noqa: ANN001
        called["count"] += 1

    monkeypatch.setattr(halftime.alert, "send_ht_alert", fake_send_ht_alert)

    alerted = halftime.run_halftime_check(str(db_path), {"fallback_over1_enabled": True})
    assert alerted == ["2001"]
    assert called["count"] == 1
    assert _candidate_status(db_path, "2001") == "alerted"


def test_halftime_alert_can_be_disabled(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "halftime_disabled.db"
    _setup_candidate(db_path, "2002")

    called = {"count": 0}

    def fake_send_ht_alert(_match, _cfg):  # noqa: ANN001
        called["count"] += 1

    monkeypatch.setattr(halftime.alert, "send_ht_alert", fake_send_ht_alert)

    alerted = halftime.run_halftime_check(str(db_path), {"fallback_over1_enabled": False})
    assert alerted == []
    assert called["count"] == 0
    assert _candidate_status(db_path, "2002") == "watching"
