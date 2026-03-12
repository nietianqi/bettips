"""Tests for CSV export and de-duplication."""

from __future__ import annotations

from pathlib import Path

from src.csv_export import write_match_signals


def test_write_and_dedupe_by_stage_and_match_id(tmp_path: Path):
    path = tmp_path / "signals.csv"
    cfg = {"enabled": True, "csv_path": str(path), "dedupe_keys": ["stage", "match_id"]}

    match = {
        "id": "2950976",
        "league": "欧罗巴杯",
        "home_team": "主队",
        "away_team": "客队",
        "kickoff_time": "2026-03-13 01:45:00",
    }

    first = write_match_signals([match], "pre_match_candidate", cfg)
    second = write_match_signals([match], "pre_match_candidate", cfg)
    third = write_match_signals([match], "ht_alert", cfg)

    assert first == 1
    assert second == 0
    assert third == 1

    text = path.read_text(encoding="utf-8-sig")
    assert "https://m.titan007.com/asian/2950976.htm" in text
    # header + 2 rows
    assert len([line for line in text.splitlines() if line.strip()]) == 3
