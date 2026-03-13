"""Tests for dual CSV output in pre-match scan."""

from __future__ import annotations

import main as app_main
import pytest


async def _run_with_config(monkeypatch, output_cfg: dict) -> list[tuple[str, dict]]:
    calls: list[tuple[str, dict]] = []

    monkeypatch.setattr(app_main, "_db_path", "bettips.db")
    monkeypatch.setattr(app_main, "_config", {"scanner": {}, "output": output_cfg})
    monkeypatch.setattr(
        app_main.scanner,
        "run_pre_match_scan",
        lambda **_kwargs: [
            {
                "match_id": "1001",
                "trigger_depth": 1.25,
                "prev_depth": 1.0,
                "upgrade_ts": "2026-03-13 01:30:00",
            }
        ],
    )
    monkeypatch.setattr(
        app_main.storage,
        "get_match",
        lambda _db, _match_id: {
            "id": "1001",
            "league": "Test League",
            "home_team": "Home",
            "away_team": "Away",
            "kickoff_time": "2026-03-13 01:45:00",
            "status": "scheduled",
        },
    )

    def _fake_write(_rows, stage, cfg):  # noqa: ANN001
        calls.append((stage, dict(cfg)))
        return 1

    monkeypatch.setattr(app_main.csv_export, "write_match_signals", _fake_write)
    await app_main.run_pre_match_scan()
    return calls


@pytest.mark.asyncio
async def test_run_pre_match_scan_writes_dual_csv(monkeypatch):
    calls = await _run_with_config(
        monkeypatch,
        {
            "enabled": True,
            "csv_path": "outputs/match_signals.csv",
            "dedupe_keys": ["stage", "match_id"],
            "pre_match_csv_enabled": True,
            "pre_match_csv_path": "outputs/pre_match_candidates.csv",
            "pre_match_dedupe_keys": ["match_id"],
        },
    )

    assert len(calls) == 2
    assert calls[0][0] == "pre_match_candidate"
    assert calls[0][1]["csv_path"] == "outputs/match_signals.csv"
    assert calls[1][0] == "pre_match_candidate"
    assert calls[1][1]["csv_path"] == "outputs/pre_match_candidates.csv"
    assert calls[1][1]["dedupe_keys"] == ["match_id"]


@pytest.mark.asyncio
async def test_run_pre_match_scan_single_csv_when_secondary_disabled(monkeypatch):
    calls = await _run_with_config(
        monkeypatch,
        {
            "enabled": True,
            "csv_path": "outputs/match_signals.csv",
            "dedupe_keys": ["stage", "match_id"],
            "pre_match_csv_enabled": False,
            "pre_match_csv_path": "outputs/pre_match_candidates.csv",
            "pre_match_dedupe_keys": ["match_id"],
        },
    )

    assert len(calls) == 1
    assert calls[0][1]["csv_path"] == "outputs/match_signals.csv"
