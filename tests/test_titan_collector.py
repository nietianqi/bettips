"""Tests for titan collector behaviors."""

from __future__ import annotations

import time

import pytest

from src.collectors.titan import TitanCollector


@pytest.mark.asyncio
async def test_fetch_odds_history_refreshes_stale_cached_lines(monkeypatch):
    collector = TitanCollector({})
    match_id = "123456"

    # Simulate stale cache that only knows line 1.
    collector._oddsid_cache[match_id] = ({1: 101}, time.time() + 3600)

    calls = {"companies": 0, "history": []}

    def fake_fetch_handicap_companies(scheid, oddskind, is_half, type_, cookie):  # noqa: ANN001
        calls["companies"] += 1
        return {
            "companies": [
                {
                    "companyId": 8,
                    "nameCn": "36*",
                    "details": [{"num": 1, "oddsId": 101}, {"num": 4, "oddsId": 404}],
                }
            ]
        }

    def fake_fetch_handicap_history(scheid, oddsid, type_, oddskind, is_half, flesh, cookie):  # noqa: ANN001
        calls["history"].append(int(oddsid))
        if int(oddsid) == 101:
            return {
                "oddsId": 101,
                "details": [
                    {
                        "homeOdds": 0.90,
                        "drawOdds": -1.00,
                        "awayOdds": 0.90,
                        "modifyTime": "1773400000",
                        "kind": "REAL",
                    }
                ],
            }
        if int(oddsid) == 404:
            return {
                "oddsId": 404,
                "details": [
                    {
                        "homeOdds": 0.88,
                        "drawOdds": -1.25,
                        "awayOdds": 0.96,
                        "modifyTime": "1773400060",
                        "kind": "REAL",
                    }
                ],
            }
        return {"oddsId": oddsid, "details": []}

    monkeypatch.setattr(collector.client, "fetch_handicap_companies", fake_fetch_handicap_companies)
    monkeypatch.setattr(collector.client, "fetch_handicap_history", fake_fetch_handicap_history)

    rows = await collector.fetch_odds_history(match_id)
    await collector.close()

    assert calls["companies"] == 1
    assert set(calls["history"]) == {101, 404}
    bookmakers = {row["bookmaker"] for row in rows}
    assert "bet365_main" in bookmakers
    assert "bet365_pan4" in bookmakers
    assert "bet365" in bookmakers

