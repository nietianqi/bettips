"""Tests for titan mobile HTTP parsers and strategy helpers."""

from datetime import datetime, timezone

from src.collectors.titan_http import (
    detect_first_late_upgrade,
    is_deep_main_line,
    normalize_handicap_history,
    parse_goal3_matches,
    parse_schedule_matches,
    resolve_bet365_oddsid,
    resolve_bet365_oddsids,
)


def test_parse_goal3_matches_core_fields():
    xml_text = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<c><match><m>2950976,16864815,-0.25,1.08,0.81,150938197,3.7,3.35,2.11,19250134,2.25,0.87,1.01,1,0,0,,,,,-0.25,2.25</m></match>"
        "<ids>2950976,2950977</ids><jcIds>2950976</jcIds><isMaintain>0</isMaintain></c>"
    )
    parsed = parse_goal3_matches(xml_text)
    assert parsed["is_maintain"] == 0
    assert parsed["ids"] == [2950976, 2950977]
    assert parsed["jc_ids"] == [2950976]
    assert len(parsed["rows"]) == 1
    row = parsed["rows"][0]
    assert row["scheid"] == 2950976
    assert row["letgoal"] == -0.25
    assert row["ou_line"] == 2.25
    assert row["first_letgoal"] == -0.25
    assert row["first_ou_line"] == 2.25


def test_normalize_handicap_history_keep_missing_draw_odds():
    payload = {
        "oddsId": 29894155,
        "details": [
            {"homeOdds": 1.5, "awayOdds": 0.5, "modifyTime": "1773292882", "kind": "REAL"},
            {"homeOdds": 0.52, "drawOdds": -0.75, "awayOdds": 1.42, "modifyTime": "1773289689", "kind": "REAL"},
        ],
    }
    rows = normalize_handicap_history(payload, fill_missing_draw_odds=False)
    assert len(rows) == 2
    assert rows[0]["modify_ts"] == 1773289689
    assert rows[0]["draw_odds"] == -0.75
    assert rows[1]["draw_odds"] is None


def test_normalize_handicap_history_forward_fill_draw_odds():
    payload = {
        "oddsId": 29894155,
        "details": [
            {"homeOdds": 0.52, "drawOdds": -0.75, "awayOdds": 1.42, "modifyTime": "1773289689", "kind": "REAL"},
            {"homeOdds": 1.5, "awayOdds": 0.5, "modifyTime": "1773292882", "kind": "REAL"},
        ],
    }
    rows = normalize_handicap_history(payload, fill_missing_draw_odds=True)
    assert rows[1]["draw_odds"] == -0.75


def test_detect_first_late_upgrade_true():
    kickoff = int(datetime(2026, 3, 12, 12, 0, tzinfo=timezone.utc).timestamp())
    history = [
        {"modify_ts": kickoff - 3600, "draw_odds": -0.75},
        {"modify_ts": kickoff - 600, "draw_odds": -1.0},
    ]
    ok, hit = detect_first_late_upgrade(history, kickoff, window_minutes=15)
    assert ok is True
    assert hit is not None
    assert hit["curr_draw_odds"] == -1.0


def test_detect_first_late_upgrade_false_when_depth_seen_before():
    kickoff = int(datetime(2026, 3, 12, 12, 0, tzinfo=timezone.utc).timestamp())
    history = [
        {"modify_ts": kickoff - 7200, "draw_odds": -1.0},
        {"modify_ts": kickoff - 3600, "draw_odds": -0.75},
        {"modify_ts": kickoff - 600, "draw_odds": -1.0},
    ]
    ok, hit = detect_first_late_upgrade(history, kickoff, window_minutes=15)
    assert ok is False
    assert hit is None


def test_is_deep_main_line():
    assert is_deep_main_line(-1.0) is True
    assert is_deep_main_line(-1.25) is True
    assert is_deep_main_line(-0.75) is False


def test_parse_schedule_matches_basic():
    sample = (
        "联赛A^113^1^^^^^^!$$"
        "2950976^113^0^20260313014500^^主队^客队^0^0^0^0^1^0!"
    )
    rows = parse_schedule_matches(sample)
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == "2950976"
    assert row["league"] == "联赛A"
    assert row["status"] == "scheduled"
    assert row["home_red"] == 1
    assert row["away_red"] == 0


def test_resolve_bet365_oddsid():
    payload = {
        "companies": [
            {"nameCn": "澳门", "details": [{"oddsId": 111}]},
            {"nameCn": "Bet365", "details": [{"num": 1, "oddsId": 29894153}, {"num": 4, "oddsId": 29894155}]},
        ]
    }
    assert resolve_bet365_oddsid(payload) == 29894155


def test_resolve_bet365_oddsids_all_lines():
    payload = {
        "companies": [
            {
                "companyId": 8,
                "nameCn": "36*",
                "details": [{"num": 1, "oddsId": 123}, {"num": 4, "oddsId": 456}, {"num": 5, "oddsId": 789}],
            }
        ]
    }
    assert resolve_bet365_oddsids(payload) == {1: 123, 4: 456, 5: 789}


def test_resolve_bet365_masked_company_name():
    payload = {
        "companies": [
            {
                "companyId": 8,
                "nameCn": "36*",
                "details": [{"num": 1, "oddsId": 123}, {"num": 4, "oddsId": 456}],
            }
        ]
    }
    assert resolve_bet365_oddsid(payload, prefer_num=4) == 456
