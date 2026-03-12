"""Demo: call Titan007 mobile endpoints and evaluate first late upgrade."""

from __future__ import annotations

from datetime import datetime, timezone

from src.collectors.titan_http import (
    TitanHttpClient,
    detect_first_late_upgrade,
    is_deep_main_line,
    normalize_handicap_history,
    parse_goal3_matches,
)


def main() -> None:
    scheid = 2950976
    oddsid = 29894155
    kickoff = datetime(2026, 3, 13, 1, 45, tzinfo=timezone.utc)  # replace with real kickoff

    client = TitanHttpClient()
    try:
        # 1) Handicap history
        raw_history = client.fetch_handicap_history(scheid=scheid, oddsid=oddsid)
        history = normalize_handicap_history(raw_history, fill_missing_draw_odds=False)

        # Deep-line check by latest valid draw_odds.
        latest_line = next((row["draw_odds"] for row in reversed(history) if row["draw_odds"] is not None), None)
        deep_ok = is_deep_main_line(latest_line, min_depth=1.0)

        # First-time late upgrade check.
        signal_ok, signal = detect_first_late_upgrade(history, kickoff_ts=kickoff, window_minutes=15)
        print("latest_line:", latest_line, "deep_ok:", deep_ok)
        print("late_upgrade:", signal_ok, signal)

        # 2) goal3 snapshot
        goal3_xml = client.fetch_goal3_xml()
        goal3 = parse_goal3_matches(goal3_xml)
        print("goal3 rows:", len(goal3["rows"]), "ids:", len(goal3["ids"]), "jc_ids:", len(goal3["jc_ids"]))

    finally:
        client.close()


if __name__ == "__main__":
    main()
