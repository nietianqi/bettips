"""Quick check for one Titan007 match by scheid.

Usage:
  python examples/check_titan_match.py 2816005
"""

from __future__ import annotations

import argparse
import sys
from datetime import timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.collectors.titan_http import (
    TitanHttpClient,
    detect_first_late_upgrade,
    is_deep_main_line,
    normalize_handicap_history,
    parse_schedule_matches,
    resolve_bet365_oddsid,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("scheid", type=int)
    parser.add_argument("--window", type=int, default=15, help="late-upgrade window minutes")
    parser.add_argument("--min-depth", type=float, default=1.0, help="deep line threshold")
    parser.add_argument("--prefer-num", type=int, default=4, help="preferred bet365 line num")
    args = parser.parse_args()

    client = TitanHttpClient()
    try:
        scheid = args.scheid

        # 1) schedule row
        schedule_text = client.fetch_schedule_text(score_type=0, language=0)
        matches = parse_schedule_matches(schedule_text)
        match = next((m for m in matches if int(m["id"]) == scheid), None)
        if not match:
            print(f"[WARN] scheid={scheid} not found in current Schedule_0_0 feed")
            return

        # 2) resolve bet365 oddsid
        companies = client.fetch_handicap_companies(scheid=scheid, oddskind=0, is_half=0, type_=1)
        oddsid = resolve_bet365_oddsid(companies, prefer_num=args.prefer_num)
        if oddsid is None:
            print(f"[WARN] scheid={scheid} cannot resolve bet365 oddsid")
            return

        # 3) fetch and normalize handicap history
        raw = client.fetch_handicap_history(scheid=scheid, oddsid=oddsid, type_=2, oddskind=0, is_half=0)
        history = normalize_handicap_history(raw, fill_missing_draw_odds=False)
        valid = [r for r in history if r.get("draw_odds") is not None and r.get("modify_ts") is not None]
        if not valid:
            print(f"[WARN] scheid={scheid} no valid drawOdds history")
            return

        latest = valid[-1]
        deep_ok = is_deep_main_line(latest["draw_odds"], min_depth=args.min_depth)

        # kickoff_time stored as UTC string in parser output
        from datetime import datetime
        kickoff_dt = datetime.fromisoformat(match["kickoff_time"]).replace(tzinfo=timezone.utc)
        kickoff_ts = int(kickoff_dt.timestamp())

        upgrade_ok, upgrade = detect_first_late_upgrade(valid, kickoff_ts=kickoff_ts, window_minutes=args.window)

        print("=" * 60)
        print(f"scheid: {scheid}")
        print(f"match: {match['home_team']} vs {match['away_team']} | {match['league']}")
        print(f"kickoff_utc: {match['kickoff_time']} | status: {match['status']}")
        print(f"bet365_oddsid: {oddsid} | history_rows(valid): {len(valid)}")
        print(f"latest_drawOdds: {latest['draw_odds']} | deep_ok(<=-{args.min_depth}): {deep_ok}")
        print(f"late_upgrade_ok({args.window}m): {upgrade_ok}")
        if upgrade:
            print(
                "upgrade_detail:",
                f"{upgrade['prev_draw_odds']} -> {upgrade['curr_draw_odds']} at {upgrade['modify_dt_utc']}",
            )
        print("=" * 60)
    finally:
        client.close()


if __name__ == "__main__":
    main()
