"""Pre-match scanning rules."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from loguru import logger

from src import storage
from src.timeutils import parse_datetime


def _clean_history(history: list[dict], kickoff: datetime) -> list[dict]:
    cleaned: list[dict] = []
    for row in history:
        ts = parse_datetime(row.get("ts"))
        if ts is None:
            continue
        if ts > kickoff:
            continue
        try:
            depth = float(row["line_depth"])
        except (KeyError, TypeError, ValueError):
            continue
        cleaned.append(
            {
                **row,
                "ts": ts,
                "line_depth": depth,
                "home_gives": bool(row.get("home_gives", 0)),
            }
        )
    cleaned.sort(key=lambda x: x["ts"])
    return cleaned


def has_first_time_late_upgrade(
    history: list[dict],
    kickoff: datetime,
    window_minutes: int = 15,
) -> tuple[bool, Optional[dict]]:
    """Detect first-time depth upgrade inside late window."""
    kickoff_dt = parse_datetime(kickoff)
    if kickoff_dt is None:
        return False, None

    cleaned = _clean_history(history, kickoff_dt)
    if len(cleaned) < 2:
        return False, None

    late_start = kickoff_dt - timedelta(minutes=window_minutes)

    for i in range(1, len(cleaned)):
        prev = cleaned[i - 1]
        curr = cleaned[i]
        curr_ts = curr["ts"]
        if not (late_start <= curr_ts <= kickoff_dt):
            continue

        prev_depth = float(prev["line_depth"])
        curr_depth = float(curr["line_depth"])
        if curr_depth <= prev_depth:
            continue

        appeared_before = any(
            abs(float(h["line_depth"]) - curr_depth) < 1e-9 and h["ts"] < curr_ts
            for h in cleaned
        )
        if appeared_before:
            continue

        record = {**curr, "prev_depth": prev_depth}
        logger.debug(f"First-time late upgrade: {prev_depth} -> {curr_depth} @ {curr_ts}")
        return True, record

    return False, None


def scan_match(
    db_path: str,
    match: dict,
    bookmaker: str = "bet365_pan4",
    main_bookmaker: str = "bet365_main",
    min_depth: float = 1.0,
    window_minutes: int = 15,
) -> Optional[dict]:
    """Run pre-match scan on one match."""
    match_id = str(match["id"])
    kickoff = parse_datetime(match.get("kickoff_time"))
    if kickoff is None:
        logger.debug(f"[{match_id}] invalid kickoff_time, skip")
        return None

    if storage.is_candidate(db_path, match_id):
        return None

    signal_history = storage.get_odds_history(db_path, match_id, bookmaker)
    cleaned_signal = _clean_history(signal_history, kickoff)
    if not cleaned_signal and bookmaker == "bet365_pan4":
        # Backward compatibility for historical data written as "bet365".
        signal_history = storage.get_odds_history(db_path, match_id, "bet365")
        cleaned_signal = _clean_history(signal_history, kickoff)
    if not cleaned_signal:
        logger.debug(f"[{match_id}] no valid signal history ({bookmaker}), skip")
        return None

    main_history = storage.get_odds_history(db_path, match_id, main_bookmaker)
    cleaned_main = _clean_history(main_history, kickoff)
    if not cleaned_main and main_bookmaker == "bet365_main":
        # Fallback to signal line when dedicated main line is unavailable.
        cleaned_main = cleaned_signal
    if not cleaned_main:
        logger.debug(f"[{match_id}] no valid main history ({main_bookmaker}), skip")
        return None

    main_depth = float(cleaned_main[-1]["line_depth"])
    if main_depth < float(min_depth):
        logger.debug(f"[{match_id}] main depth {main_depth} < {min_depth}, skip")
        return None

    triggered, upgrade_record = has_first_time_late_upgrade(cleaned_signal, kickoff, window_minutes)
    if not triggered or upgrade_record is None:
        logger.debug(f"[{match_id}] no first-time late upgrade on {bookmaker}")
        return None

    side = "home" if bool(upgrade_record.get("home_gives", 1)) else "away"
    trigger_depth = float(upgrade_record["line_depth"])
    prev_depth = float(upgrade_record["prev_depth"])
    upgrade_ts = upgrade_record["ts"].isoformat(sep=" ", timespec="seconds")

    home = match.get("home_team", "?")
    away = match.get("away_team", "?")
    league = match.get("league", "?")
    logger.info(
        f"[Pre-match candidate] {league} | {home} vs {away} | "
        f"main>={min_depth} ok | {bookmaker} {side}-gives {prev_depth}->{trigger_depth} @ {upgrade_ts}"
    )

    return {
        "match_id": match_id,
        "trigger_depth": trigger_depth,
        "prev_depth": prev_depth,
        "upgrade_ts": upgrade_ts,
    }


def run_pre_match_scan(
    db_path: str,
    bookmaker: str = "bet365_pan4",
    main_bookmaker: str = "bet365_main",
    min_depth: float = 1.0,
    window_minutes: int = 15,
    scan_window: int = 90,
) -> list[dict]:
    """Scan upcoming matches and persist new candidates."""
    upcoming = storage.get_upcoming_matches(db_path, within_minutes=scan_window)
    new_candidates: list[dict] = []

    for match in upcoming:
        result = scan_match(
            db_path=db_path,
            match=match,
            bookmaker=bookmaker,
            main_bookmaker=main_bookmaker,
            min_depth=min_depth,
            window_minutes=window_minutes,
        )
        if result:
            storage.add_candidate(db_path, result)
            new_candidates.append(result)

    if new_candidates:
        logger.info(f"New candidates this round: {len(new_candidates)}")
    return new_candidates
