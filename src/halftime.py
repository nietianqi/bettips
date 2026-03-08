"""Halftime confirmation engine."""

from __future__ import annotations

from loguru import logger

from src import alert, storage


def check_candidate(db_path: str, match: dict, alert_config: dict | None = None) -> bool:
    """Check one candidate match at halftime."""
    match_id = str(match["id"])
    status = str(match.get("status", "")).lower()

    if status in {"finished", "ft"}:
        storage.update_candidate_status(db_path, match_id, "dismissed")
        return False
    if status != "halftime":
        return False

    ht_home = match.get("ht_home")
    ht_away = match.get("ht_away")
    if ht_home is None or ht_away is None:
        logger.debug(f"[{match_id}] halftime score missing")
        return False

    if int(ht_home) != 0 or int(ht_away) != 0:
        logger.info(f"[{match_id}] halftime {ht_home}:{ht_away}, remove candidate")
        storage.update_candidate_status(db_path, match_id, "dismissed")
        return False

    events = storage.get_events(db_path, match_id)
    first_half_red_cards = [
        event
        for event in events
        if event.get("event_type") == "red_card" and int(event.get("minute") or 0) <= 55
    ]
    if first_half_red_cards:
        logger.info(f"[{match_id}] first-half red card, remove candidate")
        storage.update_candidate_status(db_path, match_id, "dismissed")
        return False

    alert.send_ht_alert(match, alert_config)
    storage.update_candidate_status(db_path, match_id, "alerted")
    return True


def run_halftime_check(db_path: str, alert_config: dict | None = None) -> list[str]:
    """Check all watching candidates."""
    candidates = storage.get_live_candidates(db_path)
    alerted: list[str] = []

    for match in candidates:
        if check_candidate(db_path, match, alert_config):
            alerted.append(str(match["id"]))

    return alerted
