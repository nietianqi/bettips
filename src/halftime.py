"""Halftime confirmation engine."""

from __future__ import annotations

from loguru import logger

from src import alert, storage


def check_candidate(db_path: str, match: dict, alert_config: dict | None = None) -> bool:
    """Check one candidate match at halftime."""
    match_id = str(match["id"])
    status = str(match.get("status", "")).lower()
    cfg = alert_config or {}
    fallback_over1_enabled = bool(cfg.get("fallback_over1_enabled", True))

    if status in {"finished", "ft"}:
        storage.update_candidate_status(db_path, match_id, "dismissed")
        return False
    if status != "halftime":
        return False

    if not fallback_over1_enabled:
        logger.debug(f"[{match_id}] fallback_over1 disabled, skip")
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
