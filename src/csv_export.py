"""CSV signal export with built-in de-duplication."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from loguru import logger


DEFAULT_HEADERS = [
    "detected_at_utc",
    "stage",
    "match_id",
    "league",
    "home_team",
    "away_team",
    "kickoff_time_utc",
    "status",
    "ht_score",
    "trigger_depth",
    "prev_depth",
    "upgrade_ts",
    "titan_asian_url",
    "titan_live_url",
]


def _read_existing_keys(path: Path, dedupe_keys: list[str]) -> set[tuple[str, ...]]:
    if not path.exists():
        return set()

    keys: set[tuple[str, ...]] = set()
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = tuple(str(row.get(k, "")).strip() for k in dedupe_keys)
                keys.add(key)
    except Exception as exc:
        logger.warning(f"Failed to read existing CSV for dedupe: {exc}")
    return keys


def _as_row(match: dict, stage: str) -> dict:
    match_id = str(match.get("id") or match.get("match_id") or "")
    ht_home = match.get("ht_home")
    ht_away = match.get("ht_away")
    ht_score = ""
    if ht_home is not None and ht_away is not None:
        ht_score = f"{ht_home}:{ht_away}"

    return {
        "detected_at_utc": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds"),
        "stage": stage,
        "match_id": match_id,
        "league": match.get("league", ""),
        "home_team": match.get("home_team", ""),
        "away_team": match.get("away_team", ""),
        "kickoff_time_utc": match.get("kickoff_time", ""),
        "status": match.get("status", ""),
        "ht_score": ht_score,
        "trigger_depth": match.get("trigger_depth", ""),
        "prev_depth": match.get("prev_depth", ""),
        "upgrade_ts": match.get("upgrade_ts", ""),
        "titan_asian_url": f"https://m.titan007.com/asian/{match_id}.htm" if match_id else "",
        "titan_live_url": f"https://live.titan007.com/detail/{match_id}cn.htm" if match_id else "",
    }


def write_match_signals(
    matches: Iterable[dict],
    stage: str,
    output_config: dict | None = None,
) -> int:
    """
    Append matches to CSV with de-duplication.

    Returns:
        Number of newly written rows.
    """
    cfg = output_config or {}
    if cfg.get("enabled", True) is False:
        return 0

    csv_path = Path(cfg.get("csv_path", "outputs/match_signals.csv"))
    dedupe_keys = list(cfg.get("dedupe_keys", ["stage", "match_id"]))
    if not dedupe_keys:
        dedupe_keys = ["stage", "match_id"]

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    existing_keys = _read_existing_keys(csv_path, dedupe_keys)

    rows_to_write: list[dict] = []
    for match in matches:
        row = _as_row(match, stage)
        key = tuple(str(row.get(k, "")).strip() for k in dedupe_keys)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        rows_to_write.append(row)

    if not rows_to_write:
        return 0

    write_header = not csv_path.exists() or csv_path.stat().st_size == 0
    with csv_path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DEFAULT_HEADERS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows_to_write)

    logger.info(f"CSV exported {len(rows_to_write)} rows -> {csv_path}")
    return len(rows_to_write)
