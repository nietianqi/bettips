"""bettips scheduler entrypoint."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

from src import halftime, scanner, storage
from src import csv_export
from src.collectors.base import BaseCollector
from src.collectors.qiutan import QiutanCollector
from src.collectors.titan import TitanCollector


def load_config(path: str = "config.yaml") -> dict:
    cfg_path = Path(path)
    if not cfg_path.exists():
        logger.error(f"Config file not found: {path}")
        sys.exit(1)
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


_collector: BaseCollector | None = None
_config: dict = {}
_db_path: str = "bettips.db"


def _collector_backend() -> str:
    return str(_config.get("runtime", {}).get("collector", "titan")).strip().lower()


def _scan_window_minutes() -> int:
    backend = _collector_backend()
    if backend == "qiutan":
        return int(_config.get("qiutan", {}).get("scan_window_minutes", 90))
    return int(_config.get("titan", {}).get("scan_window_minutes", 90))


def _build_collector() -> BaseCollector:
    backend = _collector_backend()
    if backend == "qiutan":
        return QiutanCollector(_config.get("qiutan", {}))
    return TitanCollector(_config.get("titan", {}))


async def collect_odds() -> None:
    """Fetch match list, odds history and live score updates."""
    try:
        scan_window = _scan_window_minutes()
        matches = await _collector.fetch_match_list() if _collector else []
        for match in matches:
            storage.upsert_match(_db_path, match)

        upcoming = storage.get_upcoming_matches(_db_path, within_minutes=scan_window)
        watching = storage.get_live_candidates(_db_path)
        targets = {str(m["id"]): m for m in upcoming}
        for row in watching:
            targets[str(row["id"])] = row

        logger.debug(f"Collect round targets: {len(targets)}")

        for match_id, match in targets.items():
            odds_rows = await _collector.fetch_odds_history(match_id) if _collector else []
            for record in odds_rows:
                storage.insert_odds(_db_path, record)

            live = await _collector.fetch_live_data(match_id) if _collector else {}
            if not live:
                continue

            storage.upsert_match(
                _db_path,
                {
                    "id": match_id,
                    "league": match.get("league", ""),
                    "home_team": match.get("home_team", ""),
                    "away_team": match.get("away_team", ""),
                    "kickoff_time": match.get("kickoff_time", ""),
                    "status": live.get("status", ""),
                    "ht_home": live.get("ht_home"),
                    "ht_away": live.get("ht_away"),
                    "ft_home": live.get("ft_home"),
                    "ft_away": live.get("ft_away"),
                },
            )
            for event in live.get("events", []):
                storage.insert_event(_db_path, event)

    except Exception as exc:
        logger.error(f"collect_odds failed: {exc}")


async def run_pre_match_scan() -> None:
    try:
        cfg = _config.get("scanner", {})
        new_candidates = scanner.run_pre_match_scan(
            db_path=_db_path,
            bookmaker=cfg.get("bookmaker", "bet365"),
            min_depth=float(cfg.get("min_line_depth", 1.0)),
            window_minutes=int(cfg.get("late_upgrade_window_minutes", 15)),
            scan_window=_scan_window_minutes(),
        )
        if new_candidates:
            rows = []
            for c in new_candidates:
                match = storage.get_match(_db_path, c["match_id"])
                if not match:
                    continue
                rows.append({**match, **c})
            if rows:
                csv_export.write_match_signals(rows, "pre_match_candidate", _config.get("output", {}))
    except Exception as exc:
        logger.error(f"pre-match scan failed: {exc}")


async def run_halftime_check() -> None:
    try:
        alerted = halftime.run_halftime_check(_db_path, _config.get("alert", {}))
        if alerted:
            logger.info(f"Halftime alerts sent: {len(alerted)}")
            rows = []
            for match_id in alerted:
                match = storage.get_match(_db_path, match_id)
                if not match:
                    continue
                rows.append(match)
            if rows:
                csv_export.write_match_signals(rows, "ht_alert", _config.get("output", {}))
    except Exception as exc:
        logger.error(f"halftime check failed: {exc}")


async def main() -> None:
    global _collector, _config, _db_path

    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    )
    logger.add("logs/bettips_{time:YYYY-MM-DD}.log", level="DEBUG", rotation="1 day", retention="7 days")

    _config = load_config()
    _db_path = _config.get("db", {}).get("path", "bettips.db")
    storage.init_db(_db_path)

    _collector = _build_collector()
    backend = _collector_backend()
    if backend == "qiutan":
        qiutan_cfg = _config.get("qiutan", {})
        missing = [k for k in ("match_list_pattern", "odds_history_pattern", "live_score_pattern") if not qiutan_cfg.get(k)]
        if missing:
            logger.warning(
                "Missing qiutan URL patterns: "
                f"{missing}. Run: python -m src.collectors.qiutan discover <match_url>"
            )

    scheduler = AsyncIOScheduler()
    scheduler.add_job(collect_odds, "interval", seconds=60, id="collect_odds")
    scheduler.add_job(run_pre_match_scan, "interval", seconds=60, id="pre_match_scan")
    scheduler.add_job(run_halftime_check, "interval", seconds=120, id="halftime_check")
    scheduler.start()

    logger.info("=" * 48)
    logger.info("bettips started")
    logger.info(f"collector backend: {backend}")
    logger.info(f"db path: {_db_path}")
    logger.info("jobs: collect_odds(60s), pre_match_scan(60s), halftime_check(120s)")
    logger.info("=" * 48)

    await collect_odds()
    await run_pre_match_scan()
    await run_halftime_check()

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received")
    finally:
        scheduler.shutdown()
        if _collector:
            await _collector.close()
        logger.info("bettips stopped")


if __name__ == "__main__":
    asyncio.run(main())
