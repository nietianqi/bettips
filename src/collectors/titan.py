"""Titan007 mobile collector (no manual network-pattern config required)."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Optional

from loguru import logger

from src.collectors.base import BaseCollector
from src.collectors.titan_http import (
    TitanHttpClient,
    normalize_handicap_history,
    parse_schedule_matches,
    resolve_bet365_oddsid,
)


class TitanCollector(BaseCollector):
    """Collector built on mobile titan endpoints."""

    def __init__(self, config: dict):
        self.config = config
        self.client = TitanHttpClient(
            base_url=config.get("base_url", "https://m.titan007.com"),
            timeout=int(config.get("timeout_seconds", 20)),
            min_interval_ms=int(config.get("min_interval_ms", 800)),
            random_delay_min_ms=int(config.get("random_delay_min_ms", 120)),
            random_delay_max_ms=int(config.get("random_delay_max_ms", 420)),
            retry_attempts=int(config.get("retry_attempts", 3)),
            retry_backoff_seconds=float(config.get("retry_backoff_seconds", 1.2)),
            retry_jitter_seconds=float(config.get("retry_jitter_seconds", 0.6)),
            warmup_interval_seconds=int(config.get("warmup_interval_seconds", 900)),
        )
        self._schedule_map: dict[str, dict] = {}
        self._oddsid_cache: dict[str, tuple[Optional[int], float]] = {}
        self._oddsid_ttl_seconds = int(config.get("oddsid_ttl_seconds", 6 * 3600))
        self._oddsid_missing_ttl_seconds = int(config.get("oddsid_missing_ttl_seconds", 1800))
        self._max_matches_per_round = int(config.get("max_matches_per_round", 120))

    async def close(self) -> None:
        self.client.close()

    async def fetch_match_list(self) -> list[dict]:
        text = await asyncio.to_thread(
            self.client.fetch_schedule_text,
            int(self.config.get("score_type", 0)),
            int(self.config.get("language", 0)),
            self.config.get("cookie"),
        )
        rows = parse_schedule_matches(text)
        rows = [row for row in rows if row.get("status") != "finished"]
        rows.sort(key=lambda x: x.get("kickoff_time", ""))
        if self._max_matches_per_round > 0:
            rows = rows[: self._max_matches_per_round]

        self._schedule_map = {str(row["id"]): row for row in rows}
        logger.info(f"Titan schedule rows: {len(rows)}")
        return rows

    def _get_cached_oddsid(self, scheid: str) -> tuple[bool, Optional[int]]:
        cached = self._oddsid_cache.get(scheid)
        if not cached:
            return False, None
        oddsid, expire_at = cached
        if time.time() >= expire_at:
            self._oddsid_cache.pop(scheid, None)
            return False, None
        return True, oddsid

    async def _resolve_oddsid(self, scheid: str) -> Optional[int]:
        hit, cached = self._get_cached_oddsid(scheid)
        if hit:
            return cached

        payload = await asyncio.to_thread(
            self.client.fetch_handicap_companies,
            int(scheid),
            0,
            0,
            1,
            self.config.get("cookie"),
        )
        prefer_num = self.config.get("bet365_line_num_prefer", 4)
        try:
            prefer_num = int(prefer_num) if prefer_num is not None else None
        except (TypeError, ValueError):
            prefer_num = 4
        oddsid = resolve_bet365_oddsid(payload, prefer_num=prefer_num)
        ttl = self._oddsid_ttl_seconds if oddsid is not None else self._oddsid_missing_ttl_seconds
        self._oddsid_cache[scheid] = (oddsid, time.time() + ttl)
        return oddsid

    async def fetch_odds_history(self, match_id: str) -> list[dict]:
        oddsid = await self._resolve_oddsid(str(match_id))
        if oddsid is None:
            logger.debug(f"[{match_id}] bet365 oddsid not found")
            return []

        payload = await asyncio.to_thread(
            self.client.fetch_handicap_history,
            int(match_id),
            int(oddsid),
            2,
            0,
            0,
            None,
            self.config.get("cookie"),
        )
        rows = normalize_handicap_history(payload, fill_missing_draw_odds=False)

        records: list[dict] = []
        for row in rows:
            draw = row.get("draw_odds")
            ts = row.get("modify_ts")
            if draw is None or ts is None:
                continue
            home_gives = 1 if float(draw) <= 0 else 0
            depth = abs(float(draw))
            ts_iso = datetime.utcfromtimestamp(int(ts)).isoformat(sep=" ", timespec="seconds")
            records.append(
                {
                    "match_id": str(match_id),
                    "bookmaker": "bet365",
                    "line_depth": depth,
                    "home_gives": home_gives,
                    "home_odds": row.get("home_odds"),
                    "away_odds": row.get("away_odds"),
                    "ts": ts_iso,
                }
            )
        return records

    async def fetch_live_data(self, match_id: str) -> dict:
        # Prefer cached schedule snapshot from current round.
        row = self._schedule_map.get(str(match_id))
        if row is None:
            return {}

        status = row.get("status", "scheduled")
        ht_home = row.get("ht_home")
        ht_away = row.get("ht_away")
        ft_home = row.get("ft_home")
        ft_away = row.get("ft_away")

        events: list[dict] = []
        home_red = int(row.get("home_red") or 0)
        away_red = int(row.get("away_red") or 0)
        red_minute = 45 if status == "halftime" else 90
        for _ in range(home_red):
            events.append(
                {
                    "match_id": str(match_id),
                    "minute": red_minute,
                    "event_type": "red_card",
                    "team": "home",
                }
            )
        for _ in range(away_red):
            events.append(
                {
                    "match_id": str(match_id),
                    "minute": red_minute,
                    "event_type": "red_card",
                    "team": "away",
                }
            )

        return {
            "match_id": str(match_id),
            "status": status,
            "ht_home": ht_home,
            "ht_away": ht_away,
            "ft_home": ft_home,
            "ft_away": ft_away,
            "events": events,
        }
