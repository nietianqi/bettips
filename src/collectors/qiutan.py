"""Qiutan collector implemented with Playwright network interception."""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime
from typing import Optional

from loguru import logger
from playwright.async_api import Page, Request, Response, TimeoutError, async_playwright

from src.collectors.base import BaseCollector
from src.normalizer import parse_handicap
from src.timeutils import parse_datetime


def _pick_items(raw: dict) -> list[dict]:
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if not isinstance(raw, dict):
        return []

    candidates = [
        raw.get("data", {}).get("list") if isinstance(raw.get("data"), dict) else None,
        raw.get("data", {}).get("matches") if isinstance(raw.get("data"), dict) else None,
        raw.get("data") if isinstance(raw.get("data"), list) else None,
        raw.get("list"),
        raw.get("matches"),
    ]
    for value in candidates:
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_status(raw_status) -> str:
    key = str(raw_status or "").strip().lower()
    mapping = {
        "0": "scheduled",
        "ns": "scheduled",
        "scheduled": "scheduled",
        "1": "live",
        "live": "live",
        "inplay": "live",
        "2": "halftime",
        "ht": "halftime",
        "halftime": "halftime",
        "3": "finished",
        "ft": "finished",
        "finished": "finished",
    }
    return mapping.get(key, "scheduled")


class QiutanCollector(BaseCollector):
    """Qiutan collector with URL pattern based response capture."""

    def __init__(self, config: dict):
        self.config = config
        self.base_url = config.get("base_url", "https://www.qiutan.com")
        self.headless = bool(config.get("headless", True))
        self.page_timeout = int(config.get("page_timeout_ms", 15000))
        self._playwright = None
        self._browser = None
        self._page: Optional[Page] = None

    async def _ensure_browser(self) -> None:
        if self._browser:
            return
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
        )
        self._page = await context.new_page()
        logger.info("Playwright browser started")

    async def close(self) -> None:
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
        logger.info("Playwright browser closed")

    async def _intercept_json(self, url_pattern: str, trigger_url: str) -> Optional[dict]:
        await self._ensure_browser()
        if not url_pattern:
            return None

        result_holder: dict = {}
        done = asyncio.Event()

        async def on_response(response: Response):
            if url_pattern not in response.url:
                return
            try:
                result_holder["data"] = await response.json()
                done.set()
                logger.debug(f"Captured response: {response.url}")
            except Exception as exc:
                logger.debug(f"Failed to parse response as JSON: {exc}")

        assert self._page is not None
        self._page.on("response", on_response)
        try:
            await self._page.goto(trigger_url, wait_until="domcontentloaded", timeout=self.page_timeout)
            await asyncio.wait_for(done.wait(), timeout=self.page_timeout / 1000)
        except TimeoutError:
            logger.warning(f"Capture timeout for pattern: {url_pattern}")
        except asyncio.TimeoutError:
            logger.warning(f"Capture timeout for pattern: {url_pattern}")
        except Exception as exc:
            logger.warning(f"Failed to load page {trigger_url}: {exc}")
        finally:
            self._page.remove_listener("response", on_response)

        return result_holder.get("data")

    async def fetch_match_list(self) -> list[dict]:
        pattern = self.config.get("match_list_pattern", "")
        if not pattern:
            logger.warning("match_list_pattern is not configured")
            return []
        list_url = self.config.get("match_list_page", f"{self.base_url}/index")
        raw = await self._intercept_json(pattern, list_url)
        if raw is None:
            return []
        return self._parse_match_list(raw)

    async def fetch_odds_history(self, match_id: str) -> list[dict]:
        pattern = self.config.get("odds_history_pattern", "")
        if not pattern:
            logger.warning("odds_history_pattern is not configured")
            return []
        match_url_tpl = self.config.get("match_page_template", f"{self.base_url}/match/{{match_id}}")
        match_url = str(match_url_tpl).format(match_id=match_id)
        raw = await self._intercept_json(pattern, match_url)
        if raw is None:
            return []
        return self._parse_odds_history(raw, str(match_id))

    async def fetch_live_data(self, match_id: str) -> dict:
        pattern = self.config.get("live_score_pattern", "")
        if not pattern:
            logger.warning("live_score_pattern is not configured")
            return {}
        match_url_tpl = self.config.get("match_page_template", f"{self.base_url}/match/{{match_id}}")
        match_url = str(match_url_tpl).format(match_id=match_id)
        raw = await self._intercept_json(pattern, match_url)
        if raw is None:
            return {}
        return self._parse_live_data(raw, str(match_id))

    def _parse_match_list(self, raw: dict) -> list[dict]:
        matches: list[dict] = []
        for item in _pick_items(raw):
            match_id = str(item.get("matchId") or item.get("id") or item.get("match_id") or "").strip()
            kickoff = parse_datetime(
                item.get("matchTime")
                or item.get("kickoff")
                or item.get("startTime")
                or item.get("start_time")
            )
            if not match_id or kickoff is None:
                continue

            status = _normalize_status(item.get("status") or item.get("matchStatus"))
            if status != "scheduled":
                continue

            matches.append(
                {
                    "id": match_id,
                    "league": item.get("leagueName") or item.get("league") or item.get("competitionName") or "",
                    "home_team": item.get("homeName") or item.get("homeTeam") or item.get("home") or "",
                    "away_team": item.get("awayName") or item.get("awayTeam") or item.get("away") or "",
                    "kickoff_time": kickoff.isoformat(sep=" ", timespec="seconds"),
                    "status": status,
                    "ht_home": None,
                    "ht_away": None,
                    "ft_home": None,
                    "ft_away": None,
                }
            )
        logger.info(f"Parsed {len(matches)} upcoming matches")
        return matches

    def _parse_odds_history(self, raw: dict, match_id: str) -> list[dict]:
        records: list[dict] = []
        for item in _pick_items(raw):
            bookmaker_raw = str(
                item.get("bookmaker")
                or item.get("companyName")
                or item.get("company")
                or item.get("name")
                or "bet365"
            ).lower()
            if "365" not in bookmaker_raw:
                continue

            handicap_raw = (
                item.get("handicap")
                or item.get("line")
                or item.get("spread")
                or item.get("hdp")
                or item.get("ah")
            )
            if handicap_raw is None:
                continue
            try:
                depth, home_gives = parse_handicap(str(handicap_raw))
            except ValueError:
                continue

            ts = parse_datetime(item.get("time") or item.get("ts") or item.get("updateTime") or item.get("changeTime"))
            if ts is None:
                continue

            records.append(
                {
                    "match_id": match_id,
                    "bookmaker": "bet365",
                    "line_depth": depth,
                    "home_gives": int(home_gives),
                    "home_odds": _safe_float(item.get("homeOdds") or item.get("home_odds")),
                    "away_odds": _safe_float(item.get("awayOdds") or item.get("away_odds")),
                    "ts": ts.isoformat(sep=" ", timespec="seconds"),
                }
            )

        logger.debug(f"[{match_id}] parsed odds history rows: {len(records)}")
        return records

    def _parse_live_data(self, raw: dict, match_id: str) -> dict:
        data = raw.get("data", raw) if isinstance(raw, dict) else {}
        if isinstance(data, list):
            data = data[0] if data else {}

        score = data.get("score") if isinstance(data.get("score"), dict) else {}
        half_score = score.get("half") if isinstance(score.get("half"), dict) else {}
        full_score = score.get("full") if isinstance(score.get("full"), dict) else {}

        ht_home = (
            _safe_int(data.get("ht_home"))
            if data.get("ht_home") is not None
            else _safe_int(half_score.get("home"))
        )
        ht_away = (
            _safe_int(data.get("ht_away"))
            if data.get("ht_away") is not None
            else _safe_int(half_score.get("away"))
        )
        ft_home = (
            _safe_int(data.get("ft_home"))
            if data.get("ft_home") is not None
            else _safe_int(full_score.get("home"))
        )
        ft_away = (
            _safe_int(data.get("ft_away"))
            if data.get("ft_away") is not None
            else _safe_int(full_score.get("away"))
        )

        events: list[dict] = []
        for ev in (data.get("events") or data.get("incidents") or []):
            raw_type = str(ev.get("type") or ev.get("incidentType") or "").lower()
            if "red" in raw_type or raw_type in {"5", "red_card"}:
                event_type = "red_card"
            elif "goal" in raw_type or raw_type in {"1", "goal"}:
                event_type = "goal"
            else:
                continue
            team_raw = ev.get("team") or ev.get("side")
            team = "home" if str(team_raw).lower() in {"home", "1"} else "away"
            minute = _safe_int(ev.get("minute") or ev.get("min"), 0)
            events.append(
                {
                    "match_id": match_id,
                    "minute": minute or 0,
                    "event_type": event_type,
                    "team": team,
                    "ts": datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
                }
            )

        return {
            "match_id": match_id,
            "status": _normalize_status(data.get("status") or data.get("matchStatus")),
            "ht_home": ht_home,
            "ht_away": ht_away,
            "ft_home": ft_home,
            "ft_away": ft_away,
            "events": events,
        }


async def _discover(url: str) -> None:
    print(f"\n[Discover] Opening: {url}")
    print("Browser stays open for 30 seconds. Click odds/handicap tabs manually.\n")
    captured: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
        )
        page = await context.new_page()

        def on_request(req: Request):
            if req.resource_type in ("xhr", "fetch"):
                print(f"[XHR] {req.method} {req.url}")
                captured.append({"method": req.method, "url": req.url})

        page.on("request", on_request)
        try:
            await page.goto(url, timeout=15000)
        except Exception as exc:
            print(f"[WARN] page load issue: {exc}")

        await asyncio.sleep(30)
        await browser.close()

    print(f"\n[Discover done] captured {len(captured)} XHR/fetch requests")
    print("Find URL fragments for:")
    print("- match list API")
    print("- odds history API")
    print("- live score API")
    print("Then place them into config.yaml")


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "discover":
        asyncio.run(_discover(sys.argv[2]))
    else:
        print("Usage: python -m src.collectors.qiutan discover <match_page_url>")
