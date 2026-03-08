"""
球探网数据采集器（Playwright版）

工作原理：
  用 Playwright 打开球探网页面，拦截 XHR/fetch 请求，直接读取 JSON 数据。
  不需要解析 HTML，不需要 OCR，跟你手动打开页面看到的数据完全一致。

两种使用模式：
  1. 发现模式（首次使用必做）：
       python -m src.collectors.qiutan discover "https://www.qiutan.com/match/xxx"
     会打印所有网络请求，帮你找到实际的 API 接口 URL。
     找到后把关键词填入 config.yaml。

  2. 正式模式（配置好接口后）：
       在 main.py 中实例化 QiutanCollector，自动拦截请求。
"""

import asyncio
import json
import re
import sys
from datetime import datetime
from typing import Optional

from loguru import logger
from playwright.async_api import async_playwright, Request, Response, Page

from src.collectors.base import BaseCollector
from src.normalizer import parse_handicap


class QiutanCollector(BaseCollector):
    """
    球探网爬取器。

    config 示例：
    {
        "base_url": "https://www.qiutan.com",
        "match_list_pattern": "/matchList",     # 发现后填写
        "odds_history_pattern": "/oddsHistory", # 发现后填写
        "live_score_pattern": "/liveScore",     # 发现后填写
        "headless": True,
        "scan_window_minutes": 90,
        "page_timeout_ms": 15000,
    }
    """

    def __init__(self, config: dict):
        self.config = config
        self.base_url = config.get("base_url", "https://www.qiutan.com")
        self.headless = config.get("headless", True)
        self.page_timeout = config.get("page_timeout_ms", 15000)
        self._playwright = None
        self._browser = None
        self._page: Optional[Page] = None

    async def _ensure_browser(self):
        if self._browser is None:
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
            logger.info("Playwright 浏览器已启动")

    async def close(self):
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
        logger.info("Playwright 浏览器已关闭")

    # ── 核心：拦截响应 ──────────────────────────────────────────────────────────

    async def _intercept_json(self, url_pattern: str, trigger_url: str) -> Optional[dict]:
        """
        打开 trigger_url，等待匹配 url_pattern 的响应，返回其 JSON。

        Args:
            url_pattern: URL 关键词（在 config.yaml 中配置）
            trigger_url: 要打开的页面 URL

        Returns:
            解析后的 JSON dict，或 None（超时/未匹配）
        """
        await self._ensure_browser()
        result_holder = {}

        async def on_response(response: Response):
            if url_pattern and url_pattern in response.url:
                try:
                    data = await response.json()
                    result_holder["data"] = data
                    logger.debug(f"拦截到响应: {response.url}")
                except Exception as e:
                    logger.warning(f"解析响应JSON失败: {e}")

        self._page.on("response", on_response)
        try:
            await self._page.goto(trigger_url, timeout=self.page_timeout)
            # 等待数据加载（最多等 page_timeout 毫秒）
            for _ in range(self.page_timeout // 500):
                if "data" in result_holder:
                    break
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.warning(f"页面加载失败: {e}")
        finally:
            self._page.remove_listener("response", on_response)

        return result_holder.get("data")

    # ── 接口方法 ──────────────────────────────────────────────────────────────

    async def fetch_match_list(self) -> list[dict]:
        """
        获取即将开赛的比赛列表。

        NOTE: 接口URL需要在 discover 模式下找到，然后填入 config.yaml。
              如果接口未配置，返回空列表并记录警告。
        """
        pattern = self.config.get("match_list_pattern", "")
        if not pattern:
            logger.warning(
                "match_list_pattern 未配置。"
                "请先运行发现模式: python -m src.collectors.qiutan discover <url>"
            )
            return []

        # 打开比赛列表页面（URL需根据实际情况调整）
        list_url = f"{self.base_url}/index"
        raw = await self._intercept_json(pattern, list_url)
        if raw is None:
            logger.warning("未能获取比赛列表数据")
            return []

        return self._parse_match_list(raw)

    async def fetch_odds_history(self, match_id: str) -> list[dict]:
        """
        获取 bet365 亚盘历史。

        NOTE: 接口URL需要在 discover 模式下找到。
        """
        pattern = self.config.get("odds_history_pattern", "")
        if not pattern:
            logger.warning("odds_history_pattern 未配置，请先运行发现模式")
            return []

        match_url = f"{self.base_url}/match/{match_id}"
        raw = await self._intercept_json(pattern, match_url)
        if raw is None:
            return []

        return self._parse_odds_history(raw, match_id)

    async def fetch_live_data(self, match_id: str) -> dict:
        """
        获取实时比分和事件。
        """
        pattern = self.config.get("live_score_pattern", "")
        if not pattern:
            logger.warning("live_score_pattern 未配置，请先运行发现模式")
            return {}

        match_url = f"{self.base_url}/match/{match_id}"
        raw = await self._intercept_json(pattern, match_url)
        if raw is None:
            return {}

        return self._parse_live_data(raw, match_id)

    # ── 数据解析 ──────────────────────────────────────────────────────────────
    # NOTE: 以下解析函数需要根据实际接口返回格式调整。
    # 运行发现模式后，查看实际 JSON 结构，修改对应字段名。

    def _parse_match_list(self, raw: dict) -> list[dict]:
        """
        解析比赛列表 JSON。
        TODO: 根据球探实际接口结构调整字段名。
        """
        matches = []
        # 常见结构：{"code":0, "data": {"list": [...]}}
        items = (
            raw.get("data", {}).get("list", [])
            or raw.get("data", [])
            or raw.get("list", [])
            or (raw if isinstance(raw, list) else [])
        )

        for item in items:
            try:
                match = {
                    "id": str(item.get("matchId") or item.get("id") or item.get("match_id", "")),
                    "league": item.get("leagueName") or item.get("league") or item.get("competitionName", ""),
                    "home_team": item.get("homeName") or item.get("homeTeam") or item.get("home", ""),
                    "away_team": item.get("awayName") or item.get("awayTeam") or item.get("away", ""),
                    "kickoff_time": item.get("matchTime") or item.get("kickoff") or item.get("startTime", ""),
                    "status": "scheduled",
                    "ht_home": None,
                    "ht_away": None,
                    "ft_home": None,
                    "ft_away": None,
                }
                if match["id"]:
                    matches.append(match)
            except Exception as e:
                logger.warning(f"解析比赛条目失败: {e}, 原始数据: {item}")

        logger.info(f"解析到 {len(matches)} 场比赛")
        return matches

    def _parse_odds_history(self, raw: dict, match_id: str) -> list[dict]:
        """
        解析 bet365 亚盘历史 JSON。
        TODO: 根据球探实际接口结构调整字段名和盘口字段的格式。
        """
        records = []
        # 常见结构：{"code":0, "data": [{"time":"...", "handicap":"-1", "homeOdds":0.85, ...}]}
        items = (
            raw.get("data", {}).get("list", [])
            or raw.get("data", [])
            or raw.get("list", [])
            or (raw if isinstance(raw, list) else [])
        )

        for item in items:
            try:
                # 盘口字段名可能是：handicap / line / spread / hdp
                handicap_raw = (
                    item.get("handicap")
                    or item.get("line")
                    or item.get("spread")
                    or item.get("hdp")
                    or "0"
                )
                depth, home_gives = parse_handicap(str(handicap_raw))

                record = {
                    "match_id": match_id,
                    "bookmaker": "bet365",
                    "line_depth": depth,
                    "home_gives": int(home_gives),
                    "home_odds": float(item.get("homeOdds") or item.get("home_odds") or 0),
                    "away_odds": float(item.get("awayOdds") or item.get("away_odds") or 0),
                    "ts": item.get("time") or item.get("ts") or item.get("updateTime") or "",
                }
                records.append(record)
            except Exception as e:
                logger.warning(f"解析赔率条目失败: {e}, 原始数据: {item}")

        logger.debug(f"[{match_id}] 解析到 {len(records)} 条赔率历史")
        return records

    def _parse_live_data(self, raw: dict, match_id: str) -> dict:
        """
        解析实时比分和事件 JSON。
        TODO: 根据球探实际接口结构调整字段名。
        """
        data = raw.get("data", raw)

        # 比分
        score = data.get("score") or data.get("result") or {}
        ht = score.get("half") or score.get("ht") or {}
        ft = score.get("full") or score.get("ft") or {}

        # 状态映射
        raw_status = data.get("status") or data.get("matchStatus") or ""
        status_map = {
            "1": "live", "2": "halftime", "3": "finished",
            "HT": "halftime", "FT": "finished",
        }
        status = status_map.get(str(raw_status), "live")

        # 事件（进球/红牌）
        events = []
        for ev in (data.get("events") or data.get("incidents") or []):
            event_type = None
            raw_type = str(ev.get("type") or ev.get("incidentType") or "")
            if "goal" in raw_type.lower() or raw_type in ("1", "goal"):
                event_type = "goal"
            elif "red" in raw_type.lower() or raw_type in ("5", "red_card"):
                event_type = "red_card"
            if event_type:
                team = "home" if ev.get("team") in ("home", "1", 1) else "away"
                events.append({
                    "match_id": match_id,
                    "minute": int(ev.get("minute") or ev.get("min") or 0),
                    "event_type": event_type,
                    "team": team,
                })

        return {
            "match_id": match_id,
            "status": status,
            "ht_home": ht.get("home"),
            "ht_away": ht.get("away"),
            "ft_home": ft.get("home"),
            "ft_away": ft.get("away"),
            "events": events,
        }


# ── 发现模式 ──────────────────────────────────────────────────────────────────

async def _discover(url: str):
    """
    打开指定页面，打印所有 XHR/fetch 请求。
    用于发现球探网的实际 API 接口。
    """
    print(f"\n[发现模式] 正在打开: {url}")
    print("浏览器将保持打开10秒，请手动点击页面上的指数/让球选项卡...\n")

    captured = []

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
                print(f"  [XHR] {req.method} {req.url}")
                captured.append({"method": req.method, "url": req.url})

        page.on("request", on_request)

        try:
            await page.goto(url, timeout=15000)
        except Exception as e:
            print(f"  [警告] 页面加载: {e}")

        # 等待30秒，让用户手动操作
        print("\n等待30秒，请在浏览器中点击「指数」→「让球」→ 展开 bet365...\n")
        await asyncio.sleep(30)
        await browser.close()

    print(f"\n[发现模式结束] 共捕获 {len(captured)} 个 XHR/fetch 请求")
    print("\n请在上面的输出中找到：")
    print("  - 比赛列表接口（含 match/schedule/fixture 等关键词）")
    print("  - 赔率历史接口（含 odds/handicap/index 等关键词）")
    print("  - 实时比分接口（含 score/live/result 等关键词）")
    print("\n找到后，把 URL 的关键词填入 config.yaml 的对应字段。\n")


if __name__ == "__main__":
    # python -m src.collectors.qiutan discover "https://www.qiutan.com/match/xxx"
    if len(sys.argv) >= 3 and sys.argv[1] == "discover":
        asyncio.run(_discover(sys.argv[2]))
    else:
        print("用法: python -m src.collectors.qiutan discover <比赛页面URL>")
        print("示例: python -m src.collectors.qiutan discover https://www.qiutan.com/match/12345")
