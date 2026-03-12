"""HTTP client and parsers for Titan007 mobile endpoints."""

from __future__ import annotations

import random
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Optional

import requests


MOBILE_SAFARI_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1"
)


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _cookie_header(cookies: str | dict[str, str] | None) -> Optional[str]:
    if cookies is None:
        return None
    if isinstance(cookies, str):
        return cookies
    if isinstance(cookies, dict):
        return "; ".join([f"{k}={v}" for k, v in cookies.items()])
    return None


def _parse_ids(text: Optional[str]) -> list[int]:
    if not text:
        return []
    ids: list[int] = []
    for token in str(text).split(","):
        token = token.strip()
        if not token:
            continue
        parsed = _to_int(token)
        if parsed is not None:
            ids.append(parsed)
    return ids


def _to_unix_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        num = float(value)
        if abs(num) > 1_000_000_000_000:
            num /= 1000
        return int(num)
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    text = str(value).strip()
    if not text:
        return None
    if text.lstrip("-").isdigit():
        return _to_unix_seconds(int(text))
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return None


class TitanHttpClient:
    """Minimal titan007 mobile HTTP client."""

    def __init__(
        self,
        base_url: str = "https://m.titan007.com",
        timeout: int = 20,
        user_agent: str = MOBILE_SAFARI_UA,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                "cache-control": "no-cache",
                "pragma": "no-cache",
                "user-agent": user_agent,
            }
        )

    def close(self) -> None:
        self.session.close()

    def fetch_goal3_xml(self, flesh: Optional[str] = None, cookies: str | dict[str, str] | None = None) -> str:
        """Fetch odds snapshot XML from /txt/goal3.xml."""
        if flesh is None:
            flesh = f"{random.random():.13f}"

        headers = {"referer": f"{self.base_url}/"}
        cookie_header = _cookie_header(cookies)
        if cookie_header:
            headers["cookie"] = cookie_header

        response = self.session.get(
            f"{self.base_url}/txt/goal3.xml",
            params={"flesh": flesh},
            headers=headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.text

    def fetch_handicap_history(
        self,
        scheid: int,
        oddsid: int,
        type_: int = 2,
        oddskind: int = 0,
        is_half: int = 0,
        flesh: Optional[int] = None,
        cookies: str | dict[str, str] | None = None,
    ) -> dict:
        """Fetch asian handicap history from HandicapDataInterface.ashx."""
        if flesh is None:
            flesh = int(time.time() * 1000)

        headers = {"referer": f"{self.base_url}/asian/{scheid}.htm"}
        cookie_header = _cookie_header(cookies)
        if cookie_header:
            headers["cookie"] = cookie_header

        response = self.session.get(
            f"{self.base_url}/HandicapDataInterface.ashx",
            params={
                "scheid": scheid,
                "type": type_,
                "oddskind": oddskind,
                "oddsid": oddsid,
                "isHalf": is_half,
                "flesh": flesh,
            },
            headers=headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()


def parse_goal3_matches(xml_text: str) -> dict:
    """
    Parse goal3 XML.

    NOTE:
    - goal3.xml is primarily an odds snapshot feed.
    - It reliably contains scheid + letgoal/europe/ou snapshots.
    - Team names and rich match state come from Schedule_*.txt in site scripts.
    """
    root = ET.fromstring(xml_text)
    rows: list[dict] = []

    for m in root.findall(".//m"):
        text = (m.text or "").strip()
        if not text:
            continue
        cols = [c.strip() for c in text.split(",")]
        if not cols:
            continue

        row = {
            "scheid": _to_int(cols[0]) if len(cols) > 0 else None,
            "company_id": _to_int(cols[1]) if len(cols) > 1 else None,
            "letgoal": _to_float(cols[2]) if len(cols) > 2 else None,
            "home_odds": _to_float(cols[3]) if len(cols) > 3 else None,
            "away_odds": _to_float(cols[4]) if len(cols) > 4 else None,
            "europe_odds_id": _to_int(cols[5]) if len(cols) > 5 else None,
            "home_win": _to_float(cols[6]) if len(cols) > 6 else None,
            "draw_win": _to_float(cols[7]) if len(cols) > 7 else None,
            "away_win": _to_float(cols[8]) if len(cols) > 8 else None,
            "ou_odds_id": _to_int(cols[9]) if len(cols) > 9 else None,
            "ou_line": _to_float(cols[10]) if len(cols) > 10 else None,
            "over_odds": _to_float(cols[11]) if len(cols) > 11 else None,
            "under_odds": _to_float(cols[12]) if len(cols) > 12 else None,
            # Optional columns often present in goal3.xml rows:
            "match_state": _to_int(cols[13]) if len(cols) > 13 else None,
            "home_score": _to_int(cols[14]) if len(cols) > 14 else None,
            "away_score": _to_int(cols[15]) if len(cols) > 15 else None,
            "first_letgoal": _to_float(cols[-2]) if len(cols) >= 2 else None,
            "first_ou_line": _to_float(cols[-1]) if len(cols) >= 1 else None,
            "raw_columns": cols,
        }
        rows.append(row)

    ids = _parse_ids(root.findtext("ids"))
    jc_ids = _parse_ids(root.findtext("jcIds"))
    is_maintain = _to_int(root.findtext("isMaintain")) or 0

    return {
        "rows": rows,
        "ids": ids,
        "jc_ids": jc_ids,
        "is_maintain": is_maintain,
    }


def normalize_handicap_history(json_data: dict, fill_missing_draw_odds: bool = False) -> list[dict]:
    """Normalize HandicapDataInterface.ashx response into sorted rows."""
    odds_id = _to_int(json_data.get("oddsId"))
    details = json_data.get("details", [])
    normalized: list[dict] = []
    last_draw: Optional[float] = None

    for item in details:
        draw_odds = _to_float(item.get("drawOdds"))
        if draw_odds is None and fill_missing_draw_odds:
            draw_odds = last_draw
        if draw_odds is not None:
            last_draw = draw_odds

        modify_ts = _to_unix_seconds(item.get("modifyTime"))
        normalized.append(
            {
                "odds_id": odds_id,
                "home_odds": _to_float(item.get("homeOdds")),
                "away_odds": _to_float(item.get("awayOdds")),
                "draw_odds": draw_odds,
                "kind": str(item.get("kind", "")),
                "modify_ts": modify_ts,
                "modify_dt_utc": (
                    datetime.fromtimestamp(modify_ts, tz=timezone.utc).isoformat() if modify_ts is not None else None
                ),
            }
        )

    normalized.sort(key=lambda x: (x["modify_ts"] is None, x["modify_ts"] or 0))

    deduped: list[dict] = []
    seen = set()
    for row in normalized:
        key = (
            row["modify_ts"],
            row["draw_odds"],
            row["home_odds"],
            row["away_odds"],
            row["kind"],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def is_deep_main_line(draw_odds: Optional[float], min_depth: float = 1.0) -> bool:
    """Main-team deep line check: drawOdds <= -min_depth."""
    if draw_odds is None:
        return False
    return draw_odds <= -abs(min_depth)


def detect_first_late_upgrade(
    history: list[dict],
    kickoff_ts: Any,
    window_minutes: int = 15,
) -> tuple[bool, Optional[dict]]:
    """
    Detect first-time late handicap deepening.

    Rule:
    - In [kickoff - window, kickoff], current draw_odds < previous draw_odds (more negative => deeper).
    - The new draw_odds must not appear before current row in earlier history.
    """
    kickoff = _to_unix_seconds(kickoff_ts)
    if kickoff is None:
        return False, None
    late_start = kickoff - window_minutes * 60

    rows = []
    for row in history:
        ts = _to_unix_seconds(row.get("modify_ts"))
        draw = _to_float(row.get("draw_odds"))
        if ts is None or draw is None:
            continue
        rows.append({"modify_ts": ts, "draw_odds": draw, **row})
    rows.sort(key=lambda x: x["modify_ts"])

    seen_lines: set[float] = set()
    prev: Optional[dict] = None
    for curr in rows:
        ts = curr["modify_ts"]
        line = curr["draw_odds"]

        if prev is not None and late_start <= ts <= kickoff:
            if line < prev["draw_odds"] and line not in seen_lines:
                return True, {
                    "prev_draw_odds": prev["draw_odds"],
                    "curr_draw_odds": line,
                    "modify_ts": ts,
                    "modify_dt_utc": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                }

        seen_lines.add(line)
        prev = curr
    return False, None
