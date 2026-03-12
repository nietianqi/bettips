"""HTTP client and parsers for Titan007 mobile endpoints."""

from __future__ import annotations

import random
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
from loguru import logger

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


MOBILE_SAFARI_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1"
)

MOBILE_CHROME_ANDROID_UA = (
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36"
)

MOBILE_EDGE_ANDROID_UA = (
    "Mozilla/5.0 (Linux; Android 13; SM-G996B) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36 EdgA/121.0.0.0"
)

DEFAULT_UA_POOL = [MOBILE_SAFARI_UA, MOBILE_CHROME_ANDROID_UA, MOBILE_EDGE_ANDROID_UA]

_CN_TZ = ZoneInfo("Asia/Shanghai") if ZoneInfo is not None else timezone(timedelta(hours=8))


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


def _parse_compact_match_time(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    local_dt = None
    if len(text) >= 14:
        try:
            local_dt = datetime.strptime(text[:14], "%Y%m%d%H%M%S")
        except ValueError:
            local_dt = None
    if local_dt is None and len(text) >= 12:
        try:
            local_dt = datetime.strptime(text[:12], "%Y%m%d%H%M")
        except ValueError:
            local_dt = None
    if local_dt is None:
        return None
    local_dt = local_dt.replace(tzinfo=_CN_TZ)
    return local_dt.astimezone(timezone.utc).replace(tzinfo=None)


class TitanHttpClient:
    """Minimal titan007 mobile HTTP client."""

    def __init__(
        self,
        base_url: str = "https://m.titan007.com",
        timeout: int = 20,
        user_agent: Optional[str] = None,
        user_agent_pool: Optional[list[str]] = None,
        min_interval_ms: int = 800,
        random_delay_min_ms: int = 120,
        random_delay_max_ms: int = 420,
        retry_attempts: int = 3,
        retry_backoff_seconds: float = 1.2,
        retry_jitter_seconds: float = 0.6,
        warmup_interval_seconds: int = 900,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retry_attempts = max(1, int(retry_attempts))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.retry_jitter_seconds = max(0.0, float(retry_jitter_seconds))
        self.min_interval_seconds = max(0.0, int(min_interval_ms) / 1000.0)
        self.random_delay_min_seconds = max(0.0, int(random_delay_min_ms) / 1000.0)
        self.random_delay_max_seconds = max(
            self.random_delay_min_seconds,
            int(random_delay_max_ms) / 1000.0,
        )
        self.warmup_interval_seconds = max(0, int(warmup_interval_seconds))
        self._rate_lock = threading.Lock()
        self._last_request_at = 0.0
        self._last_warmup_at = 0.0

        pool = [ua for ua in (user_agent_pool or DEFAULT_UA_POOL) if ua]
        self._user_agent_pool = pool if pool else [MOBILE_SAFARI_UA]
        self._active_user_agent = user_agent or random.choice(self._user_agent_pool)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                "cache-control": "no-cache",
                "pragma": "no-cache",
                "user-agent": self._active_user_agent,
            }
        )

    def close(self) -> None:
        self.session.close()

    def _apply_rate_limit(self) -> None:
        with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_at
            if elapsed < self.min_interval_seconds:
                time.sleep(self.min_interval_seconds - elapsed)
            if self.random_delay_max_seconds > 0:
                jitter = random.uniform(self.random_delay_min_seconds, self.random_delay_max_seconds)
                if jitter > 0:
                    time.sleep(jitter)
            self._last_request_at = time.monotonic()

    def _rotate_user_agent(self) -> None:
        if not self._user_agent_pool:
            return
        if len(self._user_agent_pool) == 1:
            self._active_user_agent = self._user_agent_pool[0]
        else:
            choices = [ua for ua in self._user_agent_pool if ua != self._active_user_agent]
            self._active_user_agent = random.choice(choices) if choices else self._active_user_agent
        self.session.headers["user-agent"] = self._active_user_agent

    def _maybe_warmup(self) -> None:
        if self.warmup_interval_seconds <= 0:
            return
        now = time.monotonic()
        if now - self._last_warmup_at < self.warmup_interval_seconds:
            return
        self._last_warmup_at = now
        try:
            self.session.get(
                f"{self.base_url}/",
                headers={"referer": f"{self.base_url}/"},
                timeout=max(5, min(self.timeout, 10)),
            )
        except Exception:
            # Warmup is best-effort only.
            pass

    def _request(
        self,
        path: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        final_headers = headers or {}
        last_exc: Optional[Exception] = None

        for attempt in range(1, self.retry_attempts + 1):
            self._apply_rate_limit()
            self._maybe_warmup()
            try:
                response = self.session.get(
                    url,
                    params=params,
                    headers=final_headers,
                    timeout=self.timeout,
                )
                if response.status_code in (403, 429):
                    raise requests.HTTPError(
                        f"Blocked with status {response.status_code}",
                        response=response,
                    )
                response.raise_for_status()
                return response
            except Exception as exc:
                last_exc = exc
                blocked = False
                if isinstance(exc, requests.HTTPError) and exc.response is not None:
                    blocked = exc.response.status_code in (403, 429, 503)
                if blocked:
                    self._rotate_user_agent()
                if attempt >= self.retry_attempts:
                    break
                sleep_s = self.retry_backoff_seconds * (2 ** (attempt - 1))
                sleep_s += random.uniform(0, self.retry_jitter_seconds)
                logger.warning(
                    f"Titan request retry {attempt}/{self.retry_attempts} path={path} "
                    f"reason={type(exc).__name__} sleep={sleep_s:.2f}s"
                )
                time.sleep(sleep_s)

        assert last_exc is not None
        raise last_exc

    def fetch_goal3_xml(self, flesh: Optional[str] = None, cookies: str | dict[str, str] | None = None) -> str:
        """Fetch odds snapshot XML from /txt/goal3.xml."""
        if flesh is None:
            flesh = f"{random.random():.13f}"

        headers = {"referer": f"{self.base_url}/"}
        cookie_header = _cookie_header(cookies)
        if cookie_header:
            headers["cookie"] = cookie_header

        response = self._request(
            "/txt/goal3.xml",
            params={"flesh": flesh},
            headers=headers,
        )
        return response.text

    def fetch_schedule_text(
        self,
        score_type: int = 0,
        language: int = 0,
        cookies: str | dict[str, str] | None = None,
    ) -> str:
        """Fetch schedule text feed used by mobile site."""
        headers = {"referer": f"{self.base_url}/"}
        cookie_header = _cookie_header(cookies)
        if cookie_header:
            headers["cookie"] = cookie_header

        response = self._request(
            f"/phone/Schedule_{language}_{score_type}.txt",
            headers=headers,
        )
        return response.text

    def fetch_handicap_companies(
        self,
        scheid: int,
        oddskind: int = 0,
        is_half: int = 0,
        type_: int = 1,
        cookies: str | dict[str, str] | None = None,
    ) -> dict:
        """Fetch company list + latest odds snapshots from HandicapDataInterface (type=1)."""
        headers = {"referer": f"{self.base_url}/asian/{scheid}.htm"}
        cookie_header = _cookie_header(cookies)
        if cookie_header:
            headers["cookie"] = cookie_header

        response = self._request(
            "/HandicapDataInterface.ashx",
            params={
                "scheid": scheid,
                "type": type_,
                "oddskind": oddskind,
                "isHalf": is_half,
            },
            headers=headers,
        )
        return response.json()

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

        response = self._request(
            "/HandicapDataInterface.ashx",
            params={
                "scheid": scheid,
                "type": type_,
                "oddskind": oddskind,
                "oddsid": oddsid,
                "isHalf": is_half,
                "flesh": flesh,
            },
            headers=headers,
        )
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


def _map_match_state(state: int) -> str:
    if state == 0:
        return "scheduled"
    if state == 2:
        return "halftime"
    if state > 0:
        return "live"
    return "finished"


def parse_schedule_matches(schedule_text: str) -> list[dict]:
    """Parse /phone/Schedule_0_0.txt into match rows."""
    parts = str(schedule_text or "").split("$$")
    if len(parts) < 2:
        return []

    sclass_data, match_data = parts[0], parts[1]
    league_map: dict[str, str] = {}
    for entry in sclass_data.split("!"):
        if not entry:
            continue
        cols = entry.split("^")
        if len(cols) < 2:
            continue
        league_map[cols[1]] = cols[0]

    matches: list[dict] = []
    for entry in match_data.split("!"):
        if not entry:
            continue
        cols = entry.split("^")
        if len(cols) < 11:
            continue

        scheid = _to_int(cols[0])
        sclass_id = cols[1]
        state = _to_int(cols[2]) or 0
        kickoff = _parse_compact_match_time(cols[3])
        if scheid is None or kickoff is None:
            continue

        home_score = _to_int(cols[7])
        away_score = _to_int(cols[8])
        ht_home = _to_int(cols[9])
        ht_away = _to_int(cols[10])
        home_red = _to_int(cols[11]) or 0
        away_red = _to_int(cols[12]) or 0

        status = _map_match_state(state)
        matches.append(
            {
                "id": str(scheid),
                "league": league_map.get(sclass_id, ""),
                "home_team": cols[5] if len(cols) > 5 else "",
                "away_team": cols[6] if len(cols) > 6 else "",
                "kickoff_time": kickoff.isoformat(sep=" ", timespec="seconds"),
                "status": status,
                "ht_home": ht_home,
                "ht_away": ht_away,
                "ft_home": home_score if status == "finished" else None,
                "ft_away": away_score if status == "finished" else None,
                "home_score": home_score,
                "away_score": away_score,
                "home_red": home_red,
                "away_red": away_red,
                "match_state_raw": state,
                "sclass_id": sclass_id,
                "raw_columns": cols,
            }
        )

    return matches


def resolve_bet365_oddsid(companies_payload: dict, prefer_num: Optional[int] = 4) -> Optional[int]:
    """Resolve bet365 oddsId from type=1 company payload."""
    companies = companies_payload.get("companies", [])
    if not isinstance(companies, list):
        return None

    def _is_bet365(company: dict) -> bool:
        company_id = _to_int(company.get("companyId"))
        name = str(company.get("nameCn") or company.get("nameEn") or company.get("name") or "").lower()
        if "365" in name:
            return True
        # Titan mobile often masks company names, e.g. "36*".
        if name.startswith("36"):
            return True
        if company_id == 8:
            return True
        return False

    for company in companies:
        if not _is_bet365(company):
            continue

        details = company.get("details", [])
        if not isinstance(details, list) or not details:
            continue

        selected = None
        if prefer_num is not None:
            for row in details:
                if _to_int(row.get("num")) == int(prefer_num):
                    selected = row
                    break
        if selected is None:
            for row in details:
                if _to_int(row.get("num")) == 1:
                    selected = row
                    break
        if selected is None:
            # Prefer the latest/sub-line by the biggest num.
            selected = sorted(details, key=lambda x: _to_int(x.get("num")) or 0, reverse=True)[0]
        if selected is None:
            selected = details[0]
        odds_id = _to_int(selected.get("oddsId"))
        if odds_id is not None:
            return odds_id
    return None


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
