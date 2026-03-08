"""
赛前规则引擎

核心职责：
1. 判断主盘口是否达到深盘标准（≥ 1.0）
2. 识别"临场前15分钟首次升深"
3. 对即将开赛的比赛做全量扫描，产出候选池
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from loguru import logger

from src.normalizer import is_deep_main_line
from src import storage


def has_first_time_late_upgrade(
    history: list[dict],
    kickoff: datetime,
    window_minutes: int = 15,
) -> tuple[bool, Optional[dict]]:
    """
    在开赛前 window_minutes 分钟内，找到 bet365 首次出现的盘口升深。

    "升深" = 新记录的 line_depth > 前一条记录的 line_depth
    "首次" = 该新深度在此之前从未在历史记录中出现过

    Args:
        history: 已按时间排序的盘口记录列表，每条包含 'ts'(datetime) 和 'line_depth'(float)
        kickoff: 开赛时间（UTC）
        window_minutes: 临场观察窗口（分钟）

    Returns:
        (True, 触发记录) 或 (False, None)

    示例（不满足）：
        12:00 → 1.0, 14:00 → 1.25, 17:30 → 1.0, 19:50 → 1.25  开球20:00
        19:50的1.25不是首次出现（14:00已出现）→ 不满足

    示例（满足）：
        12:00 → 1.0, 14:00 → 1.0, 19:50 → 1.25  开球20:00
        19:50的1.25是首次出现 → 满足
    """
    if not history:
        return False, None

    # 确保 ts 是 datetime 对象
    def _to_dt(v) -> datetime:
        if isinstance(v, datetime):
            return v
        return datetime.fromisoformat(str(v))

    sorted_h = sorted(history, key=lambda x: _to_dt(x["ts"]))

    # 确保 kickoff 是 naive datetime（UTC）
    if kickoff.tzinfo is not None:
        kickoff = kickoff.replace(tzinfo=None)

    late_start = kickoff - timedelta(minutes=window_minutes)

    for i in range(1, len(sorted_h)):
        prev = sorted_h[i - 1]
        curr = sorted_h[i]

        curr_ts = _to_dt(curr["ts"])
        if curr_ts.tzinfo is not None:
            curr_ts = curr_ts.replace(tzinfo=None)

        # 必须在临场窗口内
        if not (late_start <= curr_ts <= kickoff):
            continue

        # 必须是升深
        if curr["line_depth"] <= prev["line_depth"]:
            continue

        # 该深度在此前从未出现
        appeared_before = any(
            abs(h["line_depth"] - curr["line_depth"]) < 1e-9
            and _to_dt(h["ts"]).replace(tzinfo=None) < curr_ts
            for h in sorted_h
        )

        if not appeared_before:
            logger.debug(
                f"首次升深: {prev['line_depth']} → {curr['line_depth']} @ {curr_ts}"
            )
            return True, {**curr, "ts": curr_ts}

    return False, None


def scan_match(
    db_path: str,
    match: dict,
    bookmaker: str = "bet365",
    min_depth: float = 1.0,
    window_minutes: int = 15,
) -> Optional[dict]:
    """
    对单场比赛执行赛前扫描规则。

    Returns:
        候选字典（含 match_id, trigger_depth, prev_depth, upgrade_ts）或 None
    """
    match_id = match["id"]
    kickoff_raw = match["kickoff_time"]
    kickoff = (
        datetime.fromisoformat(str(kickoff_raw))
        if isinstance(kickoff_raw, str)
        else kickoff_raw
    )
    if kickoff.tzinfo is not None:
        kickoff = kickoff.replace(tzinfo=None)

    # 已是候选，跳过
    if storage.is_candidate(db_path, match_id):
        return None

    history = storage.get_odds_history(db_path, match_id, bookmaker)
    if not history:
        logger.debug(f"[{match_id}] 暂无盘口历史，跳过")
        return None

    # 获取最新主盘口深度
    latest = max(history, key=lambda x: x["ts"])
    current_depth = latest["line_depth"]
    home_gives = bool(latest["home_gives"])

    # 条件1：必须是主让球，且深度 ≥ min_depth
    if not home_gives:
        logger.debug(f"[{match_id}] 客让盘，跳过")
        return None

    if not is_deep_main_line(current_depth, min_depth):
        logger.debug(f"[{match_id}] 主盘口 {current_depth} < {min_depth}，跳过")
        return None

    # 条件2：临场前 window_minutes 分钟首次升深
    triggered, upgrade_record = has_first_time_late_upgrade(history, kickoff, window_minutes)
    if not triggered:
        logger.debug(f"[{match_id}] 未发现首次升深")
        return None

    home = match.get("home_team", "?")
    away = match.get("away_team", "?")
    league = match.get("league", "?")
    logger.info(
        f"[赛前候选] {league} | {home} vs {away} | "
        f"主让: {upgrade_record['line_depth']} (从{upgrade_record.get('prev_depth', '?')}升入，首次出现)"
    )

    return {
        "match_id": match_id,
        "trigger_depth": upgrade_record["line_depth"],
        "prev_depth": upgrade_record.get("prev_depth", None),
        "upgrade_ts": upgrade_record["ts"].isoformat()
        if isinstance(upgrade_record["ts"], datetime)
        else str(upgrade_record["ts"]),
    }


def run_pre_match_scan(
    db_path: str,
    bookmaker: str = "bet365",
    min_depth: float = 1.0,
    window_minutes: int = 15,
    scan_window: int = 90,
) -> list[dict]:
    """
    扫描即将开赛比赛（接下来 scan_window 分钟内），
    返回所有新发现的候选并写入数据库。
    """
    upcoming = storage.get_upcoming_matches(db_path, within_minutes=scan_window)
    new_candidates = []

    for match in upcoming:
        result = scan_match(db_path, match, bookmaker, min_depth, window_minutes)
        if result:
            storage.add_candidate(db_path, result)
            new_candidates.append(result)

    if new_candidates:
        logger.info(f"本轮扫描新增候选: {len(new_candidates)} 场")
    return new_candidates
