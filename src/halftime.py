"""
半场确认引擎

对候选池里的比赛，在半场时检查：
  1. 上半场比分是否 0:0
  2. 上半场是否有红牌
满足则触发提醒。
"""

from loguru import logger

from src import storage, alert


def check_candidate(db_path: str, match: dict) -> bool:
    """
    检查单场候选比赛的半场状态。

    Args:
        db_path: 数据库路径
        match: 包含 match_id, ht_home, ht_away, status 等字段的比赛dict

    Returns:
        True = 已触发提醒
    """
    match_id = match["id"]
    status = match.get("status", "")

    if status != "halftime":
        return False

    ht_home = match.get("ht_home")
    ht_away = match.get("ht_away")

    if ht_home is None or ht_away is None:
        logger.debug(f"[{match_id}] 半场比分尚未获取")
        return False

    # 条件1：必须 0:0
    if ht_home != 0 or ht_away != 0:
        logger.info(
            f"[{match_id}] 半场 {ht_home}:{ht_away}，不满足 0:0，从候选池移除"
        )
        storage.update_candidate_status(db_path, match_id, "dismissed")
        return False

    # 条件2：上半场无红牌
    events = storage.get_events(db_path, match_id)
    red_cards = [e for e in events if e["event_type"] == "red_card"]
    if red_cards:
        logger.info(f"[{match_id}] 上半场有红牌，从候选池移除")
        storage.update_candidate_status(db_path, match_id, "dismissed")
        return False

    # 满足条件 → 触发提醒
    alert.send_ht_alert(match)
    storage.update_candidate_status(db_path, match_id, "alerted")
    return True


def run_halftime_check(db_path: str) -> list[str]:
    """
    检查所有 watching 状态的候选比赛。

    Returns:
        已触发提醒的 match_id 列表
    """
    candidates = storage.get_live_candidates(db_path)
    alerted = []

    for match in candidates:
        if check_candidate(db_path, match):
            alerted.append(match["id"])

    return alerted
