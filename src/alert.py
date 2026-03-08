"""
提醒接口

当前模式：本地日志输出
预留接口：Telegram 推送（配置 mode=telegram 后启用）
"""

import json
from datetime import datetime
from typing import Optional

from loguru import logger


def _format_ht_alert(match: dict) -> str:
    """构造提醒文本。"""
    home = match.get("home_team", "?")
    away = match.get("away_team", "?")
    league = match.get("league", "?")
    kickoff = match.get("kickoff_time", "?")
    trigger_depth = match.get("trigger_depth", "?")
    prev_depth = match.get("prev_depth", "?")
    upgrade_ts = match.get("upgrade_ts", "?")

    ht_home = match.get("ht_home", 0)
    ht_away = match.get("ht_away", 0)

    lines = [
        "=" * 40,
        "【HT大1候选】",
        f"  联赛: {league}",
        f"  对阵: {home} vs {away}",
        f"  开球: {kickoff}",
        f"  盘口路径: 主让{prev_depth} → 升至{trigger_depth}（首次出现）",
        f"  升盘时间: {upgrade_ts}",
        f"  半场比分: {ht_home}:{ht_away}",
        f"  红牌: 无",
        "  建议关注: 下半场大1",
        "=" * 40,
    ]
    return "\n".join(lines)


def send_ht_alert(match: dict, config: Optional[dict] = None) -> None:
    """
    发送 HT大1候选 提醒。

    Args:
        match: 比赛信息 + 候选信息的合并dict
        config: alert配置（mode, telegram_token, telegram_chat_id）
    """
    text = _format_ht_alert(match)

    mode = "log"
    if config:
        mode = config.get("mode", "log")

    if mode == "log":
        logger.success(text)

    elif mode == "telegram":
        _send_telegram(text, config)

    else:
        logger.warning(f"未知提醒模式: {mode}，降级为日志输出")
        logger.success(text)


def _send_telegram(text: str, config: dict) -> None:
    """发送Telegram消息（预留实现）。"""
    token = config.get("telegram_token", "")
    chat_id = config.get("telegram_chat_id", "")

    if not token or not chat_id:
        logger.warning("Telegram token 或 chat_id 未配置，跳过推送")
        logger.success(text)
        return

    try:
        import urllib.request
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                logger.info("Telegram提醒发送成功")
            else:
                logger.error(f"Telegram返回错误: {result}")
    except Exception as e:
        logger.error(f"Telegram发送失败: {e}，降级为日志输出")
        logger.success(text)


def send_info(message: str) -> None:
    """发送普通信息日志（非候选提醒）。"""
    logger.info(f"[INFO] {message}")
