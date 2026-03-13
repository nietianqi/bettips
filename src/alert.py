"""Alert delivery adapters."""

from __future__ import annotations

import json
import urllib.request
from typing import Any, Optional

from loguru import logger


def _format_ht_alert(match: dict[str, Any]) -> str:
    """Build the halftime alert message."""
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
        f"  盘口路径(盘4): {prev_depth} → {trigger_depth}（临场首次出现）",
        f"  升盘时间: {upgrade_ts}",
        f"  半场比分: {ht_home}:{ht_away}",
        "  红牌条件: 不限制",
        "  建议关注: 下半场大1",
        "=" * 40,
    ]
    return "\n".join(lines)


def send_ht_alert(match: dict[str, Any], config: Optional[dict[str, Any]] = None) -> None:
    """Send HT over-1 candidate alerts."""
    text = _format_ht_alert(match)
    cfg = config or {}
    mode = str(cfg.get("mode", "log")).strip().lower()

    if mode == "log":
        logger.success(text)
    elif mode == "telegram":
        _send_telegram(text, cfg)
    elif mode == "feishu":
        _send_feishu(text, cfg)
    else:
        logger.warning(f"未知提醒模式: {mode}，降级为日志输出")
        logger.success(text)


def _post_json(url: str, payload: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    if not raw:
        return {}
    return json.loads(raw)


def _send_telegram(text: str, config: dict[str, Any]) -> None:
    """Send Telegram message."""
    token = str(config.get("telegram_token", "")).strip()
    chat_id = str(config.get("telegram_chat_id", "")).strip()

    if not token or not chat_id:
        logger.warning("Telegram token 或 chat_id 未配置，跳过推送")
        logger.success(text)
        return

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        result = _post_json(
            url=url,
            payload={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
            },
        )
        if result.get("ok"):
            logger.info("Telegram提醒发送成功")
            return
        logger.error(f"Telegram返回错误: {result}")
    except Exception as exc:
        logger.error(f"Telegram发送失败: {exc}，降级为日志输出")

    logger.success(text)


def _send_feishu(text: str, config: dict[str, Any]) -> None:
    """Send Feishu bot webhook message."""
    webhook = str(config.get("feishu_webhook", "")).strip()
    if not webhook:
        logger.warning("飞书 webhook 未配置，跳过推送")
        logger.success(text)
        return

    try:
        result = _post_json(
            url=webhook,
            payload={
                "msg_type": "text",
                "content": {"text": text},
            },
        )
        if int(result.get("code", -1)) == 0:
            logger.info("飞书提醒发送成功")
            return
        logger.error(f"飞书返回错误: {result}")
    except Exception as exc:
        logger.error(f"飞书发送失败: {exc}，降级为日志输出")

    logger.success(text)


def send_info(message: str) -> None:
    """Send non-candidate info log."""
    logger.info(f"[INFO] {message}")
