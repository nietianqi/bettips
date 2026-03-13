"""Tests for alert delivery adapters."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

from src import alert


class _DummyResponse:
    def __init__(self, payload: dict[str, Any]):
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def _sample_match() -> dict[str, Any]:
    return {
        "id": "2950976",
        "league": "欧罗巴杯",
        "home_team": "主队",
        "away_team": "客队",
        "kickoff_time": "2026-03-13 01:45:00",
        "trigger_depth": 1.25,
        "prev_depth": 1.0,
        "upgrade_ts": "2026-03-13 01:30:00",
        "ht_home": 0,
        "ht_away": 0,
    }


def test_send_feishu_webhook_payload(monkeypatch):
    captured: dict[str, Any] = {}

    def fake_urlopen(req, timeout=10):  # noqa: ANN001
        captured["url"] = req.full_url
        captured["timeout"] = timeout
        captured["payload"] = json.loads(req.data.decode("utf-8"))
        return _DummyResponse({"code": 0, "msg": "ok"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    alert.send_ht_alert(
        _sample_match(),
        {
            "mode": "feishu",
            "feishu_webhook": "https://open.feishu.cn/open-apis/bot/v2/hook/test",
        },
    )

    assert captured["url"].endswith("/test")
    assert captured["timeout"] == 10
    assert captured["payload"]["msg_type"] == "text"
    text = captured["payload"]["content"]["text"]
    assert "HT大1候选" in text
    assert "主队" in text and "客队" in text


def test_send_feishu_without_webhook_fallback_log(monkeypatch):
    class _DummyLogger:
        def __init__(self) -> None:
            self.success_calls = 0

        def success(self, _message: str) -> None:
            self.success_calls += 1

        def warning(self, _message: str) -> None:
            return

        def info(self, _message: str) -> None:
            return

        def error(self, _message: str) -> None:
            return

    calls = {"urlopen": 0}
    dummy_logger = _DummyLogger()

    def fake_urlopen(_req, timeout=10):  # noqa: ANN001
        calls["urlopen"] += 1
        return _DummyResponse({"code": 0, "msg": "ok"})

    monkeypatch.setattr(alert, "logger", dummy_logger)
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    alert.send_ht_alert(_sample_match(), {"mode": "feishu", "feishu_webhook": ""})

    assert calls["urlopen"] == 0
    assert dummy_logger.success_calls == 1
