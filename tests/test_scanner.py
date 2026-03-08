"""首次升深算法单元测试"""

import pytest
from datetime import datetime, timedelta
from src.scanner import has_first_time_late_upgrade


def _dt(offset_minutes: int, base: datetime = None) -> datetime:
    """从基准时间偏移 offset_minutes 分钟。"""
    if base is None:
        base = datetime(2026, 3, 8, 20, 0, 0)  # 假设20:00开球
    return base + timedelta(minutes=offset_minutes)


KICKOFF = datetime(2026, 3, 8, 20, 0, 0)


def _record(depth: float, offset_minutes: int) -> dict:
    return {"line_depth": depth, "ts": _dt(offset_minutes, KICKOFF - timedelta(hours=8))}


# 相对于 KICKOFF 的偏移（负数=开球前）
def _rec(depth: float, minutes_before_kickoff: int) -> dict:
    ts = KICKOFF - timedelta(minutes=minutes_before_kickoff)
    return {"line_depth": depth, "ts": ts}


class TestFirstTimeLateUpgrade:
    def test_basic_satisfied(self):
        """基础满足：临场15分钟内首次出现更深盘口。"""
        history = [
            _rec(1.0, 480),  # 8小时前：1.0
            _rec(1.0, 60),   # 1小时前：1.0
            _rec(1.25, 10),  # 10分钟前：1.25（首次出现）← 满足
        ]
        triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
        assert triggered is True
        assert rec["line_depth"] == pytest.approx(1.25)

    def test_not_first_time(self):
        """不满足：该深度之前已经出现过。"""
        history = [
            _rec(1.0, 480),
            _rec(1.25, 240),  # 4小时前已出现1.25
            _rec(1.0, 60),
            _rec(1.25, 10),   # 10分钟前再次出现1.25 ← 不满足（已出现过）
        ]
        triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
        assert triggered is False

    def test_upgrade_outside_window(self):
        """不满足：升深发生在15分钟窗口之外。"""
        history = [
            _rec(1.0, 480),
            _rec(1.25, 30),  # 30分钟前升盘（窗口外）← 不满足
        ]
        triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
        assert triggered is False

    def test_no_upgrade_only_flat(self):
        """不满足：盘口没有变化。"""
        history = [
            _rec(1.0, 480),
            _rec(1.0, 30),
            _rec(1.0, 10),
        ]
        triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
        assert triggered is False

    def test_downgrade_then_upgrade_first_time(self):
        """满足：先降后升，且升到的盘口是首次出现。"""
        history = [
            _rec(1.25, 480),  # 很早出现1.25
            _rec(1.0, 60),    # 下降到1.0
            _rec(1.5, 12),    # 12分钟前升到1.5（首次出现）← 满足
        ]
        triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
        assert triggered is True
        assert rec["line_depth"] == pytest.approx(1.5)

    def test_downgrade_then_back_to_seen_depth(self):
        """不满足：升回的深度之前已出现。"""
        history = [
            _rec(1.25, 480),  # 很早出现1.25
            _rec(1.0, 60),    # 下降
            _rec(1.25, 12),   # 12分钟前回到1.25，但1.25已出现 ← 不满足
        ]
        triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
        assert triggered is False

    def test_empty_history(self):
        """边界：无历史数据。"""
        triggered, rec = has_first_time_late_upgrade([], KICKOFF)
        assert triggered is False

    def test_single_record(self):
        """边界：只有一条记录。"""
        history = [_rec(1.0, 10)]
        triggered, rec = has_first_time_late_upgrade(history, KICKOFF)
        assert triggered is False

    def test_custom_window(self):
        """自定义窗口：升盘在20分钟前，用20分钟窗口应满足。"""
        history = [
            _rec(1.0, 480),
            _rec(1.25, 18),  # 18分钟前
        ]
        # 默认15分钟窗口 → 不满足
        triggered, _ = has_first_time_late_upgrade(history, KICKOFF, window_minutes=15)
        assert triggered is False
        # 20分钟窗口 → 满足
        triggered, _ = has_first_time_late_upgrade(history, KICKOFF, window_minutes=20)
        assert triggered is True

    def test_string_ts(self):
        """支持 ts 为字符串格式（ISO格式）。"""
        history = [
            {"line_depth": 1.0, "ts": "2026-03-08 12:00:00"},
            {"line_depth": 1.25, "ts": "2026-03-08 19:50:00"},
        ]
        kickoff = datetime(2026, 3, 8, 20, 0, 0)
        triggered, rec = has_first_time_late_upgrade(history, kickoff)
        assert triggered is True
