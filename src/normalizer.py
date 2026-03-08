"""
盘口字符串标准化模块

把球探/bet365等网站的原始盘口字符串转成 (line_depth: float, home_gives: bool)。

line_depth = 主队让球深度（正数表示主让，值越大越深）
home_gives = True 表示主队让球，False 表示客队让球

示例：
  "-1"        → (1.0,  True)   主让1球
  "-1/1.5"    → (1.25, True)   主让1/1.5（中值）
  "-1.5"      → (1.5,  True)   主让1.5球
  "-1.5/2"    → (1.75, True)   主让1.5/2
  "0"         → (0.0,  True)   平手盘
  "0.5"       → (0.5,  False)  客让半球（正数=客让）
  "0.5/1"     → (0.75, False)  客让0.5/1
"""

import re


def _parse_single(s: str) -> float:
    """把单个数字字符串（含小数）转成float。"""
    return float(s.strip())


def _parse_fraction(s: str) -> float:
    """
    把分数盘字符串转成中间值。
    支持格式：'1/1.5'、'0.5/1'、'1.5/2' 等。
    """
    parts = s.split("/")
    if len(parts) == 2:
        a, b = _parse_single(parts[0]), _parse_single(parts[1])
        return (a + b) / 2
    return _parse_single(s)


# 映射中文盘口描述
_CN_MAP = {
    "平手": 0.0,
    "受让半": -0.5,
    "受让半/一": -0.75,
    "受让一": -1.0,
    "受让一/球半": -1.25,
    "受让球半": -1.5,
    "受让球半/两": -1.75,
    "受让两球": -2.0,
}


def parse_handicap(raw: str) -> tuple[float, bool]:
    """
    解析盘口原始字符串。

    Args:
        raw: 原始盘口字符串，例如 "-1"、"-1/1.5"、"0.5/1"、"受让一" 等

    Returns:
        (line_depth, home_gives)
        - line_depth: 让球深度绝对值（浮点数，≥0）
        - home_gives: True=主队让球，False=客队让球
    """
    raw = raw.strip()

    # 中文盘口
    for cn, val in _CN_MAP.items():
        if cn in raw:
            depth = abs(val)
            home_gives = val <= 0  # 负数=主让
            return depth, home_gives

    # 数字盘口：先提取符号，再解析绝对值
    # 支持："-1"、"-1/1.5"、"0.5/1"、"+0.5"、"1.5"
    raw_stripped = raw.lstrip("+")

    if raw_stripped.startswith("-"):
        home_gives = True
        numeric_part = raw_stripped.lstrip("-")
    else:
        # 正数或无符号 → 客让（主队受让）
        home_gives = False
        numeric_part = raw_stripped

    if "/" in numeric_part:
        depth = _parse_fraction(numeric_part)
    else:
        try:
            depth = _parse_single(numeric_part)
        except ValueError:
            raise ValueError(f"无法解析盘口字符串: {raw!r}")

    # 平手盘（depth=0）统一返回 home_gives=True（无方向意义）
    if depth == 0.0:
        home_gives = True

    return depth, home_gives


def normalize_line(raw: str) -> float:
    """
    便捷函数：仅返回 line_depth（有向值：主让为正，客让为负）。
    用于比较升盘方向。
    """
    depth, home_gives = parse_handicap(raw)
    return depth if home_gives else -depth


def is_deep_main_line(line_depth: float, threshold: float = 1.0) -> bool:
    """判断主让盘口是否达到深盘标准（≥ threshold）。"""
    return line_depth >= threshold
