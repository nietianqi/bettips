"""Handicap string normalization helpers."""

from __future__ import annotations


def _parse_single(value: str) -> float:
    return float(value.strip())


def _parse_fraction(value: str) -> float:
    parts = value.split("/", 1)
    if len(parts) == 2:
        return (_parse_single(parts[0]) + _parse_single(parts[1])) / 2
    return _parse_single(value)


_CN_TOKEN_DEPTH = {
    "平手": 0.0,
    "平": 0.0,
    "半球": 0.5,
    "半": 0.5,
    "一球": 1.0,
    "一": 1.0,
    "球半": 1.5,
    "一球半": 1.5,
    "两球": 2.0,
    "两": 2.0,
    "两球半": 2.5,
    "三球": 3.0,
    "三": 3.0,
}


def _parse_cn_token(text: str) -> float:
    text = text.strip()
    if text in _CN_TOKEN_DEPTH:
        return _CN_TOKEN_DEPTH[text]
    raise ValueError(text)


def _parse_cn_handicap(raw: str) -> tuple[float, bool]:
    text = raw.strip().replace(" ", "")
    receives = text.startswith("+") or text.startswith("受") or "受让" in text

    if text.startswith("+") or text.startswith("-"):
        text = text[1:]
    if text.startswith("受让"):
        text = text[2:]
    elif text.startswith("受"):
        text = text[1:]
    elif text.startswith("让"):
        text = text[1:]

    if "/" in text:
        left, right = text.split("/", 1)
        depth = (_parse_cn_token(left) + _parse_cn_token(right)) / 2
    else:
        depth = _parse_cn_token(text)

    if depth == 0:
        return 0.0, True
    return depth, (not receives)


def parse_handicap(raw: str) -> tuple[float, bool]:
    """Parse handicap text into ``(depth, home_gives)``."""
    raw = raw.strip()
    if not raw:
        raise ValueError("Empty handicap string")

    if any(ord(ch) > 127 for ch in raw):
        try:
            return _parse_cn_handicap(raw)
        except ValueError:
            pass

    raw_stripped = raw.lstrip("+")
    if raw_stripped.startswith("-"):
        home_gives = True
        numeric_part = raw_stripped.lstrip("-")
    else:
        home_gives = False
        numeric_part = raw_stripped

    try:
        depth = _parse_fraction(numeric_part) if "/" in numeric_part else _parse_single(numeric_part)
    except ValueError as exc:
        raise ValueError(f"Unable to parse handicap string: {raw!r}") from exc

    if depth == 0.0:
        home_gives = True

    return depth, home_gives


def normalize_line(raw: str) -> float:
    """Return signed depth: positive=home gives, negative=away gives."""
    depth, home_gives = parse_handicap(raw)
    return depth if home_gives else -depth


def is_deep_main_line(line_depth: float, threshold: float = 1.0) -> bool:
    """Check if handicap depth reaches deep-line threshold."""
    return line_depth >= threshold
