"""Time parsing helpers used across collector/scanner/storage."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

_FALLBACK_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y-%m-%d",
)


def _from_epoch(value: float) -> datetime:
    if abs(value) > 1_000_000_000_000:
        value /= 1000.0
    return datetime.fromtimestamp(value, tz=timezone.utc).replace(tzinfo=None)


def parse_datetime(value: Any) -> Optional[datetime]:
    """Parse different datetime encodings into naive UTC datetime."""
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    if isinstance(value, (int, float)):
        return _from_epoch(float(value))

    text = str(value).strip()
    if not text:
        return None

    # Epoch string.
    if text.lstrip("-").isdigit():
        return _from_epoch(float(text))

    # Common timezone notations.
    candidate = text
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    candidate = candidate.replace(" UTC", "+00:00")

    # ISO parser first.
    try:
        dt = datetime.fromisoformat(candidate)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        pass

    for fmt in _FALLBACK_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    return None


def to_utc_iso(value: Any) -> str:
    """Convert value to a sqlite friendly UTC datetime string."""
    parsed = parse_datetime(value)
    if parsed is None:
        return ""
    return parsed.isoformat(sep=" ", timespec="seconds")
