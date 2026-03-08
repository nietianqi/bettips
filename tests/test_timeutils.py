"""Unit tests for datetime parsing helpers."""

from datetime import datetime, timezone

from src.timeutils import parse_datetime, to_utc_iso


def test_parse_datetime_iso_z():
    dt = parse_datetime("2026-03-08T12:00:00Z")
    assert dt == datetime(2026, 3, 8, 12, 0, 0)


def test_parse_datetime_epoch_ms():
    epoch_ms = int(datetime(2026, 3, 8, 6, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    dt = parse_datetime(epoch_ms)
    assert dt == datetime(2026, 3, 8, 6, 0, 0)


def test_parse_datetime_invalid():
    assert parse_datetime("not-a-date") is None


def test_to_utc_iso():
    assert to_utc_iso("2026-03-08T20:00:01Z") == "2026-03-08 20:00:01"
