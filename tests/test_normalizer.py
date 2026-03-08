"""Unit tests for handicap normalization."""

import pytest

from src.normalizer import is_deep_main_line, normalize_line, parse_handicap


def test_parse_numeric_home_gives():
    depth, home_gives = parse_handicap("-1/1.5")
    assert depth == pytest.approx(1.25)
    assert home_gives is True


def test_parse_numeric_away_gives():
    depth, home_gives = parse_handicap("0.5/1")
    assert depth == pytest.approx(0.75)
    assert home_gives is False


def test_parse_level_ball():
    depth, home_gives = parse_handicap("0")
    assert depth == 0
    assert home_gives is True


def test_parse_cn_receive_one():
    depth, home_gives = parse_handicap("受让一")
    assert depth == pytest.approx(1.0)
    assert home_gives is False


def test_parse_cn_give_one():
    depth, home_gives = parse_handicap("一球")
    assert depth == pytest.approx(1.0)
    assert home_gives is True


def test_parse_cn_receive_quarter():
    depth, home_gives = parse_handicap("受半/一")
    assert depth == pytest.approx(0.75)
    assert home_gives is False


def test_invalid_handicap():
    with pytest.raises(ValueError):
        parse_handicap("abc")


def test_normalize_line_sign():
    assert normalize_line("-1") == pytest.approx(1.0)
    assert normalize_line("0.5") == pytest.approx(-0.5)


def test_is_deep_main_line():
    assert is_deep_main_line(1.0) is True
    assert is_deep_main_line(0.75) is False
