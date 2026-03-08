"""盘口标准化单元测试"""

import pytest
from src.normalizer import parse_handicap, normalize_line, is_deep_main_line


class TestParseHandicap:
    def test_home_gives_integer(self):
        depth, home_gives = parse_handicap("-1")
        assert depth == 1.0
        assert home_gives is True

    def test_home_gives_fraction(self):
        depth, home_gives = parse_handicap("-1/1.5")
        assert depth == pytest.approx(1.25)
        assert home_gives is True

    def test_home_gives_1_5(self):
        depth, home_gives = parse_handicap("-1.5")
        assert depth == 1.5
        assert home_gives is True

    def test_home_gives_1_5_2(self):
        depth, home_gives = parse_handicap("-1.5/2")
        assert depth == pytest.approx(1.75)
        assert home_gives is True

    def test_home_gives_2(self):
        depth, home_gives = parse_handicap("-2")
        assert depth == 2.0
        assert home_gives is True

    def test_away_gives_half(self):
        depth, home_gives = parse_handicap("0.5")
        assert depth == 0.5
        assert home_gives is False

    def test_away_gives_fraction(self):
        depth, home_gives = parse_handicap("0.5/1")
        assert depth == pytest.approx(0.75)
        assert home_gives is False

    def test_level_ball(self):
        depth, home_gives = parse_handicap("0")
        assert depth == 0.0

    def test_home_gives_half(self):
        depth, home_gives = parse_handicap("-0.5")
        assert depth == 0.5
        assert home_gives is True

    def test_home_gives_0_5_1(self):
        depth, home_gives = parse_handicap("-0.5/1")
        assert depth == pytest.approx(0.75)
        assert home_gives is True

    def test_chinese_home_gives_1(self):
        depth, home_gives = parse_handicap("受让一")
        assert depth == 1.0
        assert home_gives is True

    def test_chinese_level(self):
        depth, home_gives = parse_handicap("平手")
        assert depth == 0.0

    def test_invalid_string(self):
        with pytest.raises(ValueError):
            parse_handicap("abc")

    def test_whitespace_stripped(self):
        depth, home_gives = parse_handicap("  -1.5  ")
        assert depth == 1.5
        assert home_gives is True


class TestNormalizeLine:
    def test_home_gives_positive(self):
        assert normalize_line("-1") == pytest.approx(1.0)

    def test_away_gives_negative(self):
        assert normalize_line("0.5") == pytest.approx(-0.5)

    def test_fraction(self):
        assert normalize_line("-1/1.5") == pytest.approx(1.25)


class TestIsDeepMainLine:
    def test_exactly_1(self):
        assert is_deep_main_line(1.0) is True

    def test_above_1(self):
        assert is_deep_main_line(1.5) is True

    def test_below_1(self):
        assert is_deep_main_line(0.75) is False

    def test_custom_threshold(self):
        assert is_deep_main_line(1.5, threshold=1.5) is True
        assert is_deep_main_line(1.25, threshold=1.5) is False
