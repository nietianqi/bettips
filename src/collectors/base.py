"""
数据采集抽象基类

定义采集层必须实现的接口，便于后续替换数据源（球探/其他API）。
"""

from abc import ABC, abstractmethod


class BaseCollector(ABC):
    """所有数据采集器的抽象基类。"""

    @abstractmethod
    async def fetch_match_list(self) -> list[dict]:
        """
        获取即将开赛的比赛列表。

        Returns:
            list of match dicts，每条包含：
            {
                "id": str,
                "league": str,
                "home_team": str,
                "away_team": str,
                "kickoff_time": str,  # ISO格式UTC时间
                "status": str,        # scheduled | live | halftime | finished
            }
        """
        ...

    @abstractmethod
    async def fetch_odds_history(self, match_id: str) -> list[dict]:
        """
        获取指定比赛的 bet365 亚盘历史。

        Returns:
            list of odds dicts，每条包含：
            {
                "match_id": str,
                "bookmaker": str,      # "bet365"
                "line_depth": float,   # 标准化让球深度
                "home_gives": int,     # 1=主让，0=客让
                "home_odds": float,
                "away_odds": float,
                "ts": str,             # ISO格式UTC时间
            }
        """
        ...

    @abstractmethod
    async def fetch_live_data(self, match_id: str) -> dict:
        """
        获取比赛实时数据（比分+事件）。

        Returns:
            {
                "match_id": str,
                "status": str,           # live | halftime | finished
                "ht_home": int | None,
                "ht_away": int | None,
                "ft_home": int | None,
                "ft_away": int | None,
                "events": list[dict],    # [{minute, event_type, team}, ...]
            }
        """
        ...

    async def close(self) -> None:
        """释放资源（如关闭浏览器）。子类按需覆盖。"""
        pass
