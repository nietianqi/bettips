"""
bettips 主入口 / 调度器

APScheduler 调度三个任务：
  1. collect_odds    每60秒  为即将开赛比赛抓取盘口历史和比分
  2. pre_match_scan  每60秒  筛选满足条件的候选比赛
  3. halftime_check  每120秒 检查候选比赛半场状态

使用：
  python main.py
"""

import asyncio
import sys
from pathlib import Path

import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

from src import storage
from src.collectors.qiutan import QiutanCollector
from src import scanner, halftime


# ── 配置加载 ──────────────────────────────────────────────────────────────────

def load_config(path: str = "config.yaml") -> dict:
    cfg_path = Path(path)
    if not cfg_path.exists():
        logger.error(f"配置文件不存在: {path}")
        sys.exit(1)
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── 全局状态 ──────────────────────────────────────────────────────────────────

_collector: QiutanCollector = None
_config: dict = {}
_db_path: str = "bettips.db"


# ── 任务函数 ──────────────────────────────────────────────────────────────────

async def collect_odds():
    """
    任务1：抓取即将开赛比赛的盘口历史和实时比分，存入数据库。
    """
    try:
        scan_window = _config.get("qiutan", {}).get("scan_window_minutes", 90)

        # 获取比赛列表
        matches = await _collector.fetch_match_list()
        for match in matches:
            storage.upsert_match(_db_path, match)

        # 获取已入库的即将开赛比赛
        upcoming = storage.get_upcoming_matches(_db_path, within_minutes=scan_window)
        # 加上候选比赛（可能已处于 live/halftime 状态）
        candidates = storage.get_live_candidates(_db_path)
        candidate_ids = {c["id"] for c in candidates}

        all_targets = {m["id"]: m for m in upcoming}
        for c in candidates:
            all_targets[c["id"]] = c

        logger.debug(f"本轮需要抓取盘口/比分的比赛数: {len(all_targets)}")

        for match_id in all_targets:
            # 抓赔率历史
            odds = await _collector.fetch_odds_history(match_id)
            for o in odds:
                try:
                    storage.insert_odds(_db_path, o)
                except Exception:
                    pass  # 忽略重复插入

            # 抓实时比分
            live = await _collector.fetch_live_data(match_id)
            if live:
                update = {
                    "id": match_id,
                    "league": all_targets[match_id].get("league", ""),
                    "home_team": all_targets[match_id].get("home_team", ""),
                    "away_team": all_targets[match_id].get("away_team", ""),
                    "kickoff_time": all_targets[match_id].get("kickoff_time", ""),
                    "status": live.get("status", ""),
                    "ht_home": live.get("ht_home"),
                    "ht_away": live.get("ht_away"),
                    "ft_home": live.get("ft_home"),
                    "ft_away": live.get("ft_away"),
                }
                storage.upsert_match(_db_path, update)

                for ev in live.get("events", []):
                    try:
                        storage.insert_event(_db_path, ev)
                    except Exception:
                        pass

    except Exception as e:
        logger.error(f"collect_odds 任务异常: {e}")


async def run_pre_match_scan():
    """
    任务2：赛前规则扫描，筛出候选比赛。
    """
    try:
        cfg = _config.get("scanner", {})
        scanner.run_pre_match_scan(
            db_path=_db_path,
            bookmaker=cfg.get("bookmaker", "bet365"),
            min_depth=cfg.get("min_line_depth", 1.0),
            window_minutes=cfg.get("late_upgrade_window_minutes", 15),
            scan_window=_config.get("qiutan", {}).get("scan_window_minutes", 90),
        )
    except Exception as e:
        logger.error(f"pre_match_scan 任务异常: {e}")


async def run_halftime_check():
    """
    任务3：半场确认，触发提醒。
    """
    try:
        alerted = halftime.run_halftime_check(_db_path)
        if alerted:
            logger.info(f"本轮发出提醒: {len(alerted)} 场")
    except Exception as e:
        logger.error(f"halftime_check 任务异常: {e}")


# ── 主入口 ──────────────────────────────────────────────────────────────────

async def main():
    global _collector, _config, _db_path

    # 日志配置
    logger.remove()
    logger.add(sys.stdout, level="INFO", colorize=True,
               format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")
    logger.add("logs/bettips_{time:YYYY-MM-DD}.log", level="DEBUG", rotation="1 day", retention="7 days")

    # 加载配置
    _config = load_config()
    _db_path = _config.get("db", {}).get("path", "bettips.db")

    # 初始化数据库
    storage.init_db(_db_path)

    # 初始化采集器
    _collector = QiutanCollector(_config.get("qiutan", {}))

    # 检查接口配置
    qiutan_cfg = _config.get("qiutan", {})
    missing = [
        k for k in ("match_list_pattern", "odds_history_pattern", "live_score_pattern")
        if not qiutan_cfg.get(k)
    ]
    if missing:
        logger.warning(
            f"以下接口URL未配置: {missing}\n"
            "请先运行发现模式找到实际接口:\n"
            "  python -m src.collectors.qiutan discover https://www.qiutan.com/match/<ID>\n"
            "系统将继续运行，但采集功能不可用。"
        )

    # 启动调度器
    scheduler = AsyncIOScheduler()
    scheduler.add_job(collect_odds,       "interval", seconds=60,  id="collect_odds")
    scheduler.add_job(run_pre_match_scan, "interval", seconds=60,  id="pre_match_scan")
    scheduler.add_job(run_halftime_check, "interval", seconds=120, id="halftime_check")
    scheduler.start()

    logger.info("=" * 50)
    logger.info("bettips 启动成功")
    logger.info(f"数据库: {_db_path}")
    logger.info("调度任务: collect_odds(60s) | pre_match_scan(60s) | halftime_check(120s)")
    logger.info("=" * 50)

    # 立即执行一次
    await collect_odds()
    await run_pre_match_scan()
    await run_halftime_check()

    # 保持运行
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("收到退出信号，正在关闭...")
    finally:
        scheduler.shutdown()
        if _collector:
            await _collector.close()
        logger.info("bettips 已退出")


if __name__ == "__main__":
    asyncio.run(main())
