"""SQLite 数据库操作层"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger


def _schema_path() -> Path:
    return Path(__file__).parent.parent / "db" / "schema.sql"


@contextmanager
def _conn(db_path: str):
    con = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db(db_path: str) -> None:
    """初始化数据库，执行 schema.sql。"""
    schema = _schema_path().read_text(encoding="utf-8")
    with _conn(db_path) as con:
        con.executescript(schema)
    logger.info(f"数据库初始化完成: {db_path}")


# ── matches ──────────────────────────────────────────────────────────────────

def upsert_match(db_path: str, match: dict) -> None:
    sql = """
    INSERT INTO matches (id, league, home_team, away_team, kickoff_time, status,
                         ht_home, ht_away, ft_home, ft_away, updated_at)
    VALUES (:id, :league, :home_team, :away_team, :kickoff_time, :status,
            :ht_home, :ht_away, :ft_home, :ft_away, :updated_at)
    ON CONFLICT(id) DO UPDATE SET
        status=excluded.status,
        ht_home=excluded.ht_home, ht_away=excluded.ht_away,
        ft_home=excluded.ft_home, ft_away=excluded.ft_away,
        updated_at=excluded.updated_at
    """
    match.setdefault("updated_at", datetime.utcnow().isoformat())
    with _conn(db_path) as con:
        con.execute(sql, match)


def get_match(db_path: str, match_id: str) -> Optional[dict]:
    with _conn(db_path) as con:
        row = con.execute("SELECT * FROM matches WHERE id=?", (match_id,)).fetchone()
        return dict(row) if row else None


def get_upcoming_matches(db_path: str, within_minutes: int = 90) -> list[dict]:
    """返回接下来 within_minutes 分钟内即将开赛的比赛（status=scheduled）。"""
    now = datetime.utcnow().isoformat()
    cutoff = f"datetime('{now}', '+{within_minutes} minutes')"
    sql = f"""
    SELECT * FROM matches
    WHERE status = 'scheduled'
      AND kickoff_time >= '{now}'
      AND kickoff_time <= {cutoff}
    ORDER BY kickoff_time
    """
    with _conn(db_path) as con:
        rows = con.execute(sql).fetchall()
        return [dict(r) for r in rows]


def get_live_candidates(db_path: str) -> list[dict]:
    """返回仍在 watching 状态的候选比赛（用于半场确认）。"""
    sql = """
    SELECT m.*, c.trigger_depth, c.prev_depth, c.upgrade_ts
    FROM candidates c
    JOIN matches m ON m.id = c.match_id
    WHERE c.status = 'watching'
    """
    with _conn(db_path) as con:
        rows = con.execute(sql).fetchall()
        return [dict(r) for r in rows]


# ── odds_history ──────────────────────────────────────────────────────────────

def insert_odds(db_path: str, record: dict) -> None:
    sql = """
    INSERT INTO odds_history (match_id, bookmaker, line_depth, home_gives,
                               home_odds, away_odds, ts)
    VALUES (:match_id, :bookmaker, :line_depth, :home_gives,
            :home_odds, :away_odds, :ts)
    """
    with _conn(db_path) as con:
        con.execute(sql, record)


def get_odds_history(db_path: str, match_id: str, bookmaker: str = "bet365") -> list[dict]:
    sql = """
    SELECT * FROM odds_history
    WHERE match_id=? AND bookmaker=?
    ORDER BY ts
    """
    with _conn(db_path) as con:
        rows = con.execute(sql, (match_id, bookmaker)).fetchall()
        return [dict(r) for r in rows]


# ── match_events ──────────────────────────────────────────────────────────────

def insert_event(db_path: str, event: dict) -> None:
    sql = """
    INSERT INTO match_events (match_id, minute, event_type, team, ts)
    VALUES (:match_id, :minute, :event_type, :team, :ts)
    """
    event.setdefault("ts", datetime.utcnow().isoformat())
    with _conn(db_path) as con:
        con.execute(sql, event)


def get_events(db_path: str, match_id: str) -> list[dict]:
    sql = "SELECT * FROM match_events WHERE match_id=? ORDER BY minute"
    with _conn(db_path) as con:
        rows = con.execute(sql, (match_id,)).fetchall()
        return [dict(r) for r in rows]


# ── candidates ────────────────────────────────────────────────────────────────

def add_candidate(db_path: str, candidate: dict) -> None:
    sql = """
    INSERT OR IGNORE INTO candidates
        (match_id, trigger_depth, prev_depth, upgrade_ts, detected_at)
    VALUES (:match_id, :trigger_depth, :prev_depth, :upgrade_ts, :detected_at)
    """
    candidate.setdefault("detected_at", datetime.utcnow().isoformat())
    with _conn(db_path) as con:
        con.execute(sql, candidate)


def update_candidate_status(db_path: str, match_id: str, status: str) -> None:
    now = datetime.utcnow().isoformat()
    sql = """
    UPDATE candidates SET status=?, alert_sent_at=?
    WHERE match_id=?
    """
    with _conn(db_path) as con:
        con.execute(sql, (status, now if status == "alerted" else None, match_id))


def is_candidate(db_path: str, match_id: str) -> bool:
    sql = "SELECT 1 FROM candidates WHERE match_id=?"
    with _conn(db_path) as con:
        return con.execute(sql, (match_id,)).fetchone() is not None
