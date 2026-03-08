"""SQLite storage layer."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from loguru import logger

from src.timeutils import to_utc_iso


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
    schema = _schema_path().read_text(encoding="utf-8")
    with _conn(db_path) as con:
        con.executescript(schema)
    logger.info(f"Database initialized: {db_path}")


def _norm_status(status: str) -> str:
    key = str(status or "").strip().lower()
    mapping = {
        "scheduled": "scheduled",
        "not_started": "scheduled",
        "ns": "scheduled",
        "live": "live",
        "inplay": "live",
        "halftime": "halftime",
        "ht": "halftime",
        "finished": "finished",
        "ft": "finished",
        "ended": "finished",
    }
    return mapping.get(key, "scheduled")


def upsert_match(db_path: str, match: dict) -> None:
    payload = {
        "id": str(match.get("id", "")).strip(),
        "league": match.get("league", ""),
        "home_team": match.get("home_team", ""),
        "away_team": match.get("away_team", ""),
        "kickoff_time": to_utc_iso(match.get("kickoff_time")),
        "status": _norm_status(match.get("status", "scheduled")),
        "ht_home": match.get("ht_home"),
        "ht_away": match.get("ht_away"),
        "ft_home": match.get("ft_home"),
        "ft_away": match.get("ft_away"),
        "updated_at": datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
    }
    if not payload["id"]:
        return

    sql = """
    INSERT INTO matches (id, league, home_team, away_team, kickoff_time, status,
                         ht_home, ht_away, ft_home, ft_away, updated_at)
    VALUES (:id, :league, :home_team, :away_team, :kickoff_time, :status,
            :ht_home, :ht_away, :ft_home, :ft_away, :updated_at)
    ON CONFLICT(id) DO UPDATE SET
        league=excluded.league,
        home_team=excluded.home_team,
        away_team=excluded.away_team,
        kickoff_time=excluded.kickoff_time,
        status=excluded.status,
        ht_home=excluded.ht_home,
        ht_away=excluded.ht_away,
        ft_home=excluded.ft_home,
        ft_away=excluded.ft_away,
        updated_at=excluded.updated_at
    """
    with _conn(db_path) as con:
        con.execute(sql, payload)


def get_match(db_path: str, match_id: str) -> Optional[dict]:
    with _conn(db_path) as con:
        row = con.execute("SELECT * FROM matches WHERE id=?", (str(match_id),)).fetchone()
        return dict(row) if row else None


def get_upcoming_matches(db_path: str, within_minutes: int = 90) -> list[dict]:
    now = datetime.utcnow()
    cutoff = now + timedelta(minutes=within_minutes)
    now_s = now.isoformat(sep=" ", timespec="seconds")
    cutoff_s = cutoff.isoformat(sep=" ", timespec="seconds")
    sql = """
    SELECT * FROM matches
    WHERE status='scheduled'
      AND datetime(kickoff_time) >= datetime(?)
      AND datetime(kickoff_time) <= datetime(?)
    ORDER BY datetime(kickoff_time)
    """
    with _conn(db_path) as con:
        rows = con.execute(sql, (now_s, cutoff_s)).fetchall()
        return [dict(r) for r in rows]


def get_live_candidates(db_path: str) -> list[dict]:
    sql = """
    SELECT m.*, c.trigger_depth, c.prev_depth, c.upgrade_ts
    FROM candidates c
    JOIN matches m ON m.id = c.match_id
    WHERE c.status = 'watching'
    """
    with _conn(db_path) as con:
        rows = con.execute(sql).fetchall()
        return [dict(r) for r in rows]


def insert_odds(db_path: str, record: dict) -> None:
    ts = to_utc_iso(record.get("ts"))
    if not ts:
        return
    try:
        line_depth = float(record.get("line_depth"))
    except (TypeError, ValueError):
        return

    def _optional_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    payload = {
        "match_id": str(record.get("match_id", "")).strip(),
        "bookmaker": str(record.get("bookmaker", "bet365")).strip().lower(),
        "line_depth": line_depth,
        "home_gives": int(bool(record.get("home_gives", 0))),
        "home_odds": _optional_float(record.get("home_odds")),
        "away_odds": _optional_float(record.get("away_odds")),
        "ts": ts,
    }
    if not payload["match_id"]:
        return

    sql = """
    INSERT OR IGNORE INTO odds_history
        (match_id, bookmaker, line_depth, home_gives, home_odds, away_odds, ts)
    VALUES
        (:match_id, :bookmaker, :line_depth, :home_gives, :home_odds, :away_odds, :ts)
    """
    with _conn(db_path) as con:
        con.execute(sql, payload)


def get_odds_history(db_path: str, match_id: str, bookmaker: str = "bet365") -> list[dict]:
    sql = """
    SELECT * FROM odds_history
    WHERE match_id=? AND bookmaker=?
    ORDER BY datetime(ts)
    """
    with _conn(db_path) as con:
        rows = con.execute(sql, (str(match_id), bookmaker.lower())).fetchall()
        return [dict(r) for r in rows]


def insert_event(db_path: str, event: dict) -> None:
    try:
        minute = int(event.get("minute", 0))
    except (TypeError, ValueError):
        minute = 0

    payload = {
        "match_id": str(event.get("match_id", "")).strip(),
        "minute": minute,
        "event_type": str(event.get("event_type", "")).strip(),
        "team": str(event.get("team", "")).strip(),
        "ts": to_utc_iso(event.get("ts")) or datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
    }
    if not payload["match_id"] or not payload["event_type"] or not payload["team"]:
        return

    sql = """
    INSERT OR IGNORE INTO match_events (match_id, minute, event_type, team, ts)
    VALUES (:match_id, :minute, :event_type, :team, :ts)
    """
    with _conn(db_path) as con:
        con.execute(sql, payload)


def get_events(db_path: str, match_id: str) -> list[dict]:
    sql = "SELECT * FROM match_events WHERE match_id=? ORDER BY minute, datetime(ts)"
    with _conn(db_path) as con:
        rows = con.execute(sql, (str(match_id),)).fetchall()
        return [dict(r) for r in rows]


def add_candidate(db_path: str, candidate: dict) -> None:
    payload = {
        "match_id": str(candidate.get("match_id", "")).strip(),
        "trigger_depth": candidate.get("trigger_depth"),
        "prev_depth": candidate.get("prev_depth"),
        "upgrade_ts": to_utc_iso(candidate.get("upgrade_ts")),
        "detected_at": datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
    }
    if not payload["match_id"]:
        return

    sql = """
    INSERT OR IGNORE INTO candidates
        (match_id, trigger_depth, prev_depth, upgrade_ts, detected_at)
    VALUES (:match_id, :trigger_depth, :prev_depth, :upgrade_ts, :detected_at)
    """
    with _conn(db_path) as con:
        con.execute(sql, payload)


def update_candidate_status(db_path: str, match_id: str, status: str) -> None:
    now = datetime.utcnow().isoformat(sep=" ", timespec="seconds")
    sql = """
    UPDATE candidates
    SET status=?, alert_sent_at=?
    WHERE match_id=?
    """
    alert_sent_at = now if status == "alerted" else None
    with _conn(db_path) as con:
        con.execute(sql, (status, alert_sent_at, str(match_id)))


def is_candidate(db_path: str, match_id: str) -> bool:
    sql = "SELECT 1 FROM candidates WHERE match_id=?"
    with _conn(db_path) as con:
        return con.execute(sql, (str(match_id),)).fetchone() is not None
