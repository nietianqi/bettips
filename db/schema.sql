-- bettips 数据库建表语句

CREATE TABLE IF NOT EXISTS matches (
    id TEXT PRIMARY KEY,
    league TEXT,
    home_team TEXT,
    away_team TEXT,
    kickoff_time DATETIME,
    status TEXT,          -- scheduled | live | halftime | finished
    ht_home INTEGER,
    ht_away INTEGER,
    ft_home INTEGER,
    ft_away INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS odds_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id TEXT NOT NULL,
    bookmaker TEXT NOT NULL,        -- 'bet365'
    line_depth REAL NOT NULL,       -- 标准化让球深度（正数=主让）
    home_gives INTEGER NOT NULL,    -- 1=主队让球，0=客队让球
    home_odds REAL,
    away_odds REAL,
    ts DATETIME NOT NULL,
    FOREIGN KEY (match_id) REFERENCES matches(id)
);

CREATE TABLE IF NOT EXISTS match_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id TEXT NOT NULL,
    minute INTEGER,
    event_type TEXT NOT NULL,       -- 'goal' | 'red_card' | 'yellow_card'
    team TEXT NOT NULL,             -- 'home' | 'away'
    ts DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (match_id) REFERENCES matches(id)
);

CREATE TABLE IF NOT EXISTS candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id TEXT UNIQUE NOT NULL,
    detected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    trigger_depth REAL,             -- 触发升盘的新深度
    prev_depth REAL,                -- 升盘前的深度
    upgrade_ts DATETIME,            -- 升盘发生的时间
    status TEXT DEFAULT 'watching', -- watching | alerted | dismissed
    alert_sent_at DATETIME,
    FOREIGN KEY (match_id) REFERENCES matches(id)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_odds_match_id ON odds_history(match_id);
CREATE INDEX IF NOT EXISTS idx_odds_ts ON odds_history(ts);
CREATE UNIQUE INDEX IF NOT EXISTS uq_odds_dedupe
    ON odds_history(match_id, bookmaker, line_depth, home_gives, home_odds, away_odds, ts);
CREATE INDEX IF NOT EXISTS idx_events_match_id ON match_events(match_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_dedupe
    ON match_events(match_id, minute, event_type, team);
CREATE INDEX IF NOT EXISTS idx_matches_kickoff ON matches(kickoff_time);
CREATE INDEX IF NOT EXISTS idx_candidates_status ON candidates(status);
