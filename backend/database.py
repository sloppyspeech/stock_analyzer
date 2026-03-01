import sqlite3
import json
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "stock_analyzer.db"


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS stock_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            data_type TEXT NOT NULL,  -- 'info', 'history', 'fundamentals', 'technicals'
            data TEXT NOT NULL,       -- JSON
            cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_cache_unique
            ON stock_cache(symbol, exchange, data_type);

        CREATE TABLE IF NOT EXISTS analysis_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            analysis_type TEXT NOT NULL,  -- 'fundamental', 'technical', 'ai_combined'
            prompt TEXT,
            result TEXT NOT NULL,         -- JSON or text
            model TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_analysis_symbol
            ON analysis_history(symbol, exchange);

        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT,
            UNIQUE(symbol, exchange)
        );

        CREATE TABLE IF NOT EXISTS price_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            alert_type TEXT NOT NULL,  -- 'above', 'below'
            price REAL NOT NULL,
            triggered INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS news_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            articles TEXT NOT NULL,      -- JSON array
            aggregate TEXT NOT NULL,     -- JSON aggregate sentiment
            categories TEXT,             -- JSON
            timeline TEXT,               -- JSON
            source_counts TEXT,          -- JSON
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            UNIQUE(symbol, exchange)
        );

        CREATE TABLE IF NOT EXISTS sentiment_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            sentiment_label TEXT NOT NULL,
            sentiment_score REAL NOT NULL,
            ai_analysis TEXT,
            model TEXT,
            article_count INTEGER DEFAULT 0,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_sentiment_symbol
            ON sentiment_history(symbol, exchange);
    """)

    conn.commit()
    conn.close()


def cache_get(symbol: str, exchange: str, data_type: str):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT data FROM stock_cache
        WHERE symbol=? AND exchange=? AND data_type=?
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
    """, (symbol, exchange, data_type))
    row = cursor.fetchone()
    conn.close()
    if row:
        return json.loads(row["data"])
    return None


def cache_set(symbol: str, exchange: str, data_type: str, data: dict, ttl_hours: int = 6):
    conn = get_db()
    cursor = conn.cursor()
    expires = f"datetime('now', '+{ttl_hours} hours')"
    cursor.execute(f"""
        INSERT OR REPLACE INTO stock_cache (symbol, exchange, data_type, data, expires_at)
        VALUES (?, ?, ?, ?, {expires})
    """, (symbol, exchange, data_type, json.dumps(data)))
    conn.commit()
    conn.close()


def save_analysis(symbol: str, exchange: str, analysis_type: str, result, prompt: str = None, model: str = None):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO analysis_history (symbol, exchange, analysis_type, prompt, result, model)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (symbol, exchange, analysis_type, prompt, json.dumps(result) if isinstance(result, dict) else result, model))
    conn.commit()
    conn.close()


def get_analysis_history(symbol: str, exchange: str, limit: int = 10):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM analysis_history
        WHERE symbol=? AND exchange=?
        ORDER BY created_at DESC LIMIT ?
    """, (symbol, exchange, limit))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_watchlist():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM watchlist ORDER BY added_at DESC")
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def add_to_watchlist(symbol: str, exchange: str, notes: str = None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO watchlist (symbol, exchange, notes)
            VALUES (?, ?, ?)
        """, (symbol, exchange, notes))
        conn.commit()
        success = cursor.rowcount > 0
    except Exception:
        success = False
    conn.close()
    return success


def remove_from_watchlist(symbol: str, exchange: str):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM watchlist WHERE symbol=? AND exchange=?", (symbol, exchange))
    conn.commit()
    conn.close()


def save_sentiment(symbol: str, exchange: str, label: str, score: float,
                   ai_analysis: str = None, model: str = None, article_count: int = 0):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO sentiment_history
            (symbol, exchange, sentiment_label, sentiment_score, ai_analysis, model, article_count)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (symbol, exchange, label, score, ai_analysis, model, article_count))
    conn.commit()
    conn.close()


def get_sentiment_history(symbol: str, exchange: str, limit: int = 30) -> list:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sentiment_label, sentiment_score, ai_analysis, model,
               article_count, recorded_at
        FROM sentiment_history
        WHERE symbol=? AND exchange=?
        ORDER BY recorded_at DESC LIMIT ?
    """, (symbol, exchange, limit))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows
