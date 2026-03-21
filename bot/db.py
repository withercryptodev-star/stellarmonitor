"""
Database — хранит алерты в SQLite для API и сайта.
"""

import sqlite3
import json
import time
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)
DB_FILE = "stellarmonitor.db"


def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Создаёт таблицы если не существуют."""
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS alerts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            type        TEXT NOT NULL,
            usd_value   REAL DEFAULT 0,
            pair        TEXT,
            asset       TEXT,
            amount      REAL,
            from_addr   TEXT,
            to_addr     TEXT,
            move_ctx    TEXT,
            link        TEXT,
            raw_json    TEXT,
            created_at  TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_alerts_type       ON alerts(type);
        CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at);
        CREATE INDEX IF NOT EXISTS idx_alerts_usd        ON alerts(usd_value);
    """)
    conn.commit()
    conn.close()
    log.info(f"DB initialized: {DB_FILE}")


def save_alert(alert: dict):
    """Сохраняет алерт в БД."""
    try:
        conn = get_conn()
        conn.execute("""
            INSERT INTO alerts
              (type, usd_value, pair, asset, amount, from_addr, to_addr,
               move_ctx, link, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            alert.get("type", ""),
            alert.get("usd_value") or alert.get("total_usd") or 0,
            alert.get("pair", ""),
            alert.get("asset", ""),
            alert.get("amount", 0),
            alert.get("from_full", "") or alert.get("from", ""),
            alert.get("to_full", "")   or alert.get("to", ""),
            alert.get("move_ctx", ""),
            alert.get("link", ""),
            json.dumps(alert, default=str),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"DB save error: {e}")


def get_recent_alerts(limit=50, alert_type=None, min_usd=0):
    """Возвращает последние алерты."""
    conn = get_conn()
    q = "SELECT * FROM alerts WHERE usd_value >= ?"
    params = [min_usd]
    if alert_type:
        q += " AND type = ?"
        params.append(alert_type)
    q += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats_24h():
    """Статистика за последние 24 часа."""
    conn = get_conn()
    since = "datetime('now', '-24 hours')"
    r = conn.execute(f"""
        SELECT
          COUNT(*) as total,
          SUM(CASE WHEN type IN ('WHALE_XLM','WHALE_USDC') THEN 1 ELSE 0 END) as whales,
          SUM(CASE WHEN type IN ('SDEX','SDEX_MULTI') THEN 1 ELSE 0 END) as sdex,
          SUM(CASE WHEN type='AIRDROP_SUMMARY' THEN 1 ELSE 0 END) as mass,
          SUM(CASE WHEN type='MEME_PUMP' THEN 1 ELSE 0 END) as meme,
          MAX(usd_value) as biggest_usd,
          SUM(CASE WHEN asset='USDC' OR asset='USDT' THEN usd_value ELSE 0 END) as usdc_volume
        FROM alerts WHERE created_at >= {since}
    """).fetchone()
    conn.close()
    return dict(r) if r else {}


def get_top_pairs(limit=5):
    """Топ SDEX пары за 24h."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT pair, COUNT(*) as cnt, SUM(usd_value) as vol
        FROM alerts
        WHERE type IN ('SDEX','SDEX_MULTI')
          AND pair != ''
          AND created_at >= datetime('now', '-24 hours')
        GROUP BY pair
        ORDER BY vol DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def cleanup_old(days=30):
    """Удаляет алерты старше N дней."""
    conn = get_conn()
    conn.execute(
        "DELETE FROM alerts WHERE created_at < datetime('now', ?)",
        (f"-{days} days",)
    )
    deleted = conn.total_changes
    conn.commit()
    conn.close()
    if deleted:
        log.info(f"DB cleanup: removed {deleted} old alerts")
