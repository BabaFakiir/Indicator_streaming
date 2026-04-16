import datetime as dt
import sqlite3
import threading
from pathlib import Path
from typing import Dict, List, Optional


DB_PATH = Path(__file__).with_name("chart_history.sqlite3")
RETENTION_DAYS = 7
_LOCK = threading.RLock()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _LOCK:
        conn = _conn()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS intraday_candles_1m (
                    symbol TEXT NOT NULL,
                    bucket_ts INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (symbol, bucket_ts)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_intraday_candles_symbol_ts
                ON intraday_candles_1m(symbol, bucket_ts)
                """
            )
            conn.commit()
        finally:
            conn.close()


def _bucket_start_utc(ts: dt.datetime) -> int:
    utc_ts = ts.astimezone(dt.timezone.utc).replace(second=0, microsecond=0)
    return int(utc_ts.timestamp())


def _normalize_price(price: float) -> float:
    return float(price) / 100.0


def upsert_tick(symbol: str, ts: dt.datetime, price: float) -> None:
    if not symbol:
        return
    bucket_ts = _bucket_start_utc(ts)
    normalized_price = _normalize_price(price)
    now_ts = int(dt.datetime.now(dt.timezone.utc).timestamp())

    with _LOCK:
        conn = _conn()
        try:
            conn.execute(
                """
                INSERT INTO intraday_candles_1m(symbol, bucket_ts, open, high, low, close, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, bucket_ts) DO UPDATE SET
                    high = MAX(high, excluded.high),
                    low = MIN(low, excluded.low),
                    close = excluded.close,
                    updated_at = excluded.updated_at
                """,
                (
                    symbol,
                    bucket_ts,
                    normalized_price,
                    normalized_price,
                    normalized_price,
                    normalized_price,
                    now_ts,
                ),
            )
            conn.commit()
        finally:
            conn.close()


def cleanup_old_data(now: Optional[dt.datetime] = None) -> None:
    cutoff = now or dt.datetime.now(dt.timezone.utc)
    cutoff_ts = int((cutoff - dt.timedelta(days=RETENTION_DAYS)).timestamp())
    with _LOCK:
        conn = _conn()
        try:
            conn.execute(
                "DELETE FROM intraday_candles_1m WHERE bucket_ts < ?",
                (cutoff_ts,),
            )
            conn.commit()
        finally:
            conn.close()


def list_symbols() -> List[str]:
    with _LOCK:
        conn = _conn()
        try:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM intraday_candles_1m ORDER BY symbol ASC"
            ).fetchall()
            return [str(row["symbol"]) for row in rows]
        finally:
            conn.close()


def _resolution_to_minutes(resolution: str) -> int:
    value = (resolution or "1").upper()
    if value == "D":
        return 24 * 60
    return max(1, int(value))


def _bucket_group(bucket_ts: int, resolution_minutes: int) -> int:
    resolution_seconds = resolution_minutes * 60
    return (bucket_ts // resolution_seconds) * resolution_seconds


def get_history(symbol: str, resolution: str, from_ts: int, to_ts: int) -> List[Dict]:
    resolution_minutes = _resolution_to_minutes(resolution)
    with _LOCK:
        conn = _conn()
        try:
            rows = conn.execute(
                """
                SELECT symbol, bucket_ts, open, high, low, close
                FROM intraday_candles_1m
                WHERE symbol = ? AND bucket_ts BETWEEN ? AND ?
                ORDER BY bucket_ts ASC
                """,
                (symbol, from_ts, to_ts),
            ).fetchall()
        finally:
            conn.close()

    if resolution_minutes == 1:
        return [
            {
                "time": int(row["bucket_ts"]) * 1000,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
            for row in rows
        ]

    aggregated: List[Dict] = []
    current: Optional[Dict] = None
    for row in rows:
        group_ts = _bucket_group(int(row["bucket_ts"]), resolution_minutes)
        if current is None or current["group_ts"] != group_ts:
            if current is not None:
                aggregated.append(
                    {
                        "time": current["group_ts"] * 1000,
                        "open": current["open"],
                        "high": current["high"],
                        "low": current["low"],
                        "close": current["close"],
                    }
                )
            current = {
                "group_ts": group_ts,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
            continue

        current["high"] = max(current["high"], float(row["high"]))
        current["low"] = min(current["low"], float(row["low"]))
        current["close"] = float(row["close"])

    if current is not None:
        aggregated.append(
            {
                "time": current["group_ts"] * 1000,
                "open": current["open"],
                "high": current["high"],
                "low": current["low"],
                "close": current["close"],
            }
        )
    return aggregated
