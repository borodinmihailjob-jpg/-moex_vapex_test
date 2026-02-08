import asyncio
import logging
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS instruments (
  id BIGSERIAL PRIMARY KEY,
  secid TEXT NOT NULL,
  isin TEXT,
  boardid TEXT NOT NULL DEFAULT '',
  shortname TEXT,
  asset_type TEXT NOT NULL DEFAULT 'stock'
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_instruments_secid_board_asset
  ON instruments (secid, boardid, asset_type);

CREATE TABLE IF NOT EXISTS trades (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  instrument_id BIGINT NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
  trade_date TEXT NOT NULL,
  qty DOUBLE PRECISION NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  commission DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_trades_user_instrument
  ON trades (user_id, instrument_id);

CREATE TABLE IF NOT EXISTS user_alert_settings (
  user_id BIGINT PRIMARY KEY,
  periodic_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  periodic_interval_min INTEGER NOT NULL DEFAULT 60,
  periodic_last_sent_at TEXT,
  drop_alert_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  drop_percent DOUBLE PRECISION NOT NULL DEFAULT 10,
  open_close_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  open_last_sent_date TEXT,
  close_last_sent_date TEXT
);

CREATE TABLE IF NOT EXISTS price_alert_state (
  user_id BIGINT NOT NULL,
  instrument_id BIGINT NOT NULL,
  was_below BOOLEAN NOT NULL DEFAULT FALSE,
  last_alert_at TEXT,
  PRIMARY KEY (user_id, instrument_id)
);
"""

_pools: dict[str, asyncpg.Pool] = {}
_pools_lock = asyncio.Lock()


def _norm_boardid(boardid: str | None) -> str:
    return (boardid or "").strip()


async def _get_pool(db_dsn: str) -> asyncpg.Pool:
    async with _pools_lock:
        pool = _pools.get(db_dsn)
        if pool is None:
            pool = await asyncpg.create_pool(dsn=db_dsn, min_size=1, max_size=8)
            _pools[db_dsn] = pool
        return pool


async def close_pools() -> None:
    async with _pools_lock:
        pools = list(_pools.values())
        _pools.clear()
    for p in pools:
        await p.close()


async def init_db(db_path: str):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            await conn.execute(CREATE_SQL)
        logger.info("Database initialized (PostgreSQL)")
    except Exception:
        logger.exception("Failed to initialize database")
        raise


async def upsert_instrument(
    db_path: str,
    secid: str,
    isin: str | None,
    boardid: str | None,
    shortname: str | None,
    asset_type: str = "stock",
):
    try:
        pool = await _get_pool(db_path)
        norm_board = _norm_boardid(boardid)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO instruments (secid, isin, boardid, shortname, asset_type)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (secid, boardid, asset_type)
                DO UPDATE SET
                  isin = COALESCE(EXCLUDED.isin, instruments.isin),
                  shortname = COALESCE(EXCLUDED.shortname, instruments.shortname)
                RETURNING id
                """,
                secid,
                isin,
                norm_board,
                shortname,
                asset_type,
            )
            return int(row["id"])
    except Exception:
        logger.exception("Failed upsert_instrument secid=%s boardid=%s asset_type=%s", secid, boardid, asset_type)
        raise


async def add_trade(db_path: str, user_id: int, instrument_id: int, trade_date: str, qty: float, price: float, commission: float):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO trades (user_id, instrument_id, trade_date, qty, price, commission)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                int(user_id),
                int(instrument_id),
                trade_date,
                float(qty),
                float(price),
                float(commission),
            )
        logger.info("Trade inserted: user=%s instrument=%s qty=%s price=%s", user_id, instrument_id, qty, price)
    except Exception:
        logger.exception("Failed add_trade user=%s instrument=%s", user_id, instrument_id)
        raise


async def get_position_agg(db_path: str, user_id: int, instrument_id: int):
    """
    total_qty, total_cost (qty*price + commission), avg_price = total_cost/total_qty
    """
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                  COALESCE(SUM(qty),0) AS total_qty,
                  COALESCE(SUM(qty*price + commission),0) AS total_cost
                FROM trades
                WHERE user_id=$1 AND instrument_id=$2
                """,
                int(user_id),
                int(instrument_id),
            )
            total_qty = float(row["total_qty"])
            total_cost = float(row["total_cost"])
            avg_price = (total_cost / total_qty) if total_qty else 0.0
            return total_qty, total_cost, float(avg_price)
    except Exception:
        logger.exception("Failed get_position_agg user=%s instrument=%s", user_id, instrument_id)
        raise


async def get_instrument(db_path: str, instrument_id: int):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, secid, isin, boardid, shortname, COALESCE(asset_type,'stock') AS asset_type
                FROM instruments
                WHERE id=$1
                """,
                int(instrument_id),
            )
            if not row:
                return None
            return {
                "id": int(row["id"]),
                "secid": row["secid"],
                "isin": row["isin"],
                "boardid": row["boardid"],
                "shortname": row["shortname"],
                "asset_type": row["asset_type"],
            }
    except Exception:
        logger.exception("Failed get_instrument instrument=%s", instrument_id)
        raise


async def get_user_positions(db_path: str, user_id: int):
    """
    Возвращает агрегированные позиции пользователя по инструментам.
    total_qty, total_cost (qty*price + commission), avg_price = total_cost/total_qty
    """
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                  i.id,
                  i.secid,
                  i.isin,
                  i.boardid,
                  i.shortname,
                  COALESCE(i.asset_type, 'stock') AS asset_type,
                  COALESCE(SUM(t.qty), 0) AS total_qty,
                  COALESCE(SUM(t.qty * t.price + t.commission), 0) AS total_cost
                FROM trades t
                JOIN instruments i ON i.id = t.instrument_id
                WHERE t.user_id = $1
                GROUP BY i.id, i.secid, i.isin, i.boardid, i.shortname, COALESCE(i.asset_type, 'stock')
                HAVING ABS(COALESCE(SUM(t.qty), 0)) > 1e-12
                ORDER BY i.secid
                """,
                int(user_id),
            )

        out = []
        for row in rows:
            total_qty = float(row["total_qty"])
            total_cost = float(row["total_cost"])
            avg_price = (total_cost / total_qty) if total_qty else 0.0
            out.append(
                {
                    "id": int(row["id"]),
                    "secid": row["secid"],
                    "isin": row["isin"],
                    "boardid": row["boardid"],
                    "shortname": row["shortname"],
                    "asset_type": row["asset_type"],
                    "total_qty": total_qty,
                    "total_cost": total_cost,
                    "avg_price": float(avg_price),
                }
            )
        return out
    except Exception:
        logger.exception("Failed get_user_positions user=%s", user_id)
        raise


async def ensure_user_alert_settings(db_path: str, user_id: int):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO user_alert_settings (user_id)
                VALUES ($1)
                ON CONFLICT(user_id) DO NOTHING
                """,
                int(user_id),
            )
    except Exception:
        logger.exception("Failed ensure_user_alert_settings user=%s", user_id)
        raise


async def set_periodic_alert(db_path: str, user_id: int, enabled: bool, interval_min: int | None = None):
    try:
        await ensure_user_alert_settings(db_path, user_id)
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            if interval_min is None:
                await conn.execute(
                    """
                    UPDATE user_alert_settings
                    SET periodic_enabled=$1,
                        periodic_last_sent_at=NULL
                    WHERE user_id=$2
                    """,
                    bool(enabled),
                    int(user_id),
                )
            else:
                await conn.execute(
                    """
                    UPDATE user_alert_settings
                    SET periodic_enabled=$1,
                        periodic_interval_min=$2,
                        periodic_last_sent_at=NULL
                    WHERE user_id=$3
                    """,
                    bool(enabled),
                    int(interval_min),
                    int(user_id),
                )
    except Exception:
        logger.exception("Failed set_periodic_alert user=%s", user_id)
        raise


async def set_drop_alert(db_path: str, user_id: int, enabled: bool, drop_percent: float | None = None):
    try:
        await ensure_user_alert_settings(db_path, user_id)
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            if drop_percent is None:
                await conn.execute(
                    "UPDATE user_alert_settings SET drop_alert_enabled=$1 WHERE user_id=$2",
                    bool(enabled),
                    int(user_id),
                )
            else:
                await conn.execute(
                    "UPDATE user_alert_settings SET drop_alert_enabled=$1, drop_percent=$2 WHERE user_id=$3",
                    bool(enabled),
                    float(drop_percent),
                    int(user_id),
                )
    except Exception:
        logger.exception("Failed set_drop_alert user=%s", user_id)
        raise


async def set_open_close_alert(db_path: str, user_id: int, enabled: bool):
    try:
        await ensure_user_alert_settings(db_path, user_id)
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE user_alert_settings
                SET open_close_enabled=$1,
                    open_last_sent_date=NULL,
                    close_last_sent_date=NULL
                WHERE user_id=$2
                """,
                bool(enabled),
                int(user_id),
            )
    except Exception:
        logger.exception("Failed set_open_close_alert user=%s", user_id)
        raise


async def get_user_alert_settings(db_path: str, user_id: int):
    try:
        await ensure_user_alert_settings(db_path, user_id)
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                  user_id,
                  periodic_enabled,
                  periodic_interval_min,
                  periodic_last_sent_at,
                  drop_alert_enabled,
                  drop_percent,
                  open_close_enabled,
                  open_last_sent_date,
                  close_last_sent_date
                FROM user_alert_settings
                WHERE user_id=$1
                """,
                int(user_id),
            )
        return {
            "user_id": int(row["user_id"]),
            "periodic_enabled": bool(row["periodic_enabled"]),
            "periodic_interval_min": int(row["periodic_interval_min"]),
            "periodic_last_sent_at": row["periodic_last_sent_at"],
            "drop_alert_enabled": bool(row["drop_alert_enabled"]),
            "drop_percent": float(row["drop_percent"]),
            "open_close_enabled": bool(row["open_close_enabled"]),
            "open_last_sent_date": row["open_last_sent_date"],
            "close_last_sent_date": row["close_last_sent_date"],
        }
    except Exception:
        logger.exception("Failed get_user_alert_settings user=%s", user_id)
        raise


async def list_users_with_alerts(db_path: str):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT user_id
                FROM user_alert_settings
                WHERE periodic_enabled=TRUE OR drop_alert_enabled=TRUE OR open_close_enabled=TRUE
                """
            )
        return [int(r["user_id"]) for r in rows]
    except Exception:
        logger.exception("Failed list_users_with_alerts")
        raise


async def update_periodic_last_sent_at(db_path: str, user_id: int, iso_ts: str):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_alert_settings SET periodic_last_sent_at=$1 WHERE user_id=$2",
                iso_ts,
                int(user_id),
            )
    except Exception:
        logger.exception("Failed update_periodic_last_sent_at user=%s", user_id)
        raise


async def update_open_sent_date(db_path: str, user_id: int, date_iso: str):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_alert_settings SET open_last_sent_date=$1 WHERE user_id=$2",
                date_iso,
                int(user_id),
            )
    except Exception:
        logger.exception("Failed update_open_sent_date user=%s", user_id)
        raise


async def update_close_sent_date(db_path: str, user_id: int, date_iso: str):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_alert_settings SET close_last_sent_date=$1 WHERE user_id=$2",
                date_iso,
                int(user_id),
            )
    except Exception:
        logger.exception("Failed update_close_sent_date user=%s", user_id)
        raise


async def get_price_alert_state(db_path: str, user_id: int, instrument_id: int) -> bool:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT was_below FROM price_alert_state WHERE user_id=$1 AND instrument_id=$2",
                int(user_id),
                int(instrument_id),
            )
        return bool(row["was_below"]) if row else False
    except Exception:
        logger.exception("Failed get_price_alert_state user=%s instrument=%s", user_id, instrument_id)
        raise


async def set_price_alert_state(db_path: str, user_id: int, instrument_id: int, was_below: bool, alert_ts: str | None = None):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO price_alert_state (user_id, instrument_id, was_below, last_alert_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT(user_id, instrument_id) DO UPDATE SET
                  was_below=EXCLUDED.was_below,
                  last_alert_at=EXCLUDED.last_alert_at
                """,
                int(user_id),
                int(instrument_id),
                bool(was_below),
                alert_ts,
            )
    except Exception:
        logger.exception("Failed set_price_alert_state user=%s instrument=%s", user_id, instrument_id)
        raise
