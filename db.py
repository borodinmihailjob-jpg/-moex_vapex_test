import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  telegram_user_id BIGINT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS portfolios (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name TEXT NOT NULL DEFAULT 'Основной',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, name)
);

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
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  portfolio_id BIGINT REFERENCES portfolios(id) ON DELETE CASCADE,
  instrument_id BIGINT NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
  external_trade_id TEXT,
  import_source TEXT,
  trade_date TEXT NOT NULL,
  qty DOUBLE PRECISION NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  commission DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_trades_user_instrument
  ON trades (user_id, instrument_id);

CREATE TABLE IF NOT EXISTS user_positions (
  portfolio_id BIGINT NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
  instrument_id BIGINT NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
  total_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
  avg_price DOUBLE PRECISION NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (portfolio_id, instrument_id)
);

CREATE INDEX IF NOT EXISTS ix_user_positions_portfolio
  ON user_positions (portfolio_id);

CREATE TABLE IF NOT EXISTS price_cache (
  instrument_id BIGINT PRIMARY KEY REFERENCES instruments(id) ON DELETE CASCADE,
  last_price DOUBLE PRECISION NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_alert_settings (
  user_id BIGINT PRIMARY KEY,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
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
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  instrument_id BIGINT NOT NULL,
  was_below BOOLEAN NOT NULL DEFAULT FALSE,
  last_alert_at TEXT,
  PRIMARY KEY (user_id, instrument_id)
);

"""

MIGRATION_SQL = [
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS user_ref_id BIGINT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS portfolio_id BIGINT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS external_trade_id TEXT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS import_source TEXT",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS user_ref_id BIGINT",
    "ALTER TABLE price_alert_state ADD COLUMN IF NOT EXISTS user_ref_id BIGINT",
]

POST_MIGRATION_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS ix_trades_user_ref_instrument ON trades (user_ref_id, instrument_id)",
    "CREATE INDEX IF NOT EXISTS ix_trades_portfolio_instrument ON trades (portfolio_id, instrument_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_trades_user_external_trade_id ON trades (user_id, external_trade_id)",
    "CREATE INDEX IF NOT EXISTS ix_user_alert_settings_user_ref ON user_alert_settings (user_ref_id)",
    "CREATE INDEX IF NOT EXISTS ix_price_alert_state_user_ref_instrument ON price_alert_state (user_ref_id, instrument_id)",
]

_pools: dict[str, asyncpg.Pool] = {}
_pools_lock = asyncio.Lock()
_single_instance_lock_conn: asyncpg.Connection | None = None
_single_instance_lock_key: int | None = None


def _norm_boardid(boardid: str | None) -> str:
    return (boardid or "").strip()


def _pick_canonical_metal(rows: list[asyncpg.Record]) -> asyncpg.Record:
    # Priority:
    # 1) has ISIN
    # 2) empty boardid
    # 3) user-friendly shortname
    # 4) lowest id
    return sorted(
        rows,
        key=lambda r: (
            0 if (r["isin"] or "").strip() else 1,
            0 if (r["boardid"] or "").strip() == "" else 1,
            0 if (r["shortname"] or "").strip().lower() in {"золото", "серебро", "платина", "палладий"} else 1,
            int(r["id"]),
        ),
    )[0]


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


def _advisory_lock_key(lock_name: str) -> int:
    digest = hashlib.sha256(lock_name.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


async def acquire_single_instance_lock(db_dsn: str, lock_name: str) -> bool:
    global _single_instance_lock_conn, _single_instance_lock_key
    if _single_instance_lock_conn is not None:
        return True
    key = _advisory_lock_key(lock_name)
    conn = await asyncpg.connect(dsn=db_dsn)
    locked = await conn.fetchval("SELECT pg_try_advisory_lock($1::bigint)", key)
    if locked:
        _single_instance_lock_conn = conn
        _single_instance_lock_key = key
        return True
    await conn.close()
    return False


async def release_single_instance_lock() -> None:
    global _single_instance_lock_conn, _single_instance_lock_key
    conn = _single_instance_lock_conn
    key = _single_instance_lock_key
    _single_instance_lock_conn = None
    _single_instance_lock_key = None
    if conn is None:
        return
    try:
        if key is not None:
            await conn.execute("SELECT pg_advisory_unlock($1::bigint)", key)
    except Exception:
        logger.exception("Failed to release advisory lock")
    finally:
        await conn.close()


async def _ensure_user_context(conn: asyncpg.Connection, telegram_user_id: int) -> tuple[int, int]:
    user_row = await conn.fetchrow(
        """
        INSERT INTO users (telegram_user_id)
        VALUES ($1)
        ON CONFLICT (telegram_user_id) DO UPDATE
        SET telegram_user_id = EXCLUDED.telegram_user_id
        RETURNING id
        """,
        int(telegram_user_id),
    )
    user_ref_id = int(user_row["id"])
    portfolio_row = await conn.fetchrow(
        """
        INSERT INTO portfolios (user_id, name)
        VALUES ($1, 'Основной')
        ON CONFLICT (user_id, name) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING id
        """,
        user_ref_id,
    )
    portfolio_id = int(portfolio_row["id"])
    return user_ref_id, portfolio_id


async def _get_user_context(conn: asyncpg.Connection, telegram_user_id: int) -> tuple[int | None, int | None]:
    row = await conn.fetchrow(
        """
        SELECT u.id AS user_ref_id, p.id AS portfolio_id
        FROM users u
        LEFT JOIN portfolios p ON p.user_id = u.id AND p.name = 'Основной'
        WHERE u.telegram_user_id = $1
        LIMIT 1
        """,
        int(telegram_user_id),
    )
    if not row:
        return None, None
    return (
        int(row["user_ref_id"]) if row["user_ref_id"] is not None else None,
        int(row["portfolio_id"]) if row["portfolio_id"] is not None else None,
    )


async def _backfill_user_links(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        INSERT INTO users (telegram_user_id)
        SELECT DISTINCT user_id FROM trades
        WHERE user_id IS NOT NULL
        ON CONFLICT (telegram_user_id) DO NOTHING
        """
    )
    await conn.execute(
        """
        INSERT INTO users (telegram_user_id)
        SELECT DISTINCT user_id FROM user_alert_settings
        WHERE user_id IS NOT NULL
        ON CONFLICT (telegram_user_id) DO NOTHING
        """
    )
    await conn.execute(
        """
        INSERT INTO users (telegram_user_id)
        SELECT DISTINCT user_id FROM price_alert_state
        WHERE user_id IS NOT NULL
        ON CONFLICT (telegram_user_id) DO NOTHING
        """
    )
    await conn.execute(
        """
        INSERT INTO portfolios (user_id, name)
        SELECT u.id, 'Основной'
        FROM users u
        LEFT JOIN portfolios p ON p.user_id = u.id AND p.name = 'Основной'
        WHERE p.id IS NULL
        """
    )
    await conn.execute(
        """
        UPDATE trades t
        SET user_ref_id = u.id
        FROM users u
        WHERE t.user_ref_id IS NULL
          AND u.telegram_user_id = t.user_id
        """
    )
    await conn.execute(
        """
        UPDATE trades t
        SET portfolio_id = p.id
        FROM portfolios p
        WHERE t.portfolio_id IS NULL
          AND t.user_ref_id = p.user_id
          AND p.name = 'Основной'
        """
    )
    await conn.execute(
        """
        UPDATE user_alert_settings s
        SET user_ref_id = u.id
        FROM users u
        WHERE s.user_ref_id IS NULL
          AND u.telegram_user_id = s.user_id
        """
    )
    await conn.execute(
        """
        UPDATE price_alert_state s
        SET user_ref_id = u.id
        FROM users u
        WHERE s.user_ref_id IS NULL
          AND u.telegram_user_id = s.user_id
        """
    )


async def _rebuild_positions(conn: asyncpg.Connection) -> None:
    await conn.execute("TRUNCATE TABLE user_positions")
    await conn.execute(
        """
        INSERT INTO user_positions (portfolio_id, instrument_id, total_qty, total_cost, avg_price, updated_at)
        SELECT
          t.portfolio_id,
          t.instrument_id,
          COALESCE(SUM(t.qty), 0) AS total_qty,
          COALESCE(SUM(t.qty * t.price + t.commission), 0) AS total_cost,
          CASE
            WHEN ABS(COALESCE(SUM(t.qty), 0)) > 1e-12
            THEN COALESCE(SUM(t.qty * t.price + t.commission), 0) / COALESCE(SUM(t.qty), 0)
            ELSE 0
          END AS avg_price,
          NOW()
        FROM trades t
        WHERE t.portfolio_id IS NOT NULL
        GROUP BY t.portfolio_id, t.instrument_id
        HAVING ABS(COALESCE(SUM(t.qty), 0)) > 1e-12
        """
    )


async def _deduplicate_metal_instruments(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        SELECT id, secid, isin, boardid, shortname
        FROM instruments
        WHERE asset_type = 'metal'
        ORDER BY secid, id
        """
    )
    by_secid: dict[str, list[asyncpg.Record]] = {}
    for row in rows:
        secid = str(row["secid"])
        by_secid.setdefault(secid, []).append(row)

    merged_any = False
    for secid, items in by_secid.items():
        if len(items) <= 1:
            continue
        merged_any = True
        canonical = _pick_canonical_metal(items)
        canonical_id = int(canonical["id"])
        dup_ids = [int(r["id"]) for r in items if int(r["id"]) != canonical_id]

        await conn.execute(
            """
            UPDATE instruments i
            SET isin = COALESCE(NULLIF(i.isin, ''), $2),
                shortname = COALESCE(NULLIF(i.shortname, ''), $3),
                boardid = ''
            WHERE i.id = $1
            """,
            canonical_id,
            (canonical["isin"] or None),
            (canonical["shortname"] or None),
        )

        for dup_id in dup_ids:
            await conn.execute("UPDATE trades SET instrument_id = $1 WHERE instrument_id = $2", canonical_id, dup_id)
            await conn.execute("DELETE FROM price_cache WHERE instrument_id = $1", dup_id)
            await conn.execute("DELETE FROM price_alert_state WHERE instrument_id = $1", dup_id)
            await conn.execute("DELETE FROM user_positions WHERE instrument_id = $1", dup_id)
            await conn.execute("DELETE FROM instruments WHERE id = $1", dup_id)

        logger.info(
            "Merged duplicate metal instruments secid=%s canonical_id=%s removed=%s",
            secid,
            canonical_id,
            dup_ids,
        )

    if merged_any:
        await conn.execute("DELETE FROM price_cache")
        await conn.execute("TRUNCATE TABLE price_alert_state")


async def init_db(db_path: str):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(CREATE_SQL)
                for sql in MIGRATION_SQL:
                    await conn.execute(sql)
                for sql in POST_MIGRATION_INDEX_SQL:
                    await conn.execute(sql)
                await _backfill_user_links(conn)
                await _deduplicate_metal_instruments(conn)
                await _rebuild_positions(conn)
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
        norm_board = "" if asset_type == "metal" else _norm_boardid(boardid)
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


async def add_trade(
    db_path: str,
    user_id: int,
    instrument_id: int,
    trade_date: str,
    qty: float,
    price: float,
    commission: float,
    external_trade_id: str | None = None,
    import_source: str | None = None,
) -> bool:
    try:
        pool = await _get_pool(db_path)
        qty_f = float(qty)
        cost_f = qty_f * float(price) + float(commission)
        inserted = False
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, portfolio_id = await _ensure_user_context(conn, int(user_id))
                row = await conn.fetchrow(
                    """
                    INSERT INTO trades (
                      user_id, user_ref_id, portfolio_id, instrument_id,
                      external_trade_id, import_source, trade_date, qty, price, commission
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (user_id, external_trade_id) DO NOTHING
                    RETURNING id
                    """,
                    int(user_id),
                    user_ref_id,
                    portfolio_id,
                    int(instrument_id),
                    (external_trade_id or None),
                    (import_source or None),
                    trade_date,
                    qty_f,
                    float(price),
                    float(commission),
                )
                inserted = row is not None
                if not inserted:
                    return False
                await conn.execute(
                    """
                    INSERT INTO user_positions (portfolio_id, instrument_id, total_qty, total_cost, avg_price, updated_at)
                    VALUES ($1, $2, $3, $4, 0, NOW())
                    ON CONFLICT (portfolio_id, instrument_id) DO UPDATE
                    SET total_qty = user_positions.total_qty + EXCLUDED.total_qty,
                        total_cost = user_positions.total_cost + EXCLUDED.total_cost,
                        avg_price = CASE
                            WHEN ABS(user_positions.total_qty + EXCLUDED.total_qty) > 1e-12
                            THEN (user_positions.total_cost + EXCLUDED.total_cost) / (user_positions.total_qty + EXCLUDED.total_qty)
                            ELSE 0
                        END,
                        updated_at = NOW()
                    """,
                    portfolio_id,
                    int(instrument_id),
                    qty_f,
                    cost_f,
                )
                await conn.execute(
                    """
                    DELETE FROM user_positions
                    WHERE portfolio_id = $1
                      AND instrument_id = $2
                      AND ABS(total_qty) <= 1e-12
                    """,
                    portfolio_id,
                    int(instrument_id),
                )
        logger.info(
            "Trade inserted: user=%s instrument=%s qty=%s price=%s external_trade_id=%s",
            user_id,
            instrument_id,
            qty,
            price,
            external_trade_id,
        )
        return inserted
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
            _, portfolio_id = await _get_user_context(conn, int(user_id))
            if portfolio_id is None:
                return 0.0, 0.0, 0.0
            row = await conn.fetchrow(
                """
                SELECT total_qty, total_cost, avg_price
                FROM user_positions
                WHERE portfolio_id = $1 AND instrument_id = $2
                """,
                portfolio_id,
                int(instrument_id),
            )
            if not row:
                return 0.0, 0.0, 0.0
            return float(row["total_qty"]), float(row["total_cost"]), float(row["avg_price"])
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
            _, portfolio_id = await _get_user_context(conn, int(user_id))
            if portfolio_id is None:
                return []
            rows = await conn.fetch(
                """
                SELECT
                  i.id,
                  i.secid,
                  i.isin,
                  i.boardid,
                  i.shortname,
                  COALESCE(i.asset_type, 'stock') AS asset_type,
                  up.total_qty,
                  up.total_cost,
                  up.avg_price
                FROM user_positions up
                JOIN instruments i ON i.id = up.instrument_id
                WHERE up.portfolio_id = $1
                  AND ABS(up.total_qty) > 1e-12
                ORDER BY i.secid
                """,
                portfolio_id,
            )
        return [
            {
                "id": int(row["id"]),
                "secid": row["secid"],
                "isin": row["isin"],
                "boardid": row["boardid"],
                "shortname": row["shortname"],
                "asset_type": row["asset_type"],
                "total_qty": float(row["total_qty"]),
                "total_cost": float(row["total_cost"]),
                "avg_price": float(row["avg_price"]),
            }
            for row in rows
        ]
    except Exception:
        logger.exception("Failed get_user_positions user=%s", user_id)
        raise


async def list_active_position_instruments(db_path: str) -> list[dict[str, Any]]:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT
                  i.id AS instrument_id,
                  i.secid,
                  i.boardid,
                  COALESCE(i.asset_type, 'stock') AS asset_type
                FROM user_positions up
                JOIN instruments i ON i.id = up.instrument_id
                WHERE ABS(up.total_qty) > 1e-12
                ORDER BY i.id
                """
            )
        return [
            {
                "instrument_id": int(r["instrument_id"]),
                "secid": r["secid"],
                "boardid": r["boardid"],
                "asset_type": r["asset_type"],
            }
            for r in rows
        ]
    except Exception:
        logger.exception("Failed list_active_position_instruments")
        raise


async def clear_user_portfolio(db_path: str, user_id: int) -> int:
    """
    Deletes all trades and aggregated positions for user's default portfolio.
    Returns count of deleted trades.
    """
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, portfolio_id = await _get_user_context(conn, int(user_id))
                if user_ref_id is None or portfolio_id is None:
                    return 0
                deleted_rows = await conn.fetch(
                    """
                    DELETE FROM trades
                    WHERE user_id = $1
                      AND portfolio_id = $2
                    RETURNING id
                    """,
                    int(user_id),
                    portfolio_id,
                )
                await conn.execute(
                    """
                    DELETE FROM user_positions
                    WHERE portfolio_id = $1
                    """,
                    portfolio_id,
                )
                await conn.execute(
                    """
                    DELETE FROM price_alert_state
                    WHERE user_id = $1
                    """,
                    int(user_id),
                )
                return len(deleted_rows)
    except Exception:
        logger.exception("Failed clear_user_portfolio user=%s", user_id)
        raise


async def upsert_price_cache(db_path: str, instrument_id: int, last_price: float, updated_at: datetime | None = None) -> None:
    try:
        pool = await _get_pool(db_path)
        ts = updated_at or datetime.now(timezone.utc)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO price_cache (instrument_id, last_price, updated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (instrument_id) DO UPDATE
                SET last_price = EXCLUDED.last_price,
                    updated_at = EXCLUDED.updated_at
                """,
                int(instrument_id),
                float(last_price),
                ts,
            )
    except Exception:
        logger.exception("Failed upsert_price_cache instrument=%s", instrument_id)
        raise


async def get_price_cache_map(db_path: str, instrument_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not instrument_ids:
        return {}
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT instrument_id, last_price, updated_at
                FROM price_cache
                WHERE instrument_id = ANY($1::bigint[])
                """,
                [int(x) for x in instrument_ids],
            )
        return {
            int(r["instrument_id"]): {
                "last_price": float(r["last_price"]),
                "updated_at": r["updated_at"],
            }
            for r in rows
        }
    except Exception:
        logger.exception("Failed get_price_cache_map for %s instruments", len(instrument_ids))
        raise


async def ensure_user_alert_settings(db_path: str, user_id: int):
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                await conn.execute(
                    """
                    INSERT INTO user_alert_settings (user_id, user_ref_id)
                    VALUES ($1, $2)
                    ON CONFLICT(user_id) DO UPDATE SET user_ref_id = EXCLUDED.user_ref_id
                    """,
                    int(user_id),
                    user_ref_id,
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
            user_ref_id, _ = await _get_user_context(conn, int(user_id))
            await conn.execute(
                """
                INSERT INTO price_alert_state (user_id, user_ref_id, instrument_id, was_below, last_alert_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT(user_id, instrument_id) DO UPDATE SET
                  user_ref_id=EXCLUDED.user_ref_id,
                  was_below=EXCLUDED.was_below,
                  last_alert_at=EXCLUDED.last_alert_at
                """,
                int(user_id),
                user_ref_id,
                int(instrument_id),
                bool(was_below),
                alert_ts,
            )
    except Exception:
        logger.exception("Failed set_price_alert_state user=%s instrument=%s", user_id, instrument_id)
        raise
