import asyncio
import json
import hashlib
import logging
from datetime import date, datetime, timezone
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
  trade_date_date DATE,
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
  periodic_last_sent_at_ts TIMESTAMPTZ,
  drop_alert_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  drop_percent DOUBLE PRECISION NOT NULL DEFAULT 10,
  open_close_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  open_last_sent_date TEXT,
  open_last_sent_on DATE,
  midday_last_sent_date TEXT,
  midday_last_sent_on DATE,
  main_close_last_sent_date TEXT,
  main_close_last_sent_on DATE,
  close_last_sent_date TEXT,
  close_last_sent_on DATE,
  day_open_value DOUBLE PRECISION,
  day_open_value_date TEXT,
  day_open_value_on DATE
);


CREATE TABLE IF NOT EXISTS price_alert_state (
  user_id BIGINT NOT NULL,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  instrument_id BIGINT NOT NULL,
  was_below BOOLEAN NOT NULL DEFAULT FALSE,
  last_alert_at TEXT,
  last_alert_at_ts TIMESTAMPTZ,
  PRIMARY KEY (user_id, instrument_id)
);

CREATE TABLE IF NOT EXISTS price_target_alerts (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  instrument_id BIGINT NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
  target_price DOUBLE PRECISION NOT NULL,
  range_percent DOUBLE PRECISION NOT NULL DEFAULT 5,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  last_sent_at TEXT,
  last_sent_at_ts TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, instrument_id, target_price, range_percent)
);

CREATE TABLE IF NOT EXISTS app_texts (
  id BIGSERIAL PRIMARY KEY,
  text_code TEXT NOT NULL UNIQUE,
  button_name TEXT,
  value TEXT NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS user_modes (
  user_id BIGINT PRIMARY KEY,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  last_mode TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS budget_profiles (
  user_id BIGINT PRIMARY KEY,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  onboarding_mode TEXT,
  income_type TEXT NOT NULL DEFAULT 'fixed',
  income_monthly DOUBLE PRECISION NOT NULL DEFAULT 0,
  payday_day INTEGER,
  expenses_base DOUBLE PRECISION NOT NULL DEFAULT 0,
  onboarding_completed BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS budget_incomes (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  kind TEXT NOT NULL DEFAULT 'other',
  title TEXT NOT NULL,
  amount_monthly DOUBLE PRECISION NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS budget_expenses (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  kind TEXT NOT NULL DEFAULT 'other',
  title TEXT NOT NULL,
  amount_monthly DOUBLE PRECISION NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS budget_obligations (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'other',
  amount_monthly DOUBLE PRECISION NOT NULL,
  debt_details JSONB NOT NULL DEFAULT '{}'::jsonb,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS budget_savings (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  kind TEXT NOT NULL DEFAULT 'other',
  title TEXT NOT NULL,
  amount DOUBLE PRECISION NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS budget_funds (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  target_amount DOUBLE PRECISION NOT NULL,
  already_saved DOUBLE PRECISION NOT NULL DEFAULT 0,
  target_month TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  priority TEXT NOT NULL DEFAULT 'medium',
  status TEXT NOT NULL DEFAULT 'active',
  autopilot_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS budget_month_closes (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  month_key TEXT NOT NULL,
  planned_expenses_base DOUBLE PRECISION NOT NULL DEFAULT 0,
  actual_expenses_base DOUBLE PRECISION NOT NULL DEFAULT 0,
  extra_income_total DOUBLE PRECISION NOT NULL DEFAULT 0,
  extra_income_items JSONB NOT NULL DEFAULT '[]'::jsonb,
  closed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, month_key)
);

CREATE TABLE IF NOT EXISTS budget_notification_settings (
  user_id BIGINT PRIMARY KEY,
  user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  budget_summary_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  goal_deadline_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  month_close_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

"""

MIGRATION_SQL = [
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS user_ref_id BIGINT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS portfolio_id BIGINT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS external_trade_id TEXT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS import_source TEXT",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS user_ref_id BIGINT",
    "ALTER TABLE price_alert_state ADD COLUMN IF NOT EXISTS user_ref_id BIGINT",
    "ALTER TABLE app_texts ADD COLUMN IF NOT EXISTS text_code TEXT",
    "ALTER TABLE app_texts ADD COLUMN IF NOT EXISTS button_name TEXT",
    "ALTER TABLE app_texts ADD COLUMN IF NOT EXISTS value TEXT",
    "ALTER TABLE app_texts ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS day_open_value DOUBLE PRECISION",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS day_open_value_date TEXT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS trade_date_date DATE",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS periodic_last_sent_at_ts TIMESTAMPTZ",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS open_last_sent_on DATE",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS midday_last_sent_date TEXT",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS midday_last_sent_on DATE",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS main_close_last_sent_date TEXT",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS main_close_last_sent_on DATE",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS close_last_sent_on DATE",
    "ALTER TABLE user_alert_settings ADD COLUMN IF NOT EXISTS day_open_value_on DATE",
    "ALTER TABLE price_alert_state ADD COLUMN IF NOT EXISTS last_alert_at_ts TIMESTAMPTZ",
    "CREATE TABLE IF NOT EXISTS price_target_alerts (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE, instrument_id BIGINT NOT NULL REFERENCES instruments(id) ON DELETE CASCADE, target_price DOUBLE PRECISION NOT NULL, range_percent DOUBLE PRECISION NOT NULL DEFAULT 5, enabled BOOLEAN NOT NULL DEFAULT TRUE, last_sent_at TEXT, last_sent_at_ts TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "ALTER TABLE price_target_alerts ADD COLUMN IF NOT EXISTS user_ref_id BIGINT",
    "ALTER TABLE price_target_alerts ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE",
    "ALTER TABLE price_target_alerts ADD COLUMN IF NOT EXISTS last_sent_at TEXT",
    "ALTER TABLE price_target_alerts ADD COLUMN IF NOT EXISTS last_sent_at_ts TIMESTAMPTZ",
    "ALTER TABLE price_target_alerts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
    "ALTER TABLE price_target_alerts ADD COLUMN IF NOT EXISTS range_percent DOUBLE PRECISION NOT NULL DEFAULT 5",
    "CREATE TABLE IF NOT EXISTS user_modes (user_id BIGINT PRIMARY KEY, user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE, last_mode TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS budget_profiles (user_id BIGINT PRIMARY KEY, user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE, onboarding_mode TEXT, income_type TEXT NOT NULL DEFAULT 'fixed', income_monthly DOUBLE PRECISION NOT NULL DEFAULT 0, payday_day INTEGER, expenses_base DOUBLE PRECISION NOT NULL DEFAULT 0, onboarding_completed BOOLEAN NOT NULL DEFAULT FALSE, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS budget_incomes (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE, kind TEXT NOT NULL DEFAULT 'other', title TEXT NOT NULL, amount_monthly DOUBLE PRECISION NOT NULL, active BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS budget_expenses (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE, kind TEXT NOT NULL DEFAULT 'other', title TEXT NOT NULL, amount_monthly DOUBLE PRECISION NOT NULL, payload JSONB NOT NULL DEFAULT '{}'::jsonb, active BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS budget_obligations (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE, title TEXT NOT NULL, kind TEXT NOT NULL DEFAULT 'other', amount_monthly DOUBLE PRECISION NOT NULL, debt_details JSONB NOT NULL DEFAULT '{}'::jsonb, active BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS budget_savings (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE, kind TEXT NOT NULL DEFAULT 'other', title TEXT NOT NULL, amount DOUBLE PRECISION NOT NULL, active BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS budget_funds (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE, title TEXT NOT NULL, target_amount DOUBLE PRECISION NOT NULL, already_saved DOUBLE PRECISION NOT NULL DEFAULT 0, target_month TEXT NOT NULL, payload JSONB NOT NULL DEFAULT '{}'::jsonb, priority TEXT NOT NULL DEFAULT 'medium', status TEXT NOT NULL DEFAULT 'active', autopilot_enabled BOOLEAN NOT NULL DEFAULT FALSE, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "ALTER TABLE budget_funds ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb",
    "CREATE TABLE IF NOT EXISTS budget_month_closes (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE, month_key TEXT NOT NULL, planned_expenses_base DOUBLE PRECISION NOT NULL DEFAULT 0, actual_expenses_base DOUBLE PRECISION NOT NULL DEFAULT 0, extra_income_total DOUBLE PRECISION NOT NULL DEFAULT 0, extra_income_items JSONB NOT NULL DEFAULT '[]'::jsonb, closed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE (user_id, month_key))",
    "CREATE TABLE IF NOT EXISTS budget_notification_settings (user_id BIGINT PRIMARY KEY, user_ref_id BIGINT REFERENCES users(id) ON DELETE CASCADE, budget_summary_enabled BOOLEAN NOT NULL DEFAULT TRUE, goal_deadline_enabled BOOLEAN NOT NULL DEFAULT TRUE, month_close_enabled BOOLEAN NOT NULL DEFAULT TRUE, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
]

POST_MIGRATION_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS ix_trades_user_ref_instrument ON trades (user_ref_id, instrument_id)",
    "CREATE INDEX IF NOT EXISTS ix_trades_portfolio_instrument ON trades (portfolio_id, instrument_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_trades_user_external_trade_id ON trades (user_id, external_trade_id)",
    "CREATE INDEX IF NOT EXISTS ix_user_alert_settings_user_ref ON user_alert_settings (user_ref_id)",
    "CREATE INDEX IF NOT EXISTS ix_price_alert_state_user_ref_instrument ON price_alert_state (user_ref_id, instrument_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_app_texts_text_code ON app_texts (text_code)",
    "CREATE INDEX IF NOT EXISTS ix_app_texts_button_name_active ON app_texts (button_name, active)",
    "CREATE INDEX IF NOT EXISTS ix_price_target_alerts_user_enabled ON price_target_alerts (user_id, enabled)",
    "CREATE INDEX IF NOT EXISTS ix_price_target_alerts_instr_enabled ON price_target_alerts (instrument_id, enabled)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_price_target_alerts_unique ON price_target_alerts (user_id, instrument_id, target_price, range_percent)",
    "CREATE INDEX IF NOT EXISTS ix_user_modes_user_ref ON user_modes (user_ref_id)",
    "CREATE INDEX IF NOT EXISTS ix_budget_incomes_user_active ON budget_incomes (user_id, active)",
    "CREATE INDEX IF NOT EXISTS ix_budget_expenses_user_active ON budget_expenses (user_id, active)",
    "CREATE INDEX IF NOT EXISTS ix_budget_obligations_user_active ON budget_obligations (user_id, active)",
    "CREATE INDEX IF NOT EXISTS ix_budget_savings_user_active ON budget_savings (user_id, active)",
    "CREATE INDEX IF NOT EXISTS ix_budget_funds_user_status ON budget_funds (user_id, status)",
    "CREATE INDEX IF NOT EXISTS ix_budget_month_closes_user_month ON budget_month_closes (user_id, month_key)",
    "CREATE INDEX IF NOT EXISTS ix_budget_notification_settings_user_ref ON budget_notification_settings (user_ref_id)",
]

_pools: dict[str, asyncpg.Pool] = {}
_pools_lock = asyncio.Lock()
_single_instance_lock_conn: asyncpg.Connection | None = None
_single_instance_lock_key: int | None = None


def _norm_boardid(boardid: str | None) -> str:
    return (boardid or "").strip()


def _parse_date_iso(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _parse_date_ddmmyyyy(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%d.%m.%Y").date()
    except ValueError:
        return None


def _parse_iso_utc(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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
        INSERT INTO users (telegram_user_id)
        SELECT DISTINCT user_id FROM price_target_alerts
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
    await conn.execute(
        """
        UPDATE price_target_alerts s
        SET user_ref_id = u.id
        FROM users u
        WHERE s.user_ref_id IS NULL
          AND u.telegram_user_id = s.user_id
        """
    )
    await conn.execute(
        """
        UPDATE trades
        SET trade_date_date = TO_DATE(trade_date, 'DD.MM.YYYY')
        WHERE trade_date_date IS NULL
          AND trade_date ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$'
        """
    )
    await conn.execute(
        """
        UPDATE user_alert_settings
        SET periodic_last_sent_at_ts = periodic_last_sent_at::timestamptz
        WHERE periodic_last_sent_at_ts IS NULL
          AND periodic_last_sent_at IS NOT NULL
          AND periodic_last_sent_at ~ '^\\d{4}-\\d{2}-\\d{2}T'
        """
    )
    await conn.execute(
        """
        UPDATE user_alert_settings
        SET open_last_sent_on = open_last_sent_date::date
        WHERE open_last_sent_on IS NULL
          AND open_last_sent_date IS NOT NULL
          AND open_last_sent_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
        """
    )
    await conn.execute(
        """
        UPDATE user_alert_settings
        SET close_last_sent_on = close_last_sent_date::date
        WHERE close_last_sent_on IS NULL
          AND close_last_sent_date IS NOT NULL
          AND close_last_sent_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
        """
    )
    await conn.execute(
        """
        UPDATE user_alert_settings
        SET midday_last_sent_on = midday_last_sent_date::date
        WHERE midday_last_sent_on IS NULL
          AND midday_last_sent_date IS NOT NULL
          AND midday_last_sent_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
        """
    )
    await conn.execute(
        """
        UPDATE user_alert_settings
        SET main_close_last_sent_on = main_close_last_sent_date::date
        WHERE main_close_last_sent_on IS NULL
          AND main_close_last_sent_date IS NOT NULL
          AND main_close_last_sent_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
        """
    )
    await conn.execute(
        """
        UPDATE user_alert_settings
        SET day_open_value_on = day_open_value_date::date
        WHERE day_open_value_on IS NULL
          AND day_open_value_date IS NOT NULL
          AND day_open_value_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
        """
    )
    await conn.execute(
        """
        UPDATE price_alert_state
        SET last_alert_at_ts = last_alert_at::timestamptz
        WHERE last_alert_at_ts IS NULL
          AND last_alert_at IS NOT NULL
          AND last_alert_at ~ '^\\d{4}-\\d{2}-\\d{2}T'
        """
    )
    await conn.execute(
        """
        UPDATE price_target_alerts
        SET last_sent_at_ts = last_sent_at::timestamptz
        WHERE last_sent_at_ts IS NULL
          AND last_sent_at IS NOT NULL
          AND last_sent_at ~ '^\\d{4}-\\d{2}-\\d{2}T'
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
            await conn.execute(
                """
                INSERT INTO price_cache (instrument_id, last_price, updated_at)
                SELECT $1, pc.last_price, pc.updated_at
                FROM price_cache pc
                WHERE pc.instrument_id = $2
                ON CONFLICT (instrument_id) DO UPDATE
                SET last_price = CASE
                      WHEN EXCLUDED.updated_at >= price_cache.updated_at THEN EXCLUDED.last_price
                      ELSE price_cache.last_price
                    END,
                    updated_at = GREATEST(price_cache.updated_at, EXCLUDED.updated_at)
                """,
                canonical_id,
                dup_id,
            )
            await conn.execute(
                """
                WITH moved AS (
                  DELETE FROM price_alert_state
                  WHERE instrument_id = $2
                  RETURNING user_id, user_ref_id, was_below, last_alert_at, last_alert_at_ts
                )
                INSERT INTO price_alert_state (user_id, user_ref_id, instrument_id, was_below, last_alert_at, last_alert_at_ts)
                SELECT m.user_id, m.user_ref_id, $1, m.was_below, m.last_alert_at, m.last_alert_at_ts
                FROM moved m
                ON CONFLICT (user_id, instrument_id) DO UPDATE
                SET user_ref_id = COALESCE(price_alert_state.user_ref_id, EXCLUDED.user_ref_id),
                    was_below = price_alert_state.was_below OR EXCLUDED.was_below,
                    last_alert_at = CASE
                      WHEN EXCLUDED.last_alert_at_ts IS NULL THEN price_alert_state.last_alert_at
                      WHEN price_alert_state.last_alert_at_ts IS NULL THEN EXCLUDED.last_alert_at
                      WHEN EXCLUDED.last_alert_at_ts >= price_alert_state.last_alert_at_ts THEN EXCLUDED.last_alert_at
                      ELSE price_alert_state.last_alert_at
                    END,
                    last_alert_at_ts = CASE
                      WHEN price_alert_state.last_alert_at_ts IS NULL THEN EXCLUDED.last_alert_at_ts
                      WHEN EXCLUDED.last_alert_at_ts IS NULL THEN price_alert_state.last_alert_at_ts
                      ELSE GREATEST(price_alert_state.last_alert_at_ts, EXCLUDED.last_alert_at_ts)
                    END
                """,
                canonical_id,
                dup_id,
            )
            await conn.execute("UPDATE trades SET instrument_id = $1 WHERE instrument_id = $2", canonical_id, dup_id)
            await conn.execute("DELETE FROM user_positions WHERE instrument_id = $1", dup_id)
            await conn.execute("DELETE FROM price_cache WHERE instrument_id = $1", dup_id)
            await conn.execute("DELETE FROM instruments WHERE id = $1", dup_id)

        logger.info(
            "Merged duplicate metal instruments secid=%s canonical_id=%s removed=%s",
            secid,
            canonical_id,
            dup_ids,
        )

    if merged_any:
        logger.info("Metal deduplication finished with merges")


async def _run_one_time_maintenance(conn: asyncpg.Connection, key: str) -> None:
    await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"schema_meta:{key}")
    already = await conn.fetchval("SELECT 1 FROM schema_meta WHERE key = $1", key)
    if already:
        return
    await _backfill_user_links(conn)
    await _deduplicate_metal_instruments(conn)
    await _rebuild_positions(conn)
    await conn.execute(
        """
        INSERT INTO schema_meta (key, value, updated_at)
        VALUES ($1, 'done', NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at
        """,
        key,
    )


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
                await _run_one_time_maintenance(conn, "maintenance_v2_done")
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
        trade_date_parsed = _parse_date_ddmmyyyy(trade_date) or _parse_date_iso(trade_date)
        inserted = False
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, portfolio_id = await _ensure_user_context(conn, int(user_id))
                row = await conn.fetchrow(
                    """
                    INSERT INTO trades (
                      user_id, user_ref_id, portfolio_id, instrument_id,
                      external_trade_id, import_source, trade_date, trade_date_date, qty, price, commission
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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
                    trade_date_parsed,
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


async def ensure_app_text(
    db_path: str,
    text_code: str,
    value: str,
    active: bool = True,
    button_name: str | None = None,
) -> None:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO app_texts (text_code, button_name, value, active)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (text_code) DO UPDATE
                SET button_name = COALESCE(app_texts.button_name, EXCLUDED.button_name)
                """,
                str(text_code).strip(),
                (str(button_name).strip() if button_name else None),
                str(value),
                bool(active),
            )
    except Exception:
        logger.exception("Failed ensure_app_text text_code=%s", text_code)
        raise


async def get_active_app_text(db_path: str, text_code: str) -> str | None:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT value
                FROM app_texts
                WHERE text_code = $1
                  AND active = TRUE
                LIMIT 1
                """,
                str(text_code).strip(),
            )
            if not row:
                return None
            return str(row["value"])
    except Exception:
        logger.exception("Failed get_active_app_text text_code=%s", text_code)
        raise


async def list_active_app_texts(db_path: str) -> list[dict[str, str]]:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT text_code, button_name
                FROM app_texts
                WHERE active = TRUE
                  AND COALESCE(NULLIF(button_name, ''), '') <> ''
                ORDER BY id
                """
            )
        return [
            {
                "text_code": str(r["text_code"]),
                "button_name": str(r["button_name"]),
            }
            for r in rows
        ]
    except Exception:
        logger.exception("Failed list_active_app_texts")
        raise


async def upsert_price_cache_bulk(
    db_path: str,
    rows: list[tuple[int, float]],
    updated_at: datetime | None = None,
) -> None:
    if not rows:
        return
    try:
        pool = await _get_pool(db_path)
        ts = updated_at or datetime.now(timezone.utc)
        dedup: dict[int, float] = {}
        for instrument_id, last_price in rows:
            dedup[int(instrument_id)] = float(last_price)
        instrument_ids = list(dedup.keys())
        last_prices = [dedup[iid] for iid in instrument_ids]
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO price_cache (instrument_id, last_price, updated_at)
                SELECT x.instrument_id, x.last_price, $3
                FROM UNNEST($1::bigint[], $2::double precision[]) AS x(instrument_id, last_price)
                ON CONFLICT (instrument_id) DO UPDATE
                SET last_price = EXCLUDED.last_price,
                    updated_at = EXCLUDED.updated_at
                """,
                instrument_ids,
                last_prices,
                ts,
            )
    except Exception:
        logger.exception("Failed upsert_price_cache_bulk rows=%s", len(rows))
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
                        periodic_last_sent_at=NULL,
                        periodic_last_sent_at_ts=NULL
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
                        periodic_last_sent_at=NULL,
                        periodic_last_sent_at_ts=NULL
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
                    open_last_sent_on=NULL,
                    midday_last_sent_date=NULL,
                    midday_last_sent_on=NULL,
                    main_close_last_sent_date=NULL,
                    main_close_last_sent_on=NULL,
                    close_last_sent_date=NULL,
                    close_last_sent_on=NULL,
                    day_open_value=NULL,
                    day_open_value_date=NULL,
                    day_open_value_on=NULL
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
                  periodic_last_sent_at_ts,
                  drop_alert_enabled,
                  drop_percent,
                  open_close_enabled,
                  open_last_sent_date,
                  open_last_sent_on,
                  midday_last_sent_date,
                  midday_last_sent_on,
                  main_close_last_sent_date,
                  main_close_last_sent_on,
                  close_last_sent_date,
                  close_last_sent_on,
                  day_open_value,
                  day_open_value_date,
                  day_open_value_on
                FROM user_alert_settings
                WHERE user_id=$1
                """,
                int(user_id),
            )
        periodic_ts = row["periodic_last_sent_at_ts"]
        periodic_last_sent_at = periodic_ts.isoformat() if periodic_ts is not None else row["periodic_last_sent_at"]
        open_on = row["open_last_sent_on"]
        midday_on = row["midday_last_sent_on"]
        main_close_on = row["main_close_last_sent_on"]
        close_on = row["close_last_sent_on"]
        day_open_on = row["day_open_value_on"]
        return {
            "user_id": int(row["user_id"]),
            "periodic_enabled": bool(row["periodic_enabled"]),
            "periodic_interval_min": int(row["periodic_interval_min"]),
            "periodic_last_sent_at": periodic_last_sent_at,
            "drop_alert_enabled": bool(row["drop_alert_enabled"]),
            "drop_percent": float(row["drop_percent"]),
            "open_close_enabled": bool(row["open_close_enabled"]),
            "open_last_sent_date": open_on.isoformat() if open_on is not None else row["open_last_sent_date"],
            "midday_last_sent_date": (
                midday_on.isoformat() if midday_on is not None else row["midday_last_sent_date"]
            ),
            "main_close_last_sent_date": (
                main_close_on.isoformat() if main_close_on is not None else row["main_close_last_sent_date"]
            ),
            "close_last_sent_date": close_on.isoformat() if close_on is not None else row["close_last_sent_date"],
            "day_open_value": (float(row["day_open_value"]) if row["day_open_value"] is not None else None),
            "day_open_value_date": day_open_on.isoformat() if day_open_on is not None else row["day_open_value_date"],
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
                UNION
                SELECT DISTINCT user_id
                FROM price_target_alerts
                WHERE enabled=TRUE
                """
            )
        return [int(r["user_id"]) for r in rows]
    except Exception:
        logger.exception("Failed list_users_with_alerts")
        raise


async def update_periodic_last_sent_at(db_path: str, user_id: int, iso_ts: str):
    try:
        pool = await _get_pool(db_path)
        dt = _parse_iso_utc(iso_ts)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE user_alert_settings
                SET periodic_last_sent_at=$1,
                    periodic_last_sent_at_ts=$2
                WHERE user_id=$3
                """,
                iso_ts,
                dt,
                int(user_id),
            )
    except Exception:
        logger.exception("Failed update_periodic_last_sent_at user=%s", user_id)
        raise


async def update_open_sent_date(db_path: str, user_id: int, date_iso: str):
    try:
        pool = await _get_pool(db_path)
        day = _parse_date_iso(date_iso)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE user_alert_settings
                SET open_last_sent_date=$1,
                    open_last_sent_on=$2
                WHERE user_id=$3
                """,
                date_iso,
                day,
                int(user_id),
            )
    except Exception:
        logger.exception("Failed update_open_sent_date user=%s", user_id)
        raise


async def update_midday_sent_date(db_path: str, user_id: int, date_iso: str):
    try:
        pool = await _get_pool(db_path)
        day = _parse_date_iso(date_iso)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE user_alert_settings
                SET midday_last_sent_date=$1,
                    midday_last_sent_on=$2
                WHERE user_id=$3
                """,
                date_iso,
                day,
                int(user_id),
            )
    except Exception:
        logger.exception("Failed update_midday_sent_date user=%s", user_id)
        raise


async def update_main_close_sent_date(db_path: str, user_id: int, date_iso: str):
    try:
        pool = await _get_pool(db_path)
        day = _parse_date_iso(date_iso)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE user_alert_settings
                SET main_close_last_sent_date=$1,
                    main_close_last_sent_on=$2
                WHERE user_id=$3
                """,
                date_iso,
                day,
                int(user_id),
            )
    except Exception:
        logger.exception("Failed update_main_close_sent_date user=%s", user_id)
        raise


async def update_close_sent_date(db_path: str, user_id: int, date_iso: str):
    try:
        pool = await _get_pool(db_path)
        day = _parse_date_iso(date_iso)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE user_alert_settings
                SET close_last_sent_date=$1,
                    close_last_sent_on=$2
                WHERE user_id=$3
                """,
                date_iso,
                day,
                int(user_id),
            )
    except Exception:
        logger.exception("Failed update_close_sent_date user=%s", user_id)
        raise


async def update_day_open_value(db_path: str, user_id: int, date_iso: str, open_value: float | None) -> None:
    try:
        pool = await _get_pool(db_path)
        day = _parse_date_iso(date_iso)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE user_alert_settings
                SET day_open_value=$1,
                    day_open_value_date=$2,
                    day_open_value_on=$3
                WHERE user_id=$4
                """,
                (float(open_value) if open_value is not None else None),
                date_iso,
                day,
                int(user_id),
            )
    except Exception:
        logger.exception("Failed update_day_open_value user=%s", user_id)
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


async def get_price_alert_states_bulk(db_path: str, user_id: int, instrument_ids: list[int]) -> dict[int, bool]:
    if not instrument_ids:
        return {}
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT instrument_id, was_below
                FROM price_alert_state
                WHERE user_id = $1
                  AND instrument_id = ANY($2::bigint[])
                """,
                int(user_id),
                [int(iid) for iid in instrument_ids],
            )
        return {int(row["instrument_id"]): bool(row["was_below"]) for row in rows}
    except Exception:
        logger.exception("Failed get_price_alert_states_bulk user=%s count=%s", user_id, len(instrument_ids))
        raise


async def set_price_alert_state(db_path: str, user_id: int, instrument_id: int, was_below: bool, alert_ts: str | None = None):
    try:
        pool = await _get_pool(db_path)
        alert_dt = _parse_iso_utc(alert_ts)
        async with pool.acquire() as conn:
            user_ref_id, _ = await _get_user_context(conn, int(user_id))
            await conn.execute(
                """
                INSERT INTO price_alert_state (user_id, user_ref_id, instrument_id, was_below, last_alert_at, last_alert_at_ts)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT(user_id, instrument_id) DO UPDATE SET
                  user_ref_id=EXCLUDED.user_ref_id,
                  was_below=EXCLUDED.was_below,
                  last_alert_at=EXCLUDED.last_alert_at,
                  last_alert_at_ts=EXCLUDED.last_alert_at_ts
                """,
                int(user_id),
                user_ref_id,
                int(instrument_id),
                bool(was_below),
                alert_ts,
                alert_dt,
            )
    except Exception:
        logger.exception("Failed set_price_alert_state user=%s instrument=%s", user_id, instrument_id)
        raise


async def set_price_alert_states_bulk(
    db_path: str,
    user_id: int,
    updates: list[tuple[int, bool, str | None]],
) -> None:
    if not updates:
        return
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            user_ref_id, _ = await _get_user_context(conn, int(user_id))
            if user_ref_id is None:
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
            instrument_ids: list[int] = []
            was_below_values: list[bool] = []
            alert_text_values: list[str | None] = []
            alert_ts_values: list[datetime | None] = []
            for instrument_id, was_below, alert_ts in updates:
                instrument_ids.append(int(instrument_id))
                was_below_values.append(bool(was_below))
                alert_text_values.append(alert_ts)
                alert_ts_values.append(_parse_iso_utc(alert_ts))
            await conn.execute(
                """
                INSERT INTO price_alert_state (user_id, user_ref_id, instrument_id, was_below, last_alert_at, last_alert_at_ts)
                SELECT
                  $1::bigint,
                  $2::bigint,
                  x.instrument_id,
                  x.was_below,
                  x.last_alert_at,
                  x.last_alert_at_ts
                FROM UNNEST(
                  $3::bigint[],
                  $4::boolean[],
                  $5::text[],
                  $6::timestamptz[]
                ) AS x(instrument_id, was_below, last_alert_at, last_alert_at_ts)
                ON CONFLICT(user_id, instrument_id) DO UPDATE SET
                  user_ref_id = EXCLUDED.user_ref_id,
                  was_below = EXCLUDED.was_below,
                  last_alert_at = EXCLUDED.last_alert_at,
                  last_alert_at_ts = EXCLUDED.last_alert_at_ts
                """,
                int(user_id),
                int(user_ref_id),
                instrument_ids,
                was_below_values,
                alert_text_values,
                alert_ts_values,
            )
    except Exception:
        logger.exception("Failed set_price_alert_states_bulk user=%s count=%s", user_id, len(updates))
        raise


async def create_price_target_alert(
    db_path: str,
    user_id: int,
    instrument_id: int,
    target_price: float,
    range_percent: float = 5.0,
) -> int:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                row = await conn.fetchrow(
                    """
                    INSERT INTO price_target_alerts (
                      user_id, user_ref_id, instrument_id, target_price, range_percent, enabled, last_sent_at, last_sent_at_ts
                    )
                    VALUES ($1, $2, $3, $4, $5, TRUE, NULL, NULL)
                    ON CONFLICT (user_id, instrument_id, target_price, range_percent)
                    DO UPDATE SET enabled = TRUE
                    RETURNING id
                    """,
                    int(user_id),
                    user_ref_id,
                    int(instrument_id),
                    float(target_price),
                    float(range_percent),
                )
        return int(row["id"])
    except Exception:
        logger.exception(
            "Failed create_price_target_alert user=%s instrument=%s target=%s range=%s",
            user_id,
            instrument_id,
            target_price,
            range_percent,
        )
        raise


async def list_active_price_target_alerts(db_path: str, user_id: int) -> list[dict[str, Any]]:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                  a.id,
                  a.user_id,
                  a.instrument_id,
                  a.target_price,
                  a.range_percent,
                  a.last_sent_at,
                  a.last_sent_at_ts,
                  i.secid,
                  i.boardid,
                  i.shortname,
                  COALESCE(i.asset_type, 'stock') AS asset_type
                FROM price_target_alerts a
                JOIN instruments i ON i.id = a.instrument_id
                WHERE a.user_id = $1
                  AND a.enabled = TRUE
                ORDER BY a.id
                """,
                int(user_id),
            )
        out: list[dict[str, Any]] = []
        for row in rows:
            ts = row["last_sent_at_ts"]
            out.append(
                {
                    "id": int(row["id"]),
                    "user_id": int(row["user_id"]),
                    "instrument_id": int(row["instrument_id"]),
                    "target_price": float(row["target_price"]),
                    "range_percent": float(row["range_percent"]),
                    "last_sent_at": ts.isoformat() if ts is not None else row["last_sent_at"],
                    "secid": row["secid"],
                    "boardid": row["boardid"],
                    "shortname": row["shortname"],
                    "asset_type": row["asset_type"],
                }
            )
        return out
    except Exception:
        logger.exception("Failed list_active_price_target_alerts user=%s", user_id)
        raise


async def update_price_target_alert_last_sent(db_path: str, alert_id: int, iso_ts: str) -> None:
    try:
        pool = await _get_pool(db_path)
        dt = _parse_iso_utc(iso_ts)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE price_target_alerts
                SET last_sent_at = $1,
                    last_sent_at_ts = $2
                WHERE id = $3
                """,
                iso_ts,
                dt,
                int(alert_id),
            )
    except Exception:
        logger.exception("Failed update_price_target_alert_last_sent alert=%s", alert_id)
        raise


async def disable_price_target_alert(db_path: str, user_id: int, alert_id: int) -> bool:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE price_target_alerts
                SET enabled = FALSE
                WHERE id = $1
                  AND user_id = $2
                  AND enabled = TRUE
                RETURNING id
                """,
                int(alert_id),
                int(user_id),
            )
        return row is not None
    except Exception:
        logger.exception("Failed disable_price_target_alert user=%s alert=%s", user_id, alert_id)
        raise


def _month_key_for(dt: date) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def _next_month_key(month_key: str) -> str:
    year, month = [int(x) for x in month_key.split("-", 1)]
    if month == 12:
        return f"{year + 1:04d}-01"
    return f"{year:04d}-{month + 1:02d}"


def _month_distance(from_key: str, to_key: str) -> int:
    fy, fm = [int(x) for x in from_key.split("-", 1)]
    ty, tm = [int(x) for x in to_key.split("-", 1)]
    return (ty - fy) * 12 + (tm - fm)


def _safe_month_key(raw: str | None, fallback: str) -> str:
    text = (raw or "").strip()
    if len(text) == 7 and text[4] == "-":
        try:
            y = int(text[:4])
            m = int(text[5:7])
            if 1 <= m <= 12 and 1970 <= y <= 3000:
                return f"{y:04d}-{m:02d}"
        except ValueError:
            return fallback
    return fallback


def _fund_metrics(target_amount: float, already_saved: float, target_month: str, now_key: str) -> dict[str, Any]:
    need = max(0.0, float(target_amount) - float(already_saved))
    months_left = max(1, _month_distance(now_key, target_month))
    required = need / months_left if need > 0 else 0.0
    return {
        "need": need,
        "months_left": months_left,
        "required_per_month": required,
    }


async def get_user_last_mode(db_path: str, user_id: int) -> str | None:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT last_mode FROM user_modes WHERE user_id = $1", int(user_id))
        if not row:
            return None
        mode = (row["last_mode"] or "").strip().lower()
        return mode if mode in {"exchange", "budget"} else None
    except Exception:
        logger.exception("Failed get_user_last_mode user=%s", user_id)
        raise


async def set_user_last_mode(db_path: str, user_id: int, mode: str) -> str:
    normalized = (mode or "").strip().lower()
    if normalized not in {"exchange", "budget"}:
        raise ValueError("mode must be exchange or budget")
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                await conn.execute(
                    """
                    INSERT INTO user_modes (user_id, user_ref_id, last_mode, updated_at)
                    VALUES ($1, $2, $3, NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET user_ref_id = EXCLUDED.user_ref_id,
                        last_mode = EXCLUDED.last_mode,
                        updated_at = EXCLUDED.updated_at
                    """,
                    int(user_id),
                    int(user_ref_id),
                    normalized,
                )
        return normalized
    except Exception:
        logger.exception("Failed set_user_last_mode user=%s mode=%s", user_id, normalized)
        raise


async def get_budget_profile(db_path: str, user_id: int) -> dict[str, Any]:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT onboarding_mode, income_type, income_monthly, payday_day, expenses_base, onboarding_completed
                FROM budget_profiles
                WHERE user_id = $1
                """,
                int(user_id),
            )
        if not row:
            return {
                "onboarding_mode": None,
                "income_type": "fixed",
                "income_monthly": 0.0,
                "payday_day": None,
                "expenses_base": 0.0,
                "onboarding_completed": False,
            }
        return {
            "onboarding_mode": row["onboarding_mode"],
            "income_type": row["income_type"] or "fixed",
            "income_monthly": float(row["income_monthly"] or 0.0),
            "payday_day": int(row["payday_day"]) if row["payday_day"] is not None else None,
            "expenses_base": float(row["expenses_base"] or 0.0),
            "onboarding_completed": bool(row["onboarding_completed"]),
        }
    except Exception:
        logger.exception("Failed get_budget_profile user=%s", user_id)
        raise


async def upsert_budget_profile(
    db_path: str,
    user_id: int,
    onboarding_mode: str | None = None,
    income_type: str | None = None,
    income_monthly: float | None = None,
    payday_day: int | None = None,
    expenses_base: float | None = None,
    onboarding_completed: bool | None = None,
) -> dict[str, Any]:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                current = await conn.fetchrow(
                    """
                    SELECT onboarding_mode, income_type, income_monthly, payday_day, expenses_base, onboarding_completed
                    FROM budget_profiles
                    WHERE user_id = $1
                    """,
                    int(user_id),
                )
                curr = {
                    "onboarding_mode": current["onboarding_mode"] if current else None,
                    "income_type": (current["income_type"] if current else "fixed") or "fixed",
                    "income_monthly": float(current["income_monthly"] or 0.0) if current else 0.0,
                    "payday_day": int(current["payday_day"]) if current and current["payday_day"] is not None else None,
                    "expenses_base": float(current["expenses_base"] or 0.0) if current else 0.0,
                    "onboarding_completed": bool(current["onboarding_completed"]) if current else False,
                }
                if onboarding_mode is not None:
                    curr["onboarding_mode"] = onboarding_mode
                if income_type is not None:
                    curr["income_type"] = income_type
                if income_monthly is not None:
                    curr["income_monthly"] = float(income_monthly)
                if payday_day is not None:
                    curr["payday_day"] = int(payday_day)
                if expenses_base is not None:
                    curr["expenses_base"] = float(expenses_base)
                if onboarding_completed is not None:
                    curr["onboarding_completed"] = bool(onboarding_completed)
                row = await conn.fetchrow(
                    """
                    INSERT INTO budget_profiles (
                      user_id, user_ref_id, onboarding_mode, income_type, income_monthly, payday_day, expenses_base, onboarding_completed, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET user_ref_id = EXCLUDED.user_ref_id,
                        onboarding_mode = EXCLUDED.onboarding_mode,
                        income_type = EXCLUDED.income_type,
                        income_monthly = EXCLUDED.income_monthly,
                        payday_day = EXCLUDED.payday_day,
                        expenses_base = EXCLUDED.expenses_base,
                        onboarding_completed = EXCLUDED.onboarding_completed,
                        updated_at = EXCLUDED.updated_at
                    RETURNING onboarding_mode, income_type, income_monthly, payday_day, expenses_base, onboarding_completed
                    """,
                    int(user_id),
                    int(user_ref_id),
                    curr["onboarding_mode"],
                    curr["income_type"],
                    curr["income_monthly"],
                    curr["payday_day"],
                    curr["expenses_base"],
                    curr["onboarding_completed"],
                )
        return {
            "onboarding_mode": row["onboarding_mode"],
            "income_type": row["income_type"] or "fixed",
            "income_monthly": float(row["income_monthly"] or 0.0),
            "payday_day": int(row["payday_day"]) if row["payday_day"] is not None else None,
            "expenses_base": float(row["expenses_base"] or 0.0),
            "onboarding_completed": bool(row["onboarding_completed"]),
        }
    except Exception:
        logger.exception("Failed upsert_budget_profile user=%s", user_id)
        raise


async def list_budget_obligations(db_path: str, user_id: int) -> list[dict[str, Any]]:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, title, kind, amount_monthly, debt_details
                FROM budget_obligations
                WHERE user_id = $1 AND active = TRUE
                ORDER BY id DESC
                """,
                int(user_id),
            )
        out: list[dict[str, Any]] = []
        for row in rows:
            details = row["debt_details"] or {}
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except ValueError:
                    details = {}
            out.append(
                {
                    "id": int(row["id"]),
                    "title": row["title"],
                    "kind": row["kind"] or "other",
                    "amount_monthly": float(row["amount_monthly"] or 0.0),
                    "debt_details": details if isinstance(details, dict) else {},
                }
            )
        return out
    except Exception:
        logger.exception("Failed list_budget_obligations user=%s", user_id)
        raise


async def list_budget_incomes(db_path: str, user_id: int) -> list[dict[str, Any]]:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, kind, title, amount_monthly
                FROM budget_incomes
                WHERE user_id = $1 AND active = TRUE
                ORDER BY id DESC
                """,
                int(user_id),
            )
        return [
            {
                "id": int(row["id"]),
                "kind": row["kind"] or "other",
                "title": row["title"],
                "amount_monthly": float(row["amount_monthly"] or 0.0),
            }
            for row in rows
        ]
    except Exception:
        logger.exception("Failed list_budget_incomes user=%s", user_id)
        raise


async def add_budget_income(db_path: str, user_id: int, kind: str, title: str, amount_monthly: float) -> int:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                row = await conn.fetchrow(
                    """
                    INSERT INTO budget_incomes (user_id, user_ref_id, kind, title, amount_monthly, active, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, TRUE, NOW(), NOW())
                    RETURNING id
                    """,
                    int(user_id),
                    int(user_ref_id),
                    kind.strip() or "other",
                    title.strip(),
                    float(amount_monthly),
                )
        return int(row["id"])
    except Exception:
        logger.exception("Failed add_budget_income user=%s title=%s", user_id, title)
        raise


async def list_budget_expenses(db_path: str, user_id: int) -> list[dict[str, Any]]:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, kind, title, amount_monthly, payload
                FROM budget_expenses
                WHERE user_id = $1 AND active = TRUE
                ORDER BY id DESC
                """,
                int(user_id),
            )
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = row["payload"] or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except ValueError:
                    payload = {}
            out.append(
                {
                    "id": int(row["id"]),
                    "kind": row["kind"] or "other",
                    "title": row["title"],
                    "amount_monthly": float(row["amount_monthly"] or 0.0),
                    "payload": payload if isinstance(payload, dict) else {},
                }
            )
        return out
    except Exception:
        logger.exception("Failed list_budget_expenses user=%s", user_id)
        raise


async def add_budget_expense(
    db_path: str,
    user_id: int,
    kind: str,
    title: str,
    amount_monthly: float,
    payload: dict[str, Any] | None = None,
) -> int:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                row = await conn.fetchrow(
                    """
                    INSERT INTO budget_expenses (user_id, user_ref_id, kind, title, amount_monthly, payload, active, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6::jsonb, TRUE, NOW(), NOW())
                    RETURNING id
                    """,
                    int(user_id),
                    int(user_ref_id),
                    kind.strip() or "other",
                    title.strip(),
                    float(amount_monthly),
                    json.dumps(payload or {}),
                )
        return int(row["id"])
    except Exception:
        logger.exception("Failed add_budget_expense user=%s title=%s", user_id, title)
        raise


async def update_budget_expense(
    db_path: str,
    user_id: int,
    expense_id: int,
    kind: str | None = None,
    title: str | None = None,
    amount_monthly: float | None = None,
    payload: dict[str, Any] | None = None,
) -> bool:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT kind, title, amount_monthly, payload
                FROM budget_expenses
                WHERE id = $1 AND user_id = $2 AND active = TRUE
                """,
                int(expense_id),
                int(user_id),
            )
            if not row:
                return False
            new_kind = str(kind).strip() if kind is not None else str(row["kind"] or "other")
            new_title = str(title).strip() if title is not None else str(row["title"] or "")
            new_amount = float(amount_monthly) if amount_monthly is not None else float(row["amount_monthly"] or 0.0)
            current_payload = row["payload"] if row["payload"] is not None else {}
            if isinstance(current_payload, str):
                try:
                    current_payload = json.loads(current_payload)
                except ValueError:
                    current_payload = {}
            new_payload = payload if payload is not None else (current_payload if isinstance(current_payload, dict) else {})
            upd = await conn.fetchrow(
                """
                UPDATE budget_expenses
                SET kind = $1,
                    title = $2,
                    amount_monthly = $3,
                    payload = $4::jsonb,
                    updated_at = NOW()
                WHERE id = $5 AND user_id = $6 AND active = TRUE
                RETURNING id
                """,
                new_kind,
                new_title,
                new_amount,
                json.dumps(new_payload),
                int(expense_id),
                int(user_id),
            )
        return upd is not None
    except Exception:
        logger.exception("Failed update_budget_expense user=%s expense=%s", user_id, expense_id)
        raise


async def disable_budget_expense(db_path: str, user_id: int, expense_id: int) -> bool:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE budget_expenses
                SET active = FALSE,
                    updated_at = NOW()
                WHERE id = $1 AND user_id = $2 AND active = TRUE
                RETURNING id
                """,
                int(expense_id),
                int(user_id),
            )
        return row is not None
    except Exception:
        logger.exception("Failed disable_budget_expense user=%s expense=%s", user_id, expense_id)
        raise


async def update_budget_income(
    db_path: str,
    user_id: int,
    income_id: int,
    kind: str | None = None,
    title: str | None = None,
    amount_monthly: float | None = None,
) -> bool:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT kind, title, amount_monthly
                FROM budget_incomes
                WHERE id = $1 AND user_id = $2 AND active = TRUE
                """,
                int(income_id),
                int(user_id),
            )
            if not row:
                return False
            new_kind = str(kind).strip() if kind is not None else str(row["kind"] or "other")
            new_title = str(title).strip() if title is not None else str(row["title"] or "")
            new_amount = float(amount_monthly) if amount_monthly is not None else float(row["amount_monthly"] or 0.0)
            upd = await conn.fetchrow(
                """
                UPDATE budget_incomes
                SET kind = $1,
                    title = $2,
                    amount_monthly = $3,
                    updated_at = NOW()
                WHERE id = $4 AND user_id = $5 AND active = TRUE
                RETURNING id
                """,
                new_kind,
                new_title,
                new_amount,
                int(income_id),
                int(user_id),
            )
        return upd is not None
    except Exception:
        logger.exception("Failed update_budget_income user=%s income=%s", user_id, income_id)
        raise


async def disable_budget_income(db_path: str, user_id: int, income_id: int) -> bool:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE budget_incomes
                SET active = FALSE,
                    updated_at = NOW()
                WHERE id = $1 AND user_id = $2 AND active = TRUE
                RETURNING id
                """,
                int(income_id),
                int(user_id),
            )
        return row is not None
    except Exception:
        logger.exception("Failed disable_budget_income user=%s income=%s", user_id, income_id)
        raise


async def get_budget_notification_settings(db_path: str, user_id: int) -> dict[str, Any]:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                await conn.execute(
                    """
                    INSERT INTO budget_notification_settings (
                      user_id, user_ref_id, budget_summary_enabled, goal_deadline_enabled, month_close_enabled, updated_at
                    )
                    VALUES ($1, $2, TRUE, TRUE, TRUE, NOW())
                    ON CONFLICT (user_id) DO NOTHING
                    """,
                    int(user_id),
                    int(user_ref_id),
                )
                row = await conn.fetchrow(
                    """
                    SELECT budget_summary_enabled, goal_deadline_enabled, month_close_enabled
                    FROM budget_notification_settings
                    WHERE user_id = $1
                    """,
                    int(user_id),
                )
        return {
            "budget_summary_enabled": bool(row["budget_summary_enabled"]) if row else True,
            "goal_deadline_enabled": bool(row["goal_deadline_enabled"]) if row else True,
            "month_close_enabled": bool(row["month_close_enabled"]) if row else True,
        }
    except Exception:
        logger.exception("Failed get_budget_notification_settings user=%s", user_id)
        raise


async def set_budget_notification_settings(
    db_path: str,
    user_id: int,
    budget_summary_enabled: bool | None = None,
    goal_deadline_enabled: bool | None = None,
    month_close_enabled: bool | None = None,
) -> dict[str, Any]:
    try:
        current = await get_budget_notification_settings(db_path, user_id)
        next_budget_summary = current["budget_summary_enabled"] if budget_summary_enabled is None else bool(budget_summary_enabled)
        next_goal_deadline = current["goal_deadline_enabled"] if goal_deadline_enabled is None else bool(goal_deadline_enabled)
        next_month_close = current["month_close_enabled"] if month_close_enabled is None else bool(month_close_enabled)
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                await conn.execute(
                    """
                    INSERT INTO budget_notification_settings (
                      user_id, user_ref_id, budget_summary_enabled, goal_deadline_enabled, month_close_enabled, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET user_ref_id = EXCLUDED.user_ref_id,
                        budget_summary_enabled = EXCLUDED.budget_summary_enabled,
                        goal_deadline_enabled = EXCLUDED.goal_deadline_enabled,
                        month_close_enabled = EXCLUDED.month_close_enabled,
                        updated_at = EXCLUDED.updated_at
                    """,
                    int(user_id),
                    int(user_ref_id),
                    next_budget_summary,
                    next_goal_deadline,
                    next_month_close,
                )
        return {
            "budget_summary_enabled": next_budget_summary,
            "goal_deadline_enabled": next_goal_deadline,
            "month_close_enabled": next_month_close,
        }
    except Exception:
        logger.exception("Failed set_budget_notification_settings user=%s", user_id)
        raise


async def reset_budget_data(db_path: str, user_id: int) -> dict[str, int]:
    def _affected_count(status: str) -> int:
        parts = (status or "").split()
        if not parts:
            return 0
        try:
            return int(parts[-1])
        except (TypeError, ValueError):
            return 0

    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                incomes = _affected_count(await conn.execute(
                    "UPDATE budget_incomes SET active = FALSE, updated_at = NOW() WHERE user_id = $1 AND active = TRUE",
                    int(user_id),
                ))
                expenses = _affected_count(await conn.execute(
                    "UPDATE budget_expenses SET active = FALSE, updated_at = NOW() WHERE user_id = $1 AND active = TRUE",
                    int(user_id),
                ))
                obligations = _affected_count(await conn.execute(
                    "UPDATE budget_obligations SET active = FALSE WHERE user_id = $1 AND active = TRUE",
                    int(user_id),
                ))
                savings = _affected_count(await conn.execute(
                    "UPDATE budget_savings SET active = FALSE WHERE user_id = $1 AND active = TRUE",
                    int(user_id),
                ))
                funds = _affected_count(await conn.execute(
                    "DELETE FROM budget_funds WHERE user_id = $1",
                    int(user_id),
                ))
                month_closes = _affected_count(await conn.execute(
                    "DELETE FROM budget_month_closes WHERE user_id = $1",
                    int(user_id),
                ))
                profiles = _affected_count(await conn.execute(
                    "DELETE FROM budget_profiles WHERE user_id = $1",
                    int(user_id),
                ))
                notification_settings = _affected_count(await conn.execute(
                    "DELETE FROM budget_notification_settings WHERE user_id = $1",
                    int(user_id),
                ))
        return {
            "incomes": incomes,
            "expenses": expenses,
            "obligations": obligations,
            "savings": savings,
            "funds": funds,
            "month_closes": month_closes,
            "profiles": profiles,
            "notification_settings": notification_settings,
        }
    except Exception:
        logger.exception("Failed reset_budget_data user=%s", user_id)
        raise


async def add_budget_obligation(
    db_path: str,
    user_id: int,
    title: str,
    amount_monthly: float,
    kind: str = "other",
    debt_details: dict[str, Any] | None = None,
) -> int:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                row = await conn.fetchrow(
                    """
                    INSERT INTO budget_obligations (user_id, user_ref_id, title, kind, amount_monthly, debt_details, active, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6::jsonb, TRUE, NOW())
                    RETURNING id
                    """,
                    int(user_id),
                    int(user_ref_id),
                    title.strip(),
                    kind.strip() or "other",
                    float(amount_monthly),
                    json.dumps(debt_details or {}),
                )
        return int(row["id"])
    except Exception:
        logger.exception("Failed add_budget_obligation user=%s title=%s", user_id, title)
        raise


async def list_budget_savings(db_path: str, user_id: int) -> list[dict[str, Any]]:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, kind, title, amount
                FROM budget_savings
                WHERE user_id = $1 AND active = TRUE
                ORDER BY id DESC
                """,
                int(user_id),
            )
        return [
            {
                "id": int(row["id"]),
                "kind": row["kind"] or "other",
                "title": row["title"],
                "amount": float(row["amount"] or 0.0),
            }
            for row in rows
        ]
    except Exception:
        logger.exception("Failed list_budget_savings user=%s", user_id)
        raise


async def add_budget_saving(db_path: str, user_id: int, kind: str, title: str, amount: float) -> int:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                row = await conn.fetchrow(
                    """
                    INSERT INTO budget_savings (user_id, user_ref_id, kind, title, amount, active, created_at)
                    VALUES ($1, $2, $3, $4, $5, TRUE, NOW())
                    RETURNING id
                    """,
                    int(user_id),
                    int(user_ref_id),
                    kind.strip() or "other",
                    title.strip(),
                    float(amount),
                )
        return int(row["id"])
    except Exception:
        logger.exception("Failed add_budget_saving user=%s title=%s", user_id, title)
        raise


async def list_budget_funds(db_path: str, user_id: int) -> list[dict[str, Any]]:
    now_key = _month_key_for(date.today())
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, title, target_amount, already_saved, target_month, payload, priority, status, autopilot_enabled
                FROM budget_funds
                WHERE user_id = $1 AND status <> 'deleted'
                ORDER BY id DESC
                """,
                int(user_id),
            )
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = row["payload"] or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except ValueError:
                    payload = {}
            if not isinstance(payload, dict):
                payload = {}
            target_date = str(payload.get("target_date") or "").strip()
            target_month_from_date = ""
            if len(target_date) >= 7 and target_date[4] == "-":
                target_month_from_date = target_date[:7]
            target_month = _safe_month_key(target_month_from_date or row["target_month"], _next_month_key(now_key))
            calc = _fund_metrics(
                float(row["target_amount"] or 0.0),
                float(row["already_saved"] or 0.0),
                target_month,
                now_key,
            )
            progress_pct = 0.0
            target_amount = float(row["target_amount"] or 0.0)
            already_saved = float(row["already_saved"] or 0.0)
            if target_amount > 0:
                progress_pct = max(0.0, min(100.0, already_saved / target_amount * 100.0))
            out.append(
                {
                    "id": int(row["id"]),
                    "title": row["title"],
                    "target_amount": target_amount,
                    "already_saved": already_saved,
                    "target_month": target_month,
                    "target_date": target_date or f"{target_month}-01",
                    "description": str(payload.get("description") or "").strip(),
                    "checklist": payload.get("checklist") if isinstance(payload.get("checklist"), list) else [],
                    "priority": row["priority"] or "medium",
                    "status": row["status"] or "active",
                    "autopilot_enabled": bool(row["autopilot_enabled"]),
                    "progress_pct": progress_pct,
                    **calc,
                }
            )
        return out
    except Exception:
        logger.exception("Failed list_budget_funds user=%s", user_id)
        raise


async def create_budget_fund(
    db_path: str,
    user_id: int,
    title: str,
    target_amount: float,
    already_saved: float,
    target_month: str,
    priority: str,
    payload: dict[str, Any] | None = None,
) -> int:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                row = await conn.fetchrow(
                    """
                    INSERT INTO budget_funds (
                      user_id, user_ref_id, title, target_amount, already_saved, target_month, payload, priority, status, autopilot_enabled, created_at, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, 'active', FALSE, NOW(), NOW())
                    RETURNING id
                    """,
                    int(user_id),
                    int(user_ref_id),
                    title.strip(),
                    float(target_amount),
                    float(already_saved),
                    target_month,
                    json.dumps(payload or {}),
                    priority,
                )
        return int(row["id"])
    except Exception:
        logger.exception("Failed create_budget_fund user=%s title=%s", user_id, title)
        raise


async def update_budget_fund(
    db_path: str,
    user_id: int,
    fund_id: int,
    title: str | None = None,
    target_amount: float | None = None,
    already_saved: float | None = None,
    target_month: str | None = None,
    status: str | None = None,
    autopilot_enabled: bool | None = None,
    payload: dict[str, Any] | None = None,
) -> bool:
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT title, target_amount, already_saved, target_month, status, autopilot_enabled, payload
                FROM budget_funds
                WHERE id = $1 AND user_id = $2
                """,
                int(fund_id),
                int(user_id),
            )
            if not row:
                return False
            new_title = str(title).strip() if title is not None else str(row["title"] or "")
            new_target_amount = float(target_amount) if target_amount is not None else float(row["target_amount"] or 0.0)
            new_already_saved = float(already_saved) if already_saved is not None else float(row["already_saved"] or 0.0)
            new_target_month = str(target_month) if target_month is not None else str(row["target_month"] or "")
            new_status = str(status) if status is not None else str(row["status"] or "active")
            new_autopilot = bool(autopilot_enabled) if autopilot_enabled is not None else bool(row["autopilot_enabled"])
            current_payload = row["payload"] if row["payload"] is not None else {}
            if isinstance(current_payload, str):
                try:
                    current_payload = json.loads(current_payload)
                except ValueError:
                    current_payload = {}
            if not isinstance(current_payload, dict):
                current_payload = {}
            new_payload = payload if payload is not None else current_payload
            result = await conn.fetchrow(
                """
                UPDATE budget_funds
                SET title = $1,
                    target_amount = $2,
                    already_saved = $3,
                    target_month = $4,
                    status = $5,
                    autopilot_enabled = $6,
                    payload = $7::jsonb,
                    updated_at = NOW()
                WHERE id = $8 AND user_id = $9
                RETURNING id
                """,
                new_title,
                new_target_amount,
                new_already_saved,
                new_target_month,
                new_status,
                new_autopilot,
                json.dumps(new_payload),
                int(fund_id),
                int(user_id),
            )
        return result is not None
    except Exception:
        logger.exception("Failed update_budget_fund user=%s fund=%s", user_id, fund_id)
        raise


async def close_budget_month(
    db_path: str,
    user_id: int,
    month_key: str,
    planned_expenses_base: float,
    actual_expenses_base: float,
    extra_income_items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    extra_items = extra_income_items or []
    total_extra = 0.0
    for item in extra_items:
        try:
            total_extra += float(item.get("amount") or 0.0)
        except (TypeError, ValueError):
            continue
    try:
        pool = await _get_pool(db_path)
        async with pool.acquire() as conn:
            async with conn.transaction():
                user_ref_id, _ = await _ensure_user_context(conn, int(user_id))
                await conn.execute(
                    """
                    INSERT INTO budget_month_closes (
                      user_id, user_ref_id, month_key, planned_expenses_base, actual_expenses_base, extra_income_total, extra_income_items, closed_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, NOW())
                    ON CONFLICT (user_id, month_key) DO UPDATE
                    SET user_ref_id = EXCLUDED.user_ref_id,
                        planned_expenses_base = EXCLUDED.planned_expenses_base,
                        actual_expenses_base = EXCLUDED.actual_expenses_base,
                        extra_income_total = EXCLUDED.extra_income_total,
                        extra_income_items = EXCLUDED.extra_income_items,
                        closed_at = EXCLUDED.closed_at
                    """,
                    int(user_id),
                    int(user_ref_id),
                    month_key,
                    float(planned_expenses_base),
                    float(actual_expenses_base),
                    float(total_extra),
                    json.dumps(extra_items),
                )
                rows = await conn.fetch(
                    """
                    SELECT id, title, target_amount, already_saved, target_month
                    FROM budget_funds
                    WHERE user_id = $1 AND status = 'active'
                    """,
                    int(user_id),
                )
                funds_recalc: list[dict[str, Any]] = []
                next_month = _next_month_key(month_key)
                for row in rows:
                    calc_before = _fund_metrics(
                        float(row["target_amount"] or 0.0),
                        float(row["already_saved"] or 0.0),
                        _safe_month_key(row["target_month"], next_month),
                        month_key,
                    )
                    calc_after = _fund_metrics(
                        float(row["target_amount"] or 0.0),
                        float(row["already_saved"] or 0.0),
                        _safe_month_key(row["target_month"], next_month),
                        next_month,
                    )
                    funds_recalc.append(
                        {
                            "id": int(row["id"]),
                            "title": row["title"],
                            "old_required_per_month": calc_before["required_per_month"],
                            "new_required_per_month": calc_after["required_per_month"],
                            "delta": calc_after["required_per_month"] - calc_before["required_per_month"],
                        }
                    )
        delta_expenses = float(actual_expenses_base) - float(planned_expenses_base)
        return {
            "month_key": month_key,
            "planned_expenses_base": float(planned_expenses_base),
            "actual_expenses_base": float(actual_expenses_base),
            "delta_expenses": delta_expenses,
            "extra_income_total": float(total_extra),
            "funds_recalc": funds_recalc,
        }
    except Exception:
        logger.exception("Failed close_budget_month user=%s month=%s", user_id, month_key)
        raise


async def get_budget_dashboard(db_path: str, user_id: int) -> dict[str, Any]:
    profile = await get_budget_profile(db_path, user_id)
    incomes = await list_budget_incomes(db_path, user_id)
    expenses = await list_budget_expenses(db_path, user_id)
    obligations = await list_budget_obligations(db_path, user_id)
    savings = await list_budget_savings(db_path, user_id)
    funds = await list_budget_funds(db_path, user_id)
    incomes_total = sum(float(x.get("amount_monthly") or 0.0) for x in incomes)
    if expenses:
        obligations_kinds = {"rent", "mortgage", "loan", "utilities"}
        obligations_total = sum(
            float(x.get("amount_monthly") or 0.0)
            for x in expenses
            if str(x.get("kind") or "").lower() in obligations_kinds
        )
        living_expenses_total = sum(
            float(x.get("amount_monthly") or 0.0)
            for x in expenses
            if str(x.get("kind") or "").lower() not in obligations_kinds
        )
    else:
        obligations_total = sum(float(x.get("amount_monthly") or 0.0) for x in obligations)
        living_expenses_total = float(profile.get("expenses_base") or 0.0)
    savings_total = sum(float(x.get("amount") or 0.0) for x in savings)
    legacy_income = float(profile.get("income_monthly") or 0.0)
    income = incomes_total if incomes_total > 0 else legacy_income
    expenses_base = living_expenses_total
    free = income - obligations_total - expenses_base
    today = date.today()
    current_month_key = _month_key_for(today)
    prev_month = f"{today.year - 1:04d}-12" if today.month == 1 else f"{today.year:04d}-{today.month - 1:02d}"

    pool = await _get_pool(db_path)
    async with pool.acquire() as conn:
        closed_prev = await conn.fetchval(
            "SELECT 1 FROM budget_month_closes WHERE user_id = $1 AND month_key = $2",
            int(user_id),
            prev_month,
        )

    return {
        "profile": profile,
        "incomes": incomes,
        "incomes_total": incomes_total,
        "expenses": expenses,
        "expenses_total": obligations_total + living_expenses_total,
        "living_expenses_total": living_expenses_total,
        "income": income,
        "obligations_total": obligations_total,
        "expenses_base": expenses_base,
        "free": free,
        "savings_total": savings_total,
        "obligations": obligations,
        "savings": savings,
        "funds": funds,
        "month": current_month_key,
        "previous_month": prev_month,
        "need_close_previous_month": not bool(closed_prev),
    }
