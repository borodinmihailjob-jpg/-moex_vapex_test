import aiosqlite
import logging

logger = logging.getLogger(__name__)

CREATE_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS instruments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  secid TEXT NOT NULL,
  isin TEXT,
  boardid TEXT,
  shortname TEXT,
  asset_type TEXT NOT NULL DEFAULT 'stock'
);

CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  instrument_id INTEGER NOT NULL,
  trade_date TEXT NOT NULL,       -- формат строки даты сделки
  qty REAL NOT NULL,
  price REAL NOT NULL,            -- price per 1 share
  commission REAL NOT NULL DEFAULT 0,
  FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

CREATE TABLE IF NOT EXISTS user_alert_settings (
  user_id INTEGER PRIMARY KEY,
  periodic_enabled INTEGER NOT NULL DEFAULT 0,
  periodic_interval_min INTEGER NOT NULL DEFAULT 60,
  periodic_last_sent_at TEXT,
  drop_alert_enabled INTEGER NOT NULL DEFAULT 0,
  drop_percent REAL NOT NULL DEFAULT 10,
  open_close_enabled INTEGER NOT NULL DEFAULT 0,
  open_last_sent_date TEXT,
  close_last_sent_date TEXT
);

CREATE TABLE IF NOT EXISTS price_alert_state (
  user_id INTEGER NOT NULL,
  instrument_id INTEGER NOT NULL,
  was_below INTEGER NOT NULL DEFAULT 0,
  last_alert_at TEXT,
  PRIMARY KEY (user_id, instrument_id)
);
"""

async def init_db(db_path: str):
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.executescript(CREATE_SQL)
            # Миграция для уже существующей БД.
            cur = await db.execute("PRAGMA table_info(instruments)")
            cols = await cur.fetchall()
            col_names = {c[1] for c in cols}
            if "asset_type" not in col_names:
                await db.execute("ALTER TABLE instruments ADD COLUMN asset_type TEXT NOT NULL DEFAULT 'stock'")
            await db.execute("UPDATE instruments SET asset_type='stock' WHERE asset_type IS NULL OR asset_type=''")
            await db.commit()
        logger.info("Database initialized: %s", db_path)
    except Exception:
        logger.exception("Failed to initialize database: %s", db_path)
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
        async with aiosqlite.connect(db_path) as db:
            cur = await db.execute(
                "SELECT id FROM instruments WHERE secid=? AND COALESCE(boardid,'')=COALESCE(?, '') AND COALESCE(asset_type,'stock')=?",
                (secid, boardid, asset_type)
            )
            row = await cur.fetchone()
            if row:
                return row[0]

            cur = await db.execute(
                "INSERT INTO instruments (secid, isin, boardid, shortname, asset_type) VALUES (?,?,?,?,?)",
                (secid, isin, boardid, shortname, asset_type)
            )
            await db.commit()
            logger.info("Instrument inserted: secid=%s boardid=%s asset_type=%s", secid, boardid, asset_type)
            return cur.lastrowid
    except Exception:
        logger.exception("Failed upsert_instrument secid=%s boardid=%s asset_type=%s", secid, boardid, asset_type)
        raise

async def add_trade(db_path: str, user_id: int, instrument_id: int, trade_date: str, qty: float, price: float, commission: float):
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                "INSERT INTO trades (user_id, instrument_id, trade_date, qty, price, commission) VALUES (?,?,?,?,?,?)",
                (user_id, instrument_id, trade_date, qty, price, commission)
            )
            await db.commit()
        logger.info("Trade inserted: user=%s instrument=%s qty=%s price=%s", user_id, instrument_id, qty, price)
    except Exception:
        logger.exception("Failed add_trade user=%s instrument=%s", user_id, instrument_id)
        raise

async def get_position_agg(db_path: str, user_id: int, instrument_id: int):
    """
    total_qty, total_cost (qty*price + commission), avg_price = total_cost/total_qty
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            cur = await db.execute(
                """
                SELECT
                  COALESCE(SUM(qty),0) AS total_qty,
                  COALESCE(SUM(qty*price + commission),0) AS total_cost
                FROM trades
                WHERE user_id=? AND instrument_id=?
                """,
                (user_id, instrument_id)
            )
            total_qty, total_cost = await cur.fetchone()
            avg_price = (total_cost / total_qty) if total_qty else 0.0
            return float(total_qty), float(total_cost), float(avg_price)
    except Exception:
        logger.exception("Failed get_position_agg user=%s instrument=%s", user_id, instrument_id)
        raise

async def get_instrument(db_path: str, instrument_id: int):
    try:
        async with aiosqlite.connect(db_path) as db:
            cur = await db.execute(
                "SELECT id, secid, isin, boardid, shortname, COALESCE(asset_type,'stock') FROM instruments WHERE id=?",
                (instrument_id,)
            )
            row = await cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "secid": row[1],
                "isin": row[2],
                "boardid": row[3],
                "shortname": row[4],
                "asset_type": row[5],
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
        async with aiosqlite.connect(db_path) as db:
            cur = await db.execute(
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
                WHERE t.user_id = ?
                GROUP BY i.id, i.secid, i.isin, i.boardid, i.shortname, COALESCE(i.asset_type, 'stock')
                HAVING ABS(COALESCE(SUM(t.qty), 0)) > 1e-12
                ORDER BY i.secid
                """,
                (user_id,),
            )
            rows = await cur.fetchall()

        out = []
        for row in rows:
            total_qty = float(row[6])
            total_cost = float(row[7])
            avg_price = (total_cost / total_qty) if total_qty else 0.0
            out.append(
                {
                    "id": row[0],
                    "secid": row[1],
                    "isin": row[2],
                    "boardid": row[3],
                    "shortname": row[4],
                    "asset_type": row[5],
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
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                """
                INSERT INTO user_alert_settings (user_id)
                VALUES (?)
                ON CONFLICT(user_id) DO NOTHING
                """,
                (user_id,),
            )
            await db.commit()
    except Exception:
        logger.exception("Failed ensure_user_alert_settings user=%s", user_id)
        raise

async def set_periodic_alert(db_path: str, user_id: int, enabled: bool, interval_min: int | None = None):
    try:
        await ensure_user_alert_settings(db_path, user_id)
        async with aiosqlite.connect(db_path) as db:
            if interval_min is None:
                await db.execute(
                    """
                    UPDATE user_alert_settings
                    SET periodic_enabled=?,
                        periodic_last_sent_at=NULL
                    WHERE user_id=?
                    """,
                    (1 if enabled else 0, user_id),
                )
            else:
                await db.execute(
                    """
                    UPDATE user_alert_settings
                    SET periodic_enabled=?,
                        periodic_interval_min=?,
                        periodic_last_sent_at=NULL
                    WHERE user_id=?
                    """,
                    (1 if enabled else 0, int(interval_min), user_id),
                )
            await db.commit()
    except Exception:
        logger.exception("Failed set_periodic_alert user=%s", user_id)
        raise

async def set_drop_alert(db_path: str, user_id: int, enabled: bool, drop_percent: float | None = None):
    try:
        await ensure_user_alert_settings(db_path, user_id)
        async with aiosqlite.connect(db_path) as db:
            if drop_percent is None:
                await db.execute(
                    "UPDATE user_alert_settings SET drop_alert_enabled=? WHERE user_id=?",
                    (1 if enabled else 0, user_id),
                )
            else:
                await db.execute(
                    "UPDATE user_alert_settings SET drop_alert_enabled=?, drop_percent=? WHERE user_id=?",
                    (1 if enabled else 0, float(drop_percent), user_id),
                )
            await db.commit()
    except Exception:
        logger.exception("Failed set_drop_alert user=%s", user_id)
        raise

async def set_open_close_alert(db_path: str, user_id: int, enabled: bool):
    try:
        await ensure_user_alert_settings(db_path, user_id)
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                """
                UPDATE user_alert_settings
                SET open_close_enabled=?,
                    open_last_sent_date=NULL,
                    close_last_sent_date=NULL
                WHERE user_id=?
                """,
                (1 if enabled else 0, user_id),
            )
            await db.commit()
    except Exception:
        logger.exception("Failed set_open_close_alert user=%s", user_id)
        raise

async def get_user_alert_settings(db_path: str, user_id: int):
    try:
        await ensure_user_alert_settings(db_path, user_id)
        async with aiosqlite.connect(db_path) as db:
            cur = await db.execute(
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
                WHERE user_id=?
                """,
                (user_id,),
            )
            row = await cur.fetchone()
        return {
            "user_id": row[0],
            "periodic_enabled": bool(row[1]),
            "periodic_interval_min": int(row[2]),
            "periodic_last_sent_at": row[3],
            "drop_alert_enabled": bool(row[4]),
            "drop_percent": float(row[5]),
            "open_close_enabled": bool(row[6]),
            "open_last_sent_date": row[7],
            "close_last_sent_date": row[8],
        }
    except Exception:
        logger.exception("Failed get_user_alert_settings user=%s", user_id)
        raise

async def list_users_with_alerts(db_path: str):
    try:
        async with aiosqlite.connect(db_path) as db:
            cur = await db.execute(
                """
                SELECT DISTINCT user_id
                FROM user_alert_settings
                WHERE periodic_enabled=1 OR drop_alert_enabled=1 OR open_close_enabled=1
                """
            )
            rows = await cur.fetchall()
        return [int(r[0]) for r in rows]
    except Exception:
        logger.exception("Failed list_users_with_alerts")
        raise

async def update_periodic_last_sent_at(db_path: str, user_id: int, iso_ts: str):
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                "UPDATE user_alert_settings SET periodic_last_sent_at=? WHERE user_id=?",
                (iso_ts, user_id),
            )
            await db.commit()
    except Exception:
        logger.exception("Failed update_periodic_last_sent_at user=%s", user_id)
        raise

async def update_open_sent_date(db_path: str, user_id: int, date_iso: str):
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                "UPDATE user_alert_settings SET open_last_sent_date=? WHERE user_id=?",
                (date_iso, user_id),
            )
            await db.commit()
    except Exception:
        logger.exception("Failed update_open_sent_date user=%s", user_id)
        raise

async def update_close_sent_date(db_path: str, user_id: int, date_iso: str):
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                "UPDATE user_alert_settings SET close_last_sent_date=? WHERE user_id=?",
                (date_iso, user_id),
            )
            await db.commit()
    except Exception:
        logger.exception("Failed update_close_sent_date user=%s", user_id)
        raise

async def get_price_alert_state(db_path: str, user_id: int, instrument_id: int) -> bool:
    try:
        async with aiosqlite.connect(db_path) as db:
            cur = await db.execute(
                "SELECT was_below FROM price_alert_state WHERE user_id=? AND instrument_id=?",
                (user_id, instrument_id),
            )
            row = await cur.fetchone()
        return bool(row[0]) if row else False
    except Exception:
        logger.exception("Failed get_price_alert_state user=%s instrument=%s", user_id, instrument_id)
        raise

async def set_price_alert_state(db_path: str, user_id: int, instrument_id: int, was_below: bool, alert_ts: str | None = None):
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                """
                INSERT INTO price_alert_state (user_id, instrument_id, was_below, last_alert_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, instrument_id) DO UPDATE SET
                  was_below=excluded.was_below,
                  last_alert_at=excluded.last_alert_at
                """,
                (user_id, instrument_id, 1 if was_below else 0, alert_ts),
            )
            await db.commit()
    except Exception:
        logger.exception("Failed set_price_alert_state user=%s instrument=%s", user_id, instrument_id)
        raise
