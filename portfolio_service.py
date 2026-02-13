from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import aiohttp

from db import (
    get_price_cache_map,
    get_user_positions,
    list_active_position_instruments,
    upsert_price_cache_bulk,
)
from moex_iss import ASSET_TYPE_STOCK, get_history_prices_by_asset_type, get_last_price_by_asset_type

logger = logging.getLogger(__name__)


def cache_age_seconds(updated_at: datetime | None, now_utc: datetime) -> float | None:
    if updated_at is None:
        return None
    dt = updated_at
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (now_utc - dt.astimezone(timezone.utc)).total_seconds())


async def fetch_prices_limited(
    rows: list[dict],
    *,
    price_fetch_concurrency: int,
    price_fetch_batch_size: int,
) -> list[tuple[dict, float | None]]:
    if not rows:
        return []

    sem = asyncio.Semaphore(price_fetch_concurrency)
    out: list[tuple[dict, float | None]] = []

    async with aiohttp.ClientSession() as session:
        async def load_price(row: dict) -> tuple[dict, float | None]:
            async with sem:
                try:
                    last = await get_last_price_by_asset_type(
                        session,
                        row["secid"],
                        row.get("boardid"),
                        row.get("asset_type") or ASSET_TYPE_STOCK,
                    )
                    return row, last
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
                    logger.warning(
                        "Failed to load price secid=%s boardid=%s",
                        row.get("secid"),
                        row.get("boardid"),
                    )
                    return row, None

        for i in range(0, len(rows), price_fetch_batch_size):
            batch = rows[i:i + price_fetch_batch_size]
            out.extend(await asyncio.gather(*(load_price(row) for row in batch)))
    return out


async def refresh_price_cache_once(
    db_dsn: str,
    *,
    price_fetch_concurrency: int,
    price_fetch_batch_size: int,
) -> None:
    instruments = await list_active_position_instruments(db_dsn)
    if not instruments:
        return

    priced = await fetch_prices_limited(
        instruments,
        price_fetch_concurrency=price_fetch_concurrency,
        price_fetch_batch_size=price_fetch_batch_size,
    )
    now_utc = datetime.now(timezone.utc)
    cache_rows = [
        (int(row["instrument_id"]), float(last))
        for row, last in priced
        if last is not None
    ]
    await upsert_price_cache_bulk(db_dsn, cache_rows, now_utc)


async def load_prices_for_positions(
    db_dsn: str,
    positions: list[dict],
    *,
    price_fetch_concurrency: int,
    price_fetch_batch_size: int,
) -> dict[int, float | None]:
    now_utc = datetime.now(timezone.utc)
    instrument_ids = [int(pos["id"]) for pos in positions]
    cache = await get_price_cache_map(db_dsn, instrument_ids)

    prices: dict[int, float | None] = {}
    missing_positions: list[dict] = []
    for pos in positions:
        iid = int(pos["id"])
        rec = cache.get(iid)
        if rec:
            age = cache_age_seconds(rec.get("updated_at"), now_utc)
            if age is not None and age <= 120:
                prices[iid] = float(rec["last_price"])
                continue
        missing_positions.append(pos)

    if not missing_positions:
        return prices

    loaded = await fetch_prices_limited(
        missing_positions,
        price_fetch_concurrency=price_fetch_concurrency,
        price_fetch_batch_size=price_fetch_batch_size,
    )
    cache_rows: list[tuple[int, float]] = []
    for pos, last in loaded:
        iid = int(pos["id"])
        prices[iid] = last
        if last is not None:
            cache_rows.append((iid, float(last)))
    await upsert_price_cache_bulk(db_dsn, cache_rows, now_utc)
    return prices


async def build_portfolio_map_rows(
    db_dsn: str,
    user_id: int,
    *,
    price_fetch_concurrency: int,
    price_fetch_batch_size: int,
) -> tuple[list[dict], int]:
    positions = await get_user_positions(db_dsn, user_id)
    if not positions:
        return [], 0

    prices = await load_prices_for_positions(
        db_dsn,
        positions,
        price_fetch_concurrency=price_fetch_concurrency,
        price_fetch_batch_size=price_fetch_batch_size,
    )
    rows: list[dict] = []
    unknown_prices = 0
    for pos in positions:
        qty = float(pos.get("total_qty") or 0.0)
        if qty <= 1e-12:
            continue
        last = prices.get(int(pos["id"]))
        if last is None:
            unknown_prices += 1
            continue
        total_cost = float(pos.get("total_cost") or 0.0)
        value = qty * float(last)
        if value <= 0:
            continue
        pnl_pct = (value - total_cost) / total_cost * 100.0 if abs(total_cost) > 1e-12 else None
        rows.append(
            {
                "instrument_id": int(pos["id"]),
                "secid": str(pos.get("secid") or "").strip() or "UNKNOWN",
                "shortname": (pos.get("shortname") or "").strip(),
                "boardid": pos.get("boardid"),
                "asset_type": pos.get("asset_type") or ASSET_TYPE_STOCK,
                "qty": qty,
                "last": float(last),
                "value": float(value),
                "pnl_pct": pnl_pct,
            }
        )
    rows.sort(key=lambda x: float(x["value"]), reverse=True)
    return rows, unknown_prices


async def compute_portfolio_return_30d(
    rows: list[dict],
    *,
    price_fetch_concurrency: int,
) -> tuple[float | None, dict[int, float]]:
    if not rows:
        return None, {}
    today = datetime.now().date()
    from_date = today - timedelta(days=30)
    till_date = today
    sem = asyncio.Semaphore(price_fetch_concurrency)
    base_price_map: dict[int, float] = {}

    async with aiohttp.ClientSession() as session:
        async def load_base(row: dict) -> None:
            async with sem:
                try:
                    history = await get_history_prices_by_asset_type(
                        session,
                        secid=row["secid"],
                        boardid=row.get("boardid"),
                        asset_type=row.get("asset_type") or ASSET_TYPE_STOCK,
                        from_date=from_date,
                        till_date=till_date,
                    )
                    if history and history[0][1] > 0:
                        base_price_map[int(row["instrument_id"])] = float(history[0][1])
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
                    logger.warning("Failed loading 30d history for secid=%s", row.get("secid"))

        await asyncio.gather(*(load_base(row) for row in rows))

    base_total = 0.0
    current_total = 0.0
    for row in rows:
        iid = int(row["instrument_id"])
        base_price = base_price_map.get(iid)
        if base_price is None or base_price <= 0:
            continue
        qty = float(row["qty"])
        base_total += qty * base_price
        current_total += qty * float(row["last"])
    if base_total <= 1e-12:
        return None, base_price_map
    return (current_total - base_total) / base_total * 100.0, base_price_map
