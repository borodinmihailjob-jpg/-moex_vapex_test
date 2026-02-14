import asyncio
import calendar
import logging
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from json import JSONDecodeError
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web

from broker_import_service import import_broker_xml_trades
from common_utils import safe_float
from db import (
    add_trade,
    archive_loan_account,
    add_budget_expense,
    add_budget_history_event,
    add_budget_income,
    add_budget_obligation,
    add_budget_saving,
    change_budget_saving_amount,
    clear_user_portfolio,
    close_budget_month,
    create_loan_account,
    create_loan_actual_payment,
    create_loan_event,
    create_loan_share_link,
    create_budget_fund,
    create_price_target_alert,
    disable_budget_expense,
    disable_budget_income,
    disable_budget_saving,
    disable_price_target_alert,
    ensure_user_alert_settings,
    get_budget_dashboard,
    get_budget_notification_settings,
    get_budget_profile,
    get_loan_account,
    get_loan_share_link,
    get_loan_schedule_cache,
    get_loan_reminder_settings,
    get_active_app_text,
    get_user_last_mode,
    get_position_agg,
    get_user_alert_settings,
    get_user_positions,
    list_active_app_texts,
    list_active_price_target_alerts,
    list_budget_funds,
    list_budget_expenses,
    list_budget_incomes,
    list_budget_obligations,
    list_budget_history,
    list_budget_savings,
    list_loan_accounts,
    list_loan_actual_payments,
    list_loan_events,
    reset_budget_data,
    set_user_last_mode,
    set_loan_reminder_settings,
    set_budget_notification_settings,
    set_open_close_alert,
    update_budget_income,
    update_budget_expense,
    update_budget_fund,
    update_budget_saving,
    upsert_loan_schedule_cache,
    upsert_budget_profile,
    upsert_instrument,
)
from loan_engine import (
    ExtraPaymentEvent,
    HolidayEvent,
    LoanInput,
    RateChangeEvent,
    build_version_hash as loan_version_hash,
    calculate as calculate_loan_schedule,
    month_diff,
    q_money,
)
from moex_iss import (
    ASSET_TYPE_FIAT,
    ASSET_TYPE_METAL,
    ASSET_TYPE_STOCK,
    get_history_prices_by_asset_type,
    get_last_price_by_asset_type,
    get_last_price_fiat,
    get_stock_movers_by_date,
    get_usd_rub_rate,
    search_fiat,
    search_metals,
    search_securities,
)
from miniapp_auth import MiniAppAuthError, parse_and_validate_init_data

logger = logging.getLogger(__name__)

TRADE_SIDE_BUY = "buy"
TRADE_SIDE_SELL = "sell"
MAX_XML_UPLOAD_BYTES = 5 * 1024 * 1024
_api_cache: dict[str, tuple[float, dict]] = {}
_API_CACHE_TTL_SEC = int((os.getenv("MINIAPP_API_CACHE_TTL_SEC") or "900").strip() or "900")
_API_CACHE_MAX_SIZE = int((os.getenv("MINIAPP_API_CACHE_MAX_SIZE") or "2048").strip() or "2048")
_PRICE_LOAD_CONCURRENCY = int((os.getenv("MINIAPP_PRICE_LOAD_CONCURRENCY") or "12").strip() or "12")
_INITDATA_MAX_AGE_SEC = int((os.getenv("MINIAPP_INITDATA_MAX_AGE_SEC") or "86400").strip() or "86400")
_LOAN_RATE_LIMIT: dict[str, list[float]] = {}
_LOAN_RATE_WINDOW_SEC = 60.0
_LOAN_RATE_MAX_EVENTS = int((os.getenv("LOAN_RATE_MAX_EVENTS_PER_MIN") or "30").strip() or "30")
MSK_TZ = ZoneInfo("Europe/Moscow")
APP_DB_DSN: web.AppKey[str] = web.AppKey("db_dsn", str)
APP_BOT_TOKEN: web.AppKey[str] = web.AppKey("bot_token", str)


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _is_postgres_dsn(dsn: str) -> bool:
    return str(dsn or "").startswith(("postgresql://", "postgres://"))


def _today_msk() -> date:
    return datetime.now(MSK_TZ).date()


def _cache_get(key: str) -> dict | None:
    row = _api_cache.get(key)
    if not row:
        return None
    ts, data = row
    if asyncio.get_running_loop().time() - ts > _API_CACHE_TTL_SEC:
        _api_cache.pop(key, None)
        return None
    return data


def _cache_set(key: str, data: dict) -> None:
    now = asyncio.get_running_loop().time()
    _api_cache[key] = (now, data)
    if len(_api_cache) <= _API_CACHE_MAX_SIZE:
        return
    cutoff = now - _API_CACHE_TTL_SEC
    stale_keys = [k for k, (ts, _) in _api_cache.items() if ts < cutoff]
    for stale_key in stale_keys:
        _api_cache.pop(stale_key, None)
    if len(_api_cache) <= _API_CACHE_MAX_SIZE:
        return
    to_remove = len(_api_cache) - _API_CACHE_MAX_SIZE
    oldest_keys = sorted(_api_cache.items(), key=lambda item: item[1][0])[:to_remove]
    for cache_key, _ in oldest_keys:
        _api_cache.pop(cache_key, None)


async def _read_json(request: web.Request) -> dict:
    try:
        payload = await request.json()
    except (JSONDecodeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid JSON body") from exc
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="JSON body must be an object")
    return payload


async def _auth_user_id(request: web.Request, bot_token: str) -> int:
    init_data = request.headers.get("X-Telegram-Init-Data", "").strip()
    if not init_data:
        init_data = request.query.get("initData", "").strip()

    if not init_data:
        if (os.getenv("MINIAPP_ALLOW_INSECURE_LOCAL") or "").strip().lower() in {"1", "true", "yes"}:
            uid = int((os.getenv("MINIAPP_LOCAL_USER_ID") or "0").strip() or "0")
            if uid > 0:
                return uid
        raise web.HTTPUnauthorized(text="Missing initData")

    try:
        user = parse_and_validate_init_data(
            bot_token,
            init_data,
            max_age_seconds=_INITDATA_MAX_AGE_SEC,
        )
    except MiniAppAuthError as exc:
        raise web.HTTPUnauthorized(text=f"Invalid initData: {exc}") from exc

    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        raise web.HTTPUnauthorized(text="Invalid Telegram user id")
    return user_id


async def _load_prices_for_positions(positions: list[dict]) -> dict[int, float | None]:
    if not positions:
        return {}
    prices: dict[int, float | None] = {}
    sem = asyncio.Semaphore(max(1, _PRICE_LOAD_CONCURRENCY))
    async with aiohttp.ClientSession() as session:

        async def one(pos: dict) -> tuple[int, float | None]:
            iid = int(pos["id"])
            asset_type = pos.get("asset_type") or ASSET_TYPE_STOCK
            secid = str(pos.get("secid") or "")
            boardid = pos.get("boardid")
            async with sem:
                try:
                    if asset_type == ASSET_TYPE_FIAT:
                        px = await get_last_price_fiat(session, secid, boardid or "CETS")
                    else:
                        px = await get_last_price_by_asset_type(session, secid, boardid, asset_type)
                    return iid, px
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
                    logger.warning(
                        "MiniApp price load failed secid=%s error=%s",
                        secid,
                        exc.__class__.__name__,
                    )
                    return iid, None

        rows = await asyncio.gather(*(one(p) for p in positions))
    for iid, px in rows:
        prices[iid] = px
    return prices


def _json_ok(payload: dict | list) -> web.Response:
    return web.json_response({"ok": True, "data": payload})


def _json_error(error_code: str, message: str, details: dict | None = None, status: int = 400) -> web.Response:
    return web.json_response(
        {"error_code": error_code, "message": message, "details": details or {}},
        status=status,
    )


def _loan_rate_limit(user_id: int, action: str) -> None:
    now = asyncio.get_running_loop().time()
    key = f"{user_id}:{action}"
    rows = _LOAN_RATE_LIMIT.get(key) or []
    fresh = [x for x in rows if now - x <= _LOAN_RATE_WINDOW_SEC]
    if len(fresh) >= _LOAN_RATE_MAX_EVENTS:
        raise web.HTTPTooManyRequests(text="rate limit exceeded")
    fresh.append(now)
    _LOAN_RATE_LIMIT[key] = fresh


def _parse_decimal(raw: Any, *, field: str, min_value: Decimal | None = None, max_value: Decimal | None = None) -> Decimal:
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{field} invalid") from exc
    if min_value is not None and value < min_value:
        raise ValueError(f"{field} too small")
    if max_value is not None and value > max_value:
        raise ValueError(f"{field} too large")
    return value


def _parse_money_text(raw: str | int | float | None) -> float:
    if raw is None:
        raise ValueError("empty amount")
    if isinstance(raw, (int, float)):
        value = float(raw)
    else:
        text = str(raw).strip().lower().replace("₽", "")
        text = text.replace("_", "").replace(" ", "").replace(",", ".")
        mult = 1.0
        if text.endswith("млн"):
            text = text[:-3]
            mult = 1_000_000.0
        elif text.endswith("m"):
            text = text[:-1]
            mult = 1_000_000.0
        if text.endswith("м"):
            text = text[:-1]
            mult = 1_000_000.0
        value = float(text) * mult
    if value <= 0:
        raise ValueError("amount must be > 0")
    return value


def _parse_month_key(raw: str | None) -> str:
    value = (raw or "").strip()
    if len(value) == 7 and value[4] == "-":
        try:
            year = int(value[:4])
            month = int(value[5:])
            if 1 <= month <= 12 and 1970 <= year <= 3000:
                return f"{year:04d}-{month:02d}"
        except ValueError:
            pass
    raise ValueError("invalid month format, expected YYYY-MM")


def _parse_date_ymd(raw: str | None) -> date:
    text = (raw or "").strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("invalid date format, expected YYYY-MM-DD") from exc


def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = min(d.day, calendar.monthrange(y, m)[1])
    return date(y, m, day)


def _annuity_payment(principal: float, annual_rate: float, months: int) -> float:
    if months <= 0:
        return 0.0
    monthly_rate = annual_rate / 12.0 / 100.0
    if monthly_rate <= 0:
        return principal / months
    factor = (1.0 + monthly_rate) ** months
    return principal * monthly_rate * factor / (factor - 1.0)


def _normalize_rate_periods(periods_raw: list[dict], loan_start: date, months: int) -> list[dict]:
    if not periods_raw:
        raise ValueError("rate_periods is required")
    out: list[dict] = []
    for row in periods_raw:
        if not isinstance(row, dict):
            continue
        start = _parse_date_ymd(str(row.get("start_date") or ""))
        end = _parse_date_ymd(str(row.get("end_date") or ""))
        if end < start:
            raise ValueError("rate period end_date before start_date")
        try:
            rate = float(row.get("annual_rate"))
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid annual_rate in rate_periods") from exc
        if rate < 0:
            raise ValueError("annual_rate must be >= 0")
        out.append({"start_date": start, "end_date": end, "annual_rate": rate})
    out.sort(key=lambda x: x["start_date"])
    loan_end = _add_months(loan_start, max(0, months - 1))
    if out[0]["start_date"] > loan_start or out[-1]["end_date"] < loan_end:
        raise ValueError("rate periods must cover the whole loan term")
    # Validate month-by-month coverage and disallow ambiguous overlaps.
    for idx in range(months):
        current = _add_months(loan_start, idx)
        matched = [p for p in out if p["start_date"] <= current <= p["end_date"]]
        if not matched:
            raise ValueError(f"rate periods gap at {current.strftime('%Y-%m')}")
        if len(matched) > 1:
            raise ValueError(f"overlapping rate periods at {current.strftime('%Y-%m')}")
    return out


def _resolve_rate_for_month(current: date, periods: list[dict]) -> float:
    for period in periods:
        if period["start_date"] <= current <= period["end_date"]:
            return float(period["annual_rate"])
    return float(periods[-1]["annual_rate"])


def _calc_loan_schedule(
    principal: float,
    start_date: date,
    months: int,
    payment_type: str,
    rate_periods: list[dict],
) -> dict:
    remain = float(principal)
    total_payment = 0.0
    first_payment = 0.0
    last_payment = 0.0
    monthly_payment_current = 0.0
    prev_rate = None
    principal_part_fixed = principal / months if months > 0 else 0.0
    for idx in range(months):
        current_date = _add_months(start_date, idx)
        annual_rate = _resolve_rate_for_month(current_date, rate_periods)
        monthly_rate = annual_rate / 100.0 / 12.0
        months_left = months - idx
        if payment_type == "annuity":
            if prev_rate is None or abs(float(prev_rate) - annual_rate) > 1e-12:
                monthly_payment_current = _annuity_payment(remain, annual_rate, months_left)
                prev_rate = annual_rate
            interest = remain * monthly_rate
            principal_part = monthly_payment_current - interest
            if principal_part > remain:
                principal_part = remain
            payment = principal_part + interest
        else:
            interest = remain * monthly_rate
            principal_part = min(remain, principal_part_fixed)
            payment = principal_part + interest
        remain = max(0.0, remain - principal_part)
        total_payment += payment
        if idx == 0:
            first_payment = payment
        if idx == months - 1:
            last_payment = payment
    overpayment = total_payment - principal
    return {
        "monthly_payment": monthly_payment_current if payment_type == "annuity" else None,
        "first_payment": first_payment,
        "last_payment": last_payment,
        "total_payment": total_payment,
        "overpayment": overpayment,
    }


async def miniapp_index(request: web.Request) -> web.Response:
    root = Path(__file__).resolve().parent / "miniapp"
    return web.FileResponse(root / "index.html")


async def miniapp_asset(request: web.Request) -> web.Response:
    root = Path(__file__).resolve().parent / "miniapp"
    name = (request.match_info.get("name") or "").strip()
    if "/" in name or ".." in name:
        raise web.HTTPNotFound()
    path = root / name
    if not path.is_file():
        raise web.HTTPNotFound()
    return web.FileResponse(path)


async def api_me(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    user_id = await _auth_user_id(request, bot_token)
    return _json_ok({"user_id": user_id})


async def api_portfolio(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    positions = await get_user_positions(db_dsn, user_id)
    if not positions:
        return _json_ok({"summary": {"total_value": 0.0, "pnl_pct": 0.0}, "positions": []})

    prices = await _load_prices_for_positions(positions)
    out_positions = []
    total_value = 0.0
    total_cost = 0.0
    for pos in positions:
        iid = int(pos["id"])
        last = prices.get(iid)
        qty = float(pos.get("total_qty") or 0.0)
        total_cost_pos = float(pos.get("total_cost") or 0.0)
        value = qty * float(last) if last is not None else 0.0
        pnl_pct = (
            (value - total_cost_pos) / total_cost_pos * 100.0
            if total_cost_pos > 1e-12 and last is not None
            else None
        )
        if last is not None:
            total_value += value
            total_cost += total_cost_pos
        out_positions.append(
            {
                "instrument_id": iid,
                "ticker": str(pos.get("secid") or ""),
                "name": str(pos.get("shortname") or pos.get("secid") or ""),
                "asset_type": pos.get("asset_type") or ASSET_TYPE_STOCK,
                "qty": qty,
                "last": last,
                "value": value,
                "share_pct": 0.0,
                "ret_30d": pnl_pct,
                "boardid": pos.get("boardid"),
                "isin": pos.get("isin"),
                "total_cost": total_cost_pos,
            }
        )

    for item in out_positions:
        if total_value > 0 and item["last"] is not None:
            item["share_pct"] = item["value"] / total_value * 100.0

    total_pnl_pct = (total_value - total_cost) / total_cost * 100.0 if total_cost > 1e-12 else 0.0
    return _json_ok(
        {
            "summary": {
                "total_value": total_value,
                "pnl_pct": total_pnl_pct,
                "as_of": _utc_now_iso(),
            },
            "positions": sorted(out_positions, key=lambda x: float(x["value"]), reverse=True),
        }
    )


async def api_search(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    _ = await _auth_user_id(request, bot_token)
    q = (request.query.get("q") or "").strip()
    asset_type = (request.query.get("asset_type") or ASSET_TYPE_STOCK).strip().lower()
    if not q:
        return _json_ok([])

    async with aiohttp.ClientSession() as session:
        if asset_type == ASSET_TYPE_METAL:
            cands = await search_metals(session, q)
        elif asset_type == ASSET_TYPE_FIAT:
            cands = await search_fiat(session, q)
        else:
            cands = await search_securities(session, q)

    return _json_ok(cands[:30])


async def api_trade_post(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    payload = await _read_json(request)

    secid = str(payload.get("secid") or "").strip().upper()
    if not secid:
        raise web.HTTPBadRequest(text="secid is required")

    asset_type = str(payload.get("asset_type") or ASSET_TYPE_STOCK).strip().lower()
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL, ASSET_TYPE_FIAT}:
        raise web.HTTPBadRequest(text="invalid asset_type")

    side = str(payload.get("side") or TRADE_SIDE_BUY).strip().lower()
    if side not in {TRADE_SIDE_BUY, TRADE_SIDE_SELL}:
        raise web.HTTPBadRequest(text="invalid side")

    trade_date = str(payload.get("trade_date") or "").strip()
    if not trade_date:
        trade_date = _today_msk().strftime("%d.%m.%Y")
    try:
        datetime.strptime(trade_date, "%d.%m.%Y")
    except ValueError as exc:
        raise web.HTTPBadRequest(text="trade_date must be DD.MM.YYYY") from exc

    qty = safe_float(payload.get("qty"), 0.0)
    price = safe_float(payload.get("price"), 0.0)
    commission = max(0.0, safe_float(payload.get("commission"), 0.0))
    if qty <= 0 or price <= 0:
        raise web.HTTPBadRequest(text="qty and price must be > 0")

    boardid = str(payload.get("boardid") or "").strip() or None
    if asset_type == ASSET_TYPE_FIAT and not boardid:
        boardid = "CETS"

    instrument_id = await upsert_instrument(
        db_dsn,
        secid=secid,
        isin=str(payload.get("isin") or "").strip() or None,
        boardid=boardid,
        shortname=str(payload.get("shortname") or secid).strip() or secid,
        asset_type=asset_type,
    )

    signed_qty = qty if side == TRADE_SIDE_BUY else -qty
    if side == TRADE_SIDE_SELL:
        total_qty_now, _, _ = await get_position_agg(db_dsn, user_id, instrument_id)
        if qty - float(total_qty_now) > 1e-12:
            raise web.HTTPBadRequest(text=f"Недостаточно позиции для продажи. Доступно: {total_qty_now:g}")

    await add_trade(
        db_dsn,
        user_id=user_id,
        instrument_id=instrument_id,
        trade_date=trade_date,
        qty=signed_qty,
        price=price,
        commission=commission,
    )

    total_qty, total_cost, avg_price = await get_position_agg(db_dsn, user_id, instrument_id)
    last = None
    try:
        async with aiohttp.ClientSession() as session:
            if asset_type == ASSET_TYPE_FIAT:
                last = await get_last_price_fiat(session, secid, boardid)
            else:
                last = await get_last_price_by_asset_type(session, secid, boardid, asset_type)
    except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
        logger.warning("MiniApp trade price load failed secid=%s error=%s", secid, exc.__class__.__name__)

    return _json_ok(
        {
            "saved": True,
            "instrument_id": instrument_id,
            "secid": secid,
            "side": side,
            "position": {
                "total_qty": total_qty,
                "total_cost": total_cost,
                "avg_price": avg_price,
                "last": last,
            },
        }
    )


async def api_asset_lookup_post(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    _ = await _auth_user_id(request, bot_token)
    payload = await _read_json(request)

    secid = str(payload.get("secid") or "").strip().upper()
    if not secid:
        raise web.HTTPBadRequest(text="secid is required")
    boardid = str(payload.get("boardid") or "").strip() or None
    asset_type = str(payload.get("asset_type") or ASSET_TYPE_STOCK).strip().lower()
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL, ASSET_TYPE_FIAT}:
        raise web.HTTPBadRequest(text="invalid asset_type")

    now = _today_msk()
    periods = [("week", 7), ("month", 30), ("half_year", 182), ("year", 365)]

    current = None
    dynamics: list[dict] = []
    async with aiohttp.ClientSession() as session:
        try:
            if asset_type == ASSET_TYPE_FIAT:
                current = await get_last_price_fiat(session, secid, boardid or "CETS")
            else:
                current = await get_last_price_by_asset_type(session, secid, boardid, asset_type)
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
            logger.warning("MiniApp asset lookup price failed secid=%s error=%s", secid, exc.__class__.__name__)

        for key, days in periods:
            try:
                history = await get_history_prices_by_asset_type(
                    session,
                    secid=secid,
                    boardid=boardid,
                    asset_type=asset_type,
                    from_date=now - timedelta(days=days),
                    till_date=now,
                )
            except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
                logger.warning(
                    "MiniApp asset lookup history failed secid=%s period=%s error=%s",
                    secid,
                    key,
                    exc.__class__.__name__,
                )
                dynamics.append({"period": key, "pct": None, "delta": None})
                continue
            if not history:
                dynamics.append({"period": key, "pct": None, "delta": None})
                continue
            base = safe_float(history[0][1], 0.0)
            end = safe_float(current, 0.0) if current is not None else safe_float(history[-1][1], 0.0)
            if base <= 0:
                dynamics.append({"period": key, "pct": None, "delta": None})
                continue
            delta = end - base
            pct = delta / base * 100.0
            dynamics.append({"period": key, "pct": pct, "delta": delta})

    return _json_ok(
        {
            "secid": secid,
            "boardid": boardid,
            "asset_type": asset_type,
            "name": str(payload.get("shortname") or payload.get("name") or secid),
            "last": current,
            "dynamics": dynamics,
        }
    )


async def api_top_movers(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    _ = await _auth_user_id(request, bot_token)

    day_str = (request.query.get("date") or "").strip()
    if day_str:
        try:
            day = date.fromisoformat(day_str)
        except ValueError as exc:
            raise web.HTTPBadRequest(text="invalid date, use YYYY-MM-DD") from exc
    else:
        day = _today_msk()

    try:
        async with aiohttp.ClientSession() as session:
            movers = await get_stock_movers_by_date(session, day)
    except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
        logger.warning("MiniApp top movers failed date=%s error=%s", day.isoformat(), exc.__class__.__name__)
        movers = []

    movers_sorted = sorted(movers, key=lambda x: safe_float(x.get("pct"), -10**9), reverse=True)
    top = movers_sorted[:10]
    bottom = list(reversed(movers_sorted[-10:])) if movers_sorted else []
    return _json_ok({"date": day.isoformat(), "top": top, "bottom": bottom, "count": len(movers_sorted)})


async def api_usd_rub(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    _ = await _auth_user_id(request, bot_token)
    try:
        async with aiohttp.ClientSession() as session:
            rate = await get_usd_rub_rate(session)
    except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
        logger.warning("MiniApp USD/RUB failed error=%s", exc.__class__.__name__)
        cached = _cache_get("usd_rub")
        if cached is not None:
            return _json_ok({**cached, "stale": True})
        rate = None
    payload = {"secid": "USDRUB_TOM", "rate": rate, "as_of": _utc_now_iso()}
    if rate is not None:
        _cache_set("usd_rub", payload)
    return _json_ok(payload)


async def api_price(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    _ = await _auth_user_id(request, bot_token)
    payload = await _read_json(request)
    secid = str(payload.get("secid") or "").strip().upper()
    if not secid:
        raise web.HTTPBadRequest(text="secid is required")
    boardid = str(payload.get("boardid") or "").strip() or None
    asset_type = str(payload.get("asset_type") or ASSET_TYPE_STOCK).strip().lower()
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL, ASSET_TYPE_FIAT}:
        raise web.HTTPBadRequest(text="invalid asset_type")
    try:
        async with aiohttp.ClientSession() as session:
            if asset_type == ASSET_TYPE_FIAT:
                # Fast preview for Mini App: do not block on ISS fallback timeouts.
                price = await get_last_price_fiat(
                    session,
                    secid,
                    boardid or "CETS",
                    allow_iss_fallback=False,
                )
            else:
                price = await get_last_price_by_asset_type(session, secid, boardid, asset_type)
    except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
        logger.warning("MiniApp price endpoint failed secid=%s error=%s", secid, exc.__class__.__name__)
        cached = _cache_get(f"price:{asset_type}:{secid}:{boardid or ''}")
        if cached is not None:
            return _json_ok({**cached, "stale": True})
        price = None
    payload = {"secid": secid, "price": price, "as_of": _utc_now_iso()}
    if price is not None:
        _cache_set(f"price:{asset_type}:{secid}:{boardid or ''}", payload)
    return _json_ok(payload)


async def api_portfolio_clear(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    deleted = await clear_user_portfolio(db_dsn, user_id)
    return _json_ok({"deleted_trades": deleted})


async def api_open_close_settings(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)

    if request.method == "GET":
        await ensure_user_alert_settings(db_dsn, user_id)
        settings = await get_user_alert_settings(db_dsn, user_id)
        return _json_ok({"open_close_enabled": bool(settings.get("open_close_enabled"))})

    payload = await _read_json(request)
    enabled = bool(payload.get("enabled"))
    await set_open_close_alert(db_dsn, user_id, enabled)
    return _json_ok({"open_close_enabled": enabled})


async def api_articles_list(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    _ = await _auth_user_id(request, bot_token)
    items = await list_active_app_texts(db_dsn)
    return _json_ok(items)


async def api_article_item(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    _ = await _auth_user_id(request, bot_token)
    text_code = str(request.match_info.get("text_code") or "").strip()
    if not text_code:
        raise web.HTTPBadRequest(text="text_code is required")
    value = await get_active_app_text(db_dsn, text_code)
    if not value:
        raise web.HTTPNotFound(text="article not found")
    return _json_ok({"text_code": text_code, "value": value})


async def api_import_xml(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)

    reader = await request.multipart()
    part = await reader.next()
    if part is None or part.name != "file":
        raise web.HTTPBadRequest(text="file field is required")

    filename = (part.filename or "broker_report.xml").strip() or "broker_report.xml"
    if not filename.lower().endswith(".xml"):
        raise web.HTTPBadRequest(text="only .xml is supported")

    chunks = []
    size = 0
    while True:
        chunk = await part.read_chunk(256 * 1024)
        if not chunk:
            break
        size += len(chunk)
        if size > MAX_XML_UPLOAD_BYTES:
            raise web.HTTPBadRequest(text="file is too large")
        chunks.append(chunk)
    xml_bytes = b"".join(chunks)

    try:
        result = await import_broker_xml_trades(
            db_dsn=db_dsn,
            user_id=user_id,
            file_name=filename,
            xml_bytes=xml_bytes,
        )
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc

    return _json_ok(
        {
            "file": result.file,
            "rows": result.rows,
            "imported": result.imported,
            "duplicates": result.duplicates,
            "skipped": result.skipped,
            "unresolved_isins": list(result.unresolved_isins),
        }
    )


async def api_alerts_get(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    alerts = await list_active_price_target_alerts(db_dsn, user_id)
    return _json_ok(alerts)


async def api_alerts_post(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    payload = await _read_json(request)

    secid = str(payload.get("secid") or "").strip()
    if not secid:
        raise web.HTTPBadRequest(text="secid is required")
    shortname = str(payload.get("shortname") or secid).strip()
    isin = str(payload.get("isin") or "").strip() or None
    boardid = str(payload.get("boardid") or "").strip() or None
    asset_type = str(payload.get("asset_type") or ASSET_TYPE_STOCK).strip().lower()
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL, ASSET_TYPE_FIAT}:
        raise web.HTTPBadRequest(text="invalid asset_type")
    if asset_type == ASSET_TYPE_FIAT and not boardid:
        boardid = "CETS"

    try:
        target_price = float(payload.get("target_price"))
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="target_price is required") from exc
    if target_price <= 0:
        raise web.HTTPBadRequest(text="target_price must be > 0")

    range_raw = payload.get("range_percent")
    if range_raw is None or str(range_raw).strip() == "":
        range_percent = 0.0
    else:
        try:
            range_percent = float(range_raw)
        except (TypeError, ValueError) as exc:
            raise web.HTTPBadRequest(text="range_percent must be a number") from exc
    if range_percent < 0 or range_percent > 50:
        raise web.HTTPBadRequest(text="range_percent out of bounds")

    instrument_id = await upsert_instrument(
        db_dsn,
        secid=secid,
        isin=isin,
        boardid=boardid,
        shortname=shortname,
        asset_type=asset_type,
    )
    alert_id = await create_price_target_alert(
        db_dsn,
        user_id=user_id,
        instrument_id=instrument_id,
        target_price=target_price,
        range_percent=range_percent,
    )
    return _json_ok({"id": alert_id})


async def api_alerts_delete(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        alert_id = int(request.match_info["alert_id"])
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid alert_id") from exc
    ok = await disable_price_target_alert(db_dsn, user_id, alert_id)
    return _json_ok({"disabled": bool(ok)})


async def api_mode(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    if request.method == "GET":
        mode = await get_user_last_mode(db_dsn, user_id)
        return _json_ok({"last_mode": mode})
    payload = await _read_json(request)
    mode = str(payload.get("mode") or "").strip().lower()
    if mode not in {"exchange", "budget"}:
        raise web.HTTPBadRequest(text="mode must be exchange or budget")
    saved = await set_user_last_mode(db_dsn, user_id, mode)
    return _json_ok({"last_mode": saved})


async def api_budget_dashboard(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    data = await get_budget_dashboard(db_dsn, user_id)
    return _json_ok(data)


async def api_budget_profile(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    if request.method == "GET":
        return _json_ok(await get_budget_profile(db_dsn, user_id))
    payload = await _read_json(request)
    income_type = payload.get("income_type")
    if income_type is not None:
        income_type = str(income_type).strip().lower()
        if income_type not in {"fixed", "irregular"}:
            raise web.HTTPBadRequest(text="income_type must be fixed or irregular")
    onboarding_mode = payload.get("onboarding_mode")
    if onboarding_mode is not None:
        onboarding_mode = str(onboarding_mode).strip().lower()
        if onboarding_mode not in {"quick", "precise"}:
            raise web.HTTPBadRequest(text="onboarding_mode must be quick or precise")
    payday_day = payload.get("payday_day")
    if payday_day is not None and str(payday_day).strip() != "":
        try:
            payday_day = int(payday_day)
        except (TypeError, ValueError) as exc:
            raise web.HTTPBadRequest(text="payday_day must be int") from exc
        if payday_day < 1 or payday_day > 31:
            raise web.HTTPBadRequest(text="payday_day out of bounds")
    else:
        payday_day = None
    income_monthly = None
    if payload.get("income_monthly") is not None:
        try:
            income_monthly = _parse_money_text(payload.get("income_monthly"))
        except ValueError as exc:
            raise web.HTTPBadRequest(text="invalid income_monthly") from exc
    expenses_base = None
    if payload.get("expenses_base") is not None:
        try:
            expenses_base = _parse_money_text(payload.get("expenses_base"))
        except ValueError as exc:
            raise web.HTTPBadRequest(text="invalid expenses_base") from exc
    onboarding_completed = payload.get("onboarding_completed")
    if onboarding_completed is not None:
        onboarding_completed = bool(onboarding_completed)
    updated = await upsert_budget_profile(
        db_dsn,
        user_id=user_id,
        onboarding_mode=onboarding_mode,
        income_type=income_type,
        income_monthly=income_monthly,
        payday_day=payday_day,
        expenses_base=expenses_base,
        onboarding_completed=onboarding_completed,
    )
    await add_budget_history_event(
        db_dsn,
        user_id=user_id,
        entity="profile",
        action="update",
        payload={
            "income_type": updated.get("income_type"),
            "income_monthly": updated.get("income_monthly"),
            "payday_day": updated.get("payday_day"),
            "expenses_base": updated.get("expenses_base"),
            "onboarding_completed": updated.get("onboarding_completed"),
        },
    )
    return _json_ok(updated)


async def api_budget_obligations(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    if request.method == "GET":
        rows = await list_budget_obligations(db_dsn, user_id)
        total = sum(float(x.get("amount_monthly") or 0.0) for x in rows)
        return _json_ok({"items": rows, "total": total})
    payload = await _read_json(request)
    title = str(payload.get("title") or "").strip()
    if not title:
        raise web.HTTPBadRequest(text="title is required")
    kind = str(payload.get("kind") or "other").strip().lower()
    try:
        amount = _parse_money_text(payload.get("amount_monthly"))
    except ValueError as exc:
        raise web.HTTPBadRequest(text="invalid amount_monthly") from exc
    debt_details = payload.get("debt_details")
    if debt_details is not None and not isinstance(debt_details, dict):
        raise web.HTTPBadRequest(text="debt_details must be object")
    item_id = await add_budget_obligation(
        db_dsn,
        user_id=user_id,
        title=title,
        amount_monthly=amount,
        kind=kind,
        debt_details=debt_details if isinstance(debt_details, dict) else None,
    )
    await add_budget_history_event(
        db_dsn,
        user_id=user_id,
        entity="obligation",
        entity_id=item_id,
        action="create",
        payload={"title": title, "kind": kind, "amount_monthly": amount},
    )
    return _json_ok({"id": item_id})


async def api_budget_savings(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    if request.method == "GET":
        rows = await list_budget_savings(db_dsn, user_id)
        total = sum(float(x.get("amount") or 0.0) for x in rows)
        return _json_ok({"items": rows, "total": total})
    payload = await _read_json(request)
    kind = str(payload.get("kind") or "other").strip().lower()
    title = str(payload.get("title") or kind or "Сбережения").strip()
    try:
        amount = _parse_money_text(payload.get("amount"))
    except ValueError as exc:
        raise web.HTTPBadRequest(text="invalid amount") from exc
    item_id = await add_budget_saving(db_dsn, user_id, kind=kind, title=title, amount=amount)
    await add_budget_history_event(
        db_dsn,
        user_id=user_id,
        entity="saving",
        entity_id=item_id,
        action="create",
        payload={"kind": kind, "title": title, "amount": amount},
    )
    return _json_ok({"id": item_id})


async def api_budget_saving_item(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        saving_id = int(request.match_info["saving_id"])
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid saving_id") from exc
    payload = await _read_json(request)
    action = str(payload.get("action") or "").strip().lower()
    if action not in {"edit", "delete", "topup", "spend"}:
        raise web.HTTPBadRequest(text="invalid action")
    if action == "delete":
        ok = await disable_budget_saving(db_dsn, user_id, saving_id)
        if ok:
            await add_budget_history_event(
                db_dsn,
                user_id=user_id,
                entity="saving",
                entity_id=saving_id,
                action="delete",
                payload={},
            )
        return _json_ok({"deleted": ok})
    if action == "edit":
        kwargs: dict[str, Any] = {}
        if payload.get("kind") is not None:
            kwargs["kind"] = str(payload.get("kind") or "other").strip().lower()
        if payload.get("title") is not None:
            title = str(payload.get("title") or "").strip()
            if not title:
                raise web.HTTPBadRequest(text="title required")
            kwargs["title"] = title
        ok = await update_budget_saving(db_dsn, user_id=user_id, saving_id=saving_id, **kwargs)
        if ok:
            await add_budget_history_event(
                db_dsn,
                user_id=user_id,
                entity="saving",
                entity_id=saving_id,
                action="edit",
                payload=kwargs,
            )
        return _json_ok({"updated": ok})
    try:
        amount = _parse_money_text(payload.get("amount"))
    except ValueError as exc:
        raise web.HTTPBadRequest(text="invalid amount") from exc
    delta = amount if action == "topup" else -amount
    try:
        result = await change_budget_saving_amount(db_dsn, user_id=user_id, saving_id=saving_id, delta=delta)
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    if result is None:
        raise web.HTTPNotFound(text="saving not found")
    await add_budget_history_event(
        db_dsn,
        user_id=user_id,
        entity="saving",
        entity_id=saving_id,
        action=action,
        payload={"amount": amount, "before": result["amount_before"], "after": result["amount_after"]},
    )
    return _json_ok(result)


async def api_budget_incomes(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    if request.method == "GET":
        rows = await list_budget_incomes(db_dsn, user_id)
        total = sum(float(x.get("amount_monthly") or 0.0) for x in rows)
        return _json_ok({"items": rows, "total": total})
    payload = await _read_json(request)
    kind = str(payload.get("kind") or "other").strip().lower()
    title = str(payload.get("title") or "").strip()
    if not title:
        raise web.HTTPBadRequest(text="title required")
    try:
        amount = _parse_money_text(payload.get("amount_monthly"))
    except ValueError as exc:
        raise web.HTTPBadRequest(text="invalid amount_monthly") from exc
    item_id = await add_budget_income(db_dsn, user_id, kind=kind, title=title, amount_monthly=amount)
    await add_budget_history_event(
        db_dsn,
        user_id=user_id,
        entity="income",
        entity_id=item_id,
        action="create",
        payload={"kind": kind, "title": title, "amount_monthly": amount},
    )
    return _json_ok({"id": item_id})


async def api_budget_income_item(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        income_id = int(request.match_info["income_id"])
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid income_id") from exc
    payload = await _read_json(request)
    action = str(payload.get("action") or "").strip().lower()
    if action not in {"edit", "delete"}:
        raise web.HTTPBadRequest(text="invalid action")
    if action == "delete":
        ok = await disable_budget_income(db_dsn, user_id, income_id)
        if ok:
            await add_budget_history_event(
                db_dsn,
                user_id=user_id,
                entity="income",
                entity_id=income_id,
                action="delete",
                payload={},
            )
        return _json_ok({"deleted": ok})
    kwargs = {}
    if payload.get("kind") is not None:
        kwargs["kind"] = str(payload.get("kind") or "other").strip().lower()
    if payload.get("title") is not None:
        title = str(payload.get("title") or "").strip()
        if not title:
            raise web.HTTPBadRequest(text="title required")
        kwargs["title"] = title
    if payload.get("amount_monthly") is not None:
        try:
            kwargs["amount_monthly"] = _parse_money_text(payload.get("amount_monthly"))
        except ValueError as exc:
            raise web.HTTPBadRequest(text="invalid amount_monthly") from exc
    ok = await update_budget_income(db_dsn, user_id, income_id, **kwargs)
    if ok:
        await add_budget_history_event(
            db_dsn,
            user_id=user_id,
            entity="income",
            entity_id=income_id,
            action="edit",
            payload=kwargs,
        )
    return _json_ok({"updated": ok})


def _build_expense_payload(payload: dict) -> tuple[str, str, float, dict]:
    kind = str(payload.get("kind") or "").strip().lower()
    if kind not in {"rent", "mortgage", "loan", "utilities", "other"}:
        raise web.HTTPBadRequest(text="invalid expense kind")
    title = str(payload.get("title") or "").strip()
    if not title:
        title = {
            "rent": "Аренда",
            "mortgage": "Ипотека",
            "loan": "Кредит",
            "utilities": "ЖКХ",
            "other": "Прочие расходы",
        }[kind]

    if kind in {"rent", "utilities"}:
        pay_date = _parse_date_ymd(str(payload.get("payment_date") or ""))
        amount = _parse_money_text(payload.get("amount_monthly"))
        details = {"payment_date": pay_date.isoformat()}
        return kind, title, amount, details

    if kind == "other":
        amount = _parse_money_text(payload.get("amount_monthly"))
        details = {}
        return kind, title, amount, details

    start_date = _parse_date_ymd(str(payload.get("start_date") or ""))
    try:
        principal = _parse_money_text(payload.get("principal"))
        months = int(payload.get("months"))
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid principal or months") from exc
    if months <= 0:
        raise web.HTTPBadRequest(text="months must be > 0")
    payment_type = str(payload.get("payment_type") or "annuity").strip().lower()
    if payment_type not in {"annuity", "diff"}:
        raise web.HTTPBadRequest(text="payment_type must be annuity or diff")
    rates_raw = payload.get("rate_periods")
    if not isinstance(rates_raw, list):
        raise web.HTTPBadRequest(text="rate_periods must be list")
    try:
        rate_periods = _normalize_rate_periods(rates_raw, start_date, months)
        calc = _calc_loan_schedule(
            principal=principal,
            start_date=start_date,
            months=months,
            payment_type=payment_type,
            rate_periods=rate_periods,
        )
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    amount = float(calc["monthly_payment"] if calc["monthly_payment"] is not None else calc["first_payment"])
    details = {
        "start_date": start_date.isoformat(),
        "principal": principal,
        "months": months,
        "payment_type": payment_type,
        "rate_periods": [
            {
                "start_date": r["start_date"].isoformat(),
                "end_date": r["end_date"].isoformat(),
                "annual_rate": r["annual_rate"],
            }
            for r in rate_periods
        ],
        "calculation": calc,
    }
    return kind, title, amount, details


async def api_budget_expenses(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    if request.method == "GET":
        rows = await list_budget_expenses(db_dsn, user_id)
        total = sum(float(x.get("amount_monthly") or 0.0) for x in rows)
        return _json_ok({"items": rows, "total": total})
    payload = await _read_json(request)
    kind, title, amount, details = _build_expense_payload(payload)
    item_id = await add_budget_expense(
        db_dsn,
        user_id=user_id,
        kind=kind,
        title=title,
        amount_monthly=amount,
        payload=details,
    )
    await add_budget_history_event(
        db_dsn,
        user_id=user_id,
        entity="expense",
        entity_id=item_id,
        action="create",
        payload={"kind": kind, "title": title, "amount_monthly": amount},
    )
    return _json_ok({"id": item_id, "amount_monthly": amount, "details": details})


async def api_budget_expense_item(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        expense_id = int(request.match_info["expense_id"])
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid expense_id") from exc
    payload = await _read_json(request)
    action = str(payload.get("action") or "").strip().lower()
    if action not in {"edit", "delete"}:
        raise web.HTTPBadRequest(text="invalid action")
    if action == "delete":
        ok = await disable_budget_expense(db_dsn, user_id, expense_id)
        if ok:
            await add_budget_history_event(
                db_dsn,
                user_id=user_id,
                entity="expense",
                entity_id=expense_id,
                action="delete",
                payload={},
            )
        return _json_ok({"deleted": ok})
    kind, title, amount, details = _build_expense_payload(payload)
    ok = await update_budget_expense(
        db_dsn,
        user_id=user_id,
        expense_id=expense_id,
        kind=kind,
        title=title,
        amount_monthly=amount,
        payload=details,
    )
    if ok:
        await add_budget_history_event(
            db_dsn,
            user_id=user_id,
            entity="expense",
            entity_id=expense_id,
            action="edit",
            payload={"kind": kind, "title": title, "amount_monthly": amount},
        )
    return _json_ok({"updated": ok, "amount_monthly": amount, "details": details})


async def api_budget_reset(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    result = await reset_budget_data(db_dsn, user_id)
    await add_budget_history_event(
        db_dsn,
        user_id=user_id,
        entity="budget",
        action="reset",
        payload=result,
    )
    return _json_ok(result)


async def api_budget_notification_settings(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    if request.method == "GET":
        return _json_ok(await get_budget_notification_settings(db_dsn, user_id))
    payload = await _read_json(request)
    updated = await set_budget_notification_settings(
        db_dsn,
        user_id=user_id,
        budget_summary_enabled=payload.get("budget_summary_enabled"),
        goal_deadline_enabled=payload.get("goal_deadline_enabled"),
        month_close_enabled=payload.get("month_close_enabled"),
    )
    await add_budget_history_event(
        db_dsn,
        user_id=user_id,
        entity="settings",
        action="notification_update",
        payload=updated,
    )
    return _json_ok(updated)


def _calc_fund_strategy(
    target_amount: float,
    already_saved: float,
    target_month: str,
    income: float,
    obligations_total: float,
    expenses_base: float,
) -> dict:
    now = _today_msk()
    target_dt = datetime.strptime(target_month + "-01", "%Y-%m-%d").date()
    months_left = max(1, (target_dt.year - now.year) * 12 + (target_dt.month - now.month))
    need = max(0.0, target_amount - already_saved)
    required_per_month = need / months_left if need > 0 else 0.0
    free = income - obligations_total - expenses_base
    gap = max(0.0, required_per_month - free)
    return {
        "need": need,
        "months_left": months_left,
        "required_per_month": required_per_month,
        "free": free,
        "is_feasible": gap <= 1e-9,
        "gap": gap,
    }


def _normalize_goal_checklist(raw: object) -> list[dict]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise web.HTTPBadRequest(text="checklist must be list")
    out: list[dict] = []
    for item in raw:
        if isinstance(item, dict):
            text = str(item.get("text") or "").strip()
            done = bool(item.get("done"))
        else:
            text = str(item or "").strip()
            done = False
        if text:
            out.append({"text": text, "done": done})
    return out


async def api_budget_fund_strategy(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    payload = await _read_json(request)
    title = str(payload.get("title") or "").strip()
    if not title:
        raise web.HTTPBadRequest(text="title required")
    try:
        target_amount = _parse_money_text(payload.get("target_amount"))
        already_saved = float(payload.get("already_saved") or 0.0)
    except (ValueError, TypeError) as exc:
        raise web.HTTPBadRequest(text="invalid amounts") from exc
    if already_saved < 0:
        raise web.HTTPBadRequest(text="already_saved must be >= 0")
    target_date_raw = str(payload.get("target_date") or "").strip()
    if target_date_raw:
        try:
            target_month = _parse_date_ymd(target_date_raw).strftime("%Y-%m")
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
    else:
        try:
            target_month = _parse_month_key(str(payload.get("target_month") or ""))
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc

    dashboard = await get_budget_dashboard(db_dsn, user_id)
    strategy = _calc_fund_strategy(
        target_amount=target_amount,
        already_saved=already_saved,
        target_month=target_month,
        income=float(dashboard["income"]),
        obligations_total=float(dashboard["obligations_total"]),
        expenses_base=float(dashboard["expenses_base"]),
    )
    return _json_ok(
        {
            "fund_title": title,
            "target_amount": target_amount,
            "already_saved": already_saved,
            "target_month": target_month,
            "priority": str(payload.get("priority") or "medium"),
            "budget_now": {
                "income": dashboard["income"],
                "obligations_total": dashboard["obligations_total"],
                "expenses_base": dashboard["expenses_base"],
                "free": dashboard["free"],
            },
            **strategy,
        }
    )


async def api_budget_funds(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    if request.method == "GET":
        return _json_ok(await list_budget_funds(db_dsn, user_id))
    payload = await _read_json(request)
    title = str(payload.get("title") or "").strip()
    if not title:
        raise web.HTTPBadRequest(text="title required")
    try:
        target_amount = _parse_money_text(payload.get("target_amount"))
    except ValueError as exc:
        raise web.HTTPBadRequest(text="invalid target_amount") from exc
    target_date_raw = str(payload.get("target_date") or "").strip()
    if target_date_raw:
        try:
            target_date = _parse_date_ymd(target_date_raw)
            target_month = target_date.strftime("%Y-%m")
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
    else:
        target_date = None
        try:
            target_month = _parse_month_key(str(payload.get("target_month") or ""))
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
    already_saved_raw = payload.get("already_saved")
    try:
        already_saved = float(already_saved_raw or 0.0)
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid already_saved") from exc
    if already_saved < 0:
        raise web.HTTPBadRequest(text="already_saved must be >= 0")
    priority = str(payload.get("priority") or "medium").strip().lower()
    if priority not in {"high", "medium", "low"}:
        raise web.HTTPBadRequest(text="priority must be high|medium|low")
    description = str(payload.get("description") or "").strip()
    checklist = _normalize_goal_checklist(payload.get("checklist"))
    fund_payload = {
        "description": description,
        "target_date": target_date.isoformat() if target_date else f"{target_month}-01",
        "checklist": checklist,
    }
    fund_id = await create_budget_fund(
        db_dsn,
        user_id=user_id,
        title=title,
        target_amount=target_amount,
        already_saved=already_saved,
        target_month=target_month,
        priority=priority,
        payload=fund_payload,
    )
    await add_budget_history_event(
        db_dsn,
        user_id=user_id,
        entity="goal",
        entity_id=fund_id,
        action="create",
        payload={"title": title, "target_amount": target_amount, "target_month": target_month},
    )
    return _json_ok({"id": fund_id})


async def api_budget_fund_item(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        fund_id = int(request.match_info["fund_id"])
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid fund_id") from exc
    payload = await _read_json(request)
    action = str(payload.get("action") or "").strip().lower()
    if action not in {"topup", "edit", "pause", "delete", "autopilot"}:
        raise web.HTTPBadRequest(text="invalid action")

    if action == "topup":
        try:
            amount = _parse_money_text(payload.get("amount"))
        except ValueError as exc:
            raise web.HTTPBadRequest(text="invalid amount") from exc
        rows = await list_budget_funds(db_dsn, user_id)
        fund = next((x for x in rows if int(x["id"]) == fund_id), None)
        if not fund:
            raise web.HTTPNotFound(text="fund not found")
        ok = await update_budget_fund(
            db_dsn,
            user_id=user_id,
            fund_id=fund_id,
            already_saved=float(fund["already_saved"]) + amount,
        )
        if ok:
            await add_budget_history_event(
                db_dsn,
                user_id=user_id,
                entity="goal",
                entity_id=fund_id,
                action="topup",
                payload={"amount": amount},
            )
        return _json_ok({"updated": ok})

    if action == "edit":
        kwargs = {}
        if payload.get("title") is not None:
            title = str(payload.get("title") or "").strip()
            if not title:
                raise web.HTTPBadRequest(text="title required")
            kwargs["title"] = title
        if payload.get("target_amount") is not None:
            try:
                kwargs["target_amount"] = _parse_money_text(payload.get("target_amount"))
            except ValueError as exc:
                raise web.HTTPBadRequest(text="invalid target_amount") from exc
        target_date_raw = payload.get("target_date")
        if target_date_raw is not None:
            try:
                d = _parse_date_ymd(str(target_date_raw))
                kwargs["target_month"] = d.strftime("%Y-%m")
            except ValueError as exc:
                raise web.HTTPBadRequest(text=str(exc)) from exc
        elif payload.get("target_month") is not None:
            try:
                kwargs["target_month"] = _parse_month_key(str(payload.get("target_month")))
            except ValueError as exc:
                raise web.HTTPBadRequest(text=str(exc)) from exc
        current_rows = await list_budget_funds(db_dsn, user_id)
        current = next((x for x in current_rows if int(x["id"]) == fund_id), None)
        if current:
            description = str(payload.get("description") if payload.get("description") is not None else current.get("description") or "").strip()
            checklist = _normalize_goal_checklist(payload.get("checklist") if payload.get("checklist") is not None else current.get("checklist"))
            kwargs["payload"] = {
                "description": description,
                "target_date": str(payload.get("target_date") or current.get("target_date") or f"{current.get('target_month')}-01"),
                "checklist": checklist,
            }
        ok = await update_budget_fund(db_dsn, user_id=user_id, fund_id=fund_id, **kwargs)
        if ok:
            await add_budget_history_event(
                db_dsn,
                user_id=user_id,
                entity="goal",
                entity_id=fund_id,
                action="edit",
                payload=kwargs,
            )
        return _json_ok({"updated": ok})

    if action == "autopilot":
        enabled = bool(payload.get("enabled"))
        ok = await update_budget_fund(db_dsn, user_id=user_id, fund_id=fund_id, autopilot_enabled=enabled)
        if ok:
            await add_budget_history_event(
                db_dsn,
                user_id=user_id,
                entity="goal",
                entity_id=fund_id,
                action="autopilot",
                payload={"enabled": enabled},
            )
        return _json_ok({"updated": ok, "autopilot_enabled": enabled})

    status = "paused" if action == "pause" else "deleted"
    ok = await update_budget_fund(db_dsn, user_id=user_id, fund_id=fund_id, status=status)
    if ok:
        await add_budget_history_event(
            db_dsn,
            user_id=user_id,
            entity="goal",
            entity_id=fund_id,
            action=status,
            payload={},
        )
    return _json_ok({"updated": ok, "status": status})


async def api_budget_month_close(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    payload = await _read_json(request)

    month_raw = payload.get("month_key") or (_today_msk().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    try:
        month_key = _parse_month_key(str(month_raw))
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    planned_raw = payload.get("planned_expenses_base")
    try:
        planned_expenses_base = float(planned_raw or 0.0)
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid planned_expenses_base") from exc
    if planned_expenses_base < 0:
        raise web.HTTPBadRequest(text="planned_expenses_base must be >= 0")
    try:
        actual_expenses_base = _parse_money_text(payload.get("actual_expenses_base"))
    except ValueError as exc:
        raise web.HTTPBadRequest(text="invalid actual_expenses_base") from exc
    extra_income_items = payload.get("extra_income_items") or []
    if not isinstance(extra_income_items, list):
        raise web.HTTPBadRequest(text="extra_income_items must be list")
    clean_items = []
    for item in extra_income_items:
        if not isinstance(item, dict):
            continue
        label = str(item.get("type") or "Другое").strip() or "Другое"
        amount_raw = item.get("amount")
        try:
            amount = _parse_money_text(amount_raw)
        except ValueError:
            continue
        comment = str(item.get("comment") or "").strip()
        clean_items.append({"type": label, "amount": amount, "comment": comment})

    result = await close_budget_month(
        db_dsn,
        user_id=user_id,
        month_key=month_key,
        planned_expenses_base=planned_expenses_base,
        actual_expenses_base=actual_expenses_base,
        extra_income_items=clean_items,
    )
    await add_budget_history_event(
        db_dsn,
        user_id=user_id,
        entity="month_close",
        action="close",
        payload=result,
    )
    return _json_ok(result)


async def api_budget_history(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    limit_raw = (request.query.get("limit") or "").strip()
    try:
        limit = int(limit_raw) if limit_raw else 100
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid limit") from exc
    rows = await list_budget_history(db_dsn, user_id, limit=limit)
    return _json_ok({"items": rows})


def _loan_row_to_input(loan: dict[str, Any]) -> LoanInput:
    issue_date = _parse_date_ymd(str(loan.get("issue_date"))) if loan.get("issue_date") else None
    return LoanInput(
        principal=Decimal(str(loan["principal"])),
        current_principal=Decimal(str(loan.get("current_principal") or loan["principal"])),
        annual_rate=Decimal(str(loan["annual_rate"])),
        payment_type=str(loan["payment_type"]),
        term_months=int(loan["term_months"]),
        first_payment_date=_parse_date_ymd(str(loan["first_payment_date"])),
        issue_date=issue_date,
        currency=str(loan.get("currency") or "RUB"),
        calc_date=_today_msk(),
        accrual_mode=str(loan.get("accrual_mode") or "MONTHLY").upper(),  # type: ignore[arg-type]
        insurance_monthly=Decimal(str(loan.get("insurance_monthly") or "0")),
        one_time_costs=Decimal(str(loan.get("one_time_costs") or "0")),
    )


def _normalize_loan_rate_periods(periods_raw: Any, first_payment_date: date, term_months: int, fallback_rate: Decimal) -> list[dict[str, Any]]:
    if not isinstance(periods_raw, list) or not periods_raw:
        return [
            {
                "start_date": first_payment_date,
                "end_date": _add_months(first_payment_date, max(0, term_months - 1)),
                "annual_rate": fallback_rate,
            }
        ]
    out: list[dict[str, Any]] = []
    for row in periods_raw:
        if not isinstance(row, dict):
            raise ValueError("rate_periods must contain objects")
        start = _parse_date_ymd(str(row.get("start_date") or ""))
        end = _parse_date_ymd(str(row.get("end_date") or ""))
        if end < start:
            raise ValueError("rate period end_date before start_date")
        rate = _parse_decimal(row.get("annual_rate"), field="annual_rate", min_value=Decimal("0"), max_value=Decimal("100"))
        out.append({"start_date": start, "end_date": end, "annual_rate": rate})
    out.sort(key=lambda x: x["start_date"])

    # Continuous non-overlapping periods improve predictability for the user.
    if out[0]["start_date"] > first_payment_date:
        raise ValueError("rate_periods must start not later than first_payment_date")
    expected_start = out[0]["start_date"]
    for row in out:
        if row["start_date"] != expected_start:
            raise ValueError("rate_periods must be continuous without gaps")
        expected_start = row["end_date"] + timedelta(days=1)
    return out


def _loan_event_from_row(row: dict[str, Any]) -> ExtraPaymentEvent | RateChangeEvent | HolidayEvent | None:
    event_type = str(row.get("event_type") or "").upper()
    payload = row.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    if event_type == "EXTRA_PAYMENT":
        return ExtraPaymentEvent(
            date=_parse_date_ymd(str(row["event_date"])),
            amount=Decimal(str(payload.get("amount") or "0")),
            mode=str(payload.get("mode") or "ONE_TIME").upper(),  # type: ignore[arg-type]
            strategy=str(payload.get("strategy") or "REDUCE_TERM").upper(),  # type: ignore[arg-type]
            end_date=_parse_date_ymd(str(payload.get("end_date"))) if payload.get("end_date") else None,
        )
    if event_type == "RATE_CHANGE":
        return RateChangeEvent(
            date=_parse_date_ymd(str(row["event_date"])),
            annual_rate=Decimal(str(payload.get("annual_rate") or "0")),
        )
    if event_type == "HOLIDAY":
        return HolidayEvent(
            start_date=_parse_date_ymd(str(payload.get("start_date") or row["event_date"])),
            end_date=_parse_date_ymd(str(payload.get("end_date") or row["event_date"])),
            holiday_type=str(payload.get("holiday_type") or "INTEREST_ONLY").upper(),  # type: ignore[arg-type]
        )
    return None


def _is_valid_loan_summary(summary: dict[str, Any]) -> bool:
    if not isinstance(summary, dict):
        return False
    required = ("monthly_payment", "remaining_balance", "payments_count")
    for key in required:
        if summary.get(key) is None:
            return False
    return True


async def _compute_loan_cached(
    db_dsn: str,
    user_id: int,
    loan_row: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> tuple[dict, list[dict], int]:
    loan_input = _loan_row_to_input(loan_row)
    actual_rows: list[dict[str, Any]] = []
    if _is_postgres_dsn(db_dsn):
        try:
            actual_rows = await list_loan_actual_payments(db_dsn, user_id, int(loan_row["id"]))
        except Exception:
            logger.exception("Failed to load actual loan payments for loan_id=%s", loan_row.get("id"))
    if actual_rows:
        total_principal_paid = sum(Decimal(str(r.get("principal_paid") or "0")) for r in actual_rows)
        adjusted_current = loan_input.current_principal - total_principal_paid
        if adjusted_current <= Decimal("0"):
            adjusted_current = Decimal("0.01")
        loan_input.current_principal = adjusted_current
        latest_date = max(_parse_date_ymd(str(r["payment_date"])) for r in actual_rows)
        loan_input.calc_date = latest_date + timedelta(days=1)
    events: list[ExtraPaymentEvent | RateChangeEvent | HolidayEvent] = []
    for row in event_rows:
        ev = _loan_event_from_row(row)
        if ev is not None:
            events.append(ev)

    version, vhash = loan_version_hash(loan_input, events)
    cached = await get_loan_schedule_cache(db_dsn, user_id, int(loan_row["id"]))
    if (
        cached
        and str(cached.get("version_hash")) == vhash
        and _is_valid_loan_summary(cached.get("summary_json") or {})
    ):
        return (
            cached.get("summary_json") or {},
            cached.get("payload_json") or [],
            int(cached.get("version") or version),
        )

    summary, schedule, version_calc, hash_calc = calculate_loan_schedule(loan_input, events)
    await upsert_loan_schedule_cache(
        db_dsn,
        loan_id=int(loan_row["id"]),
        version=version_calc,
        version_hash=hash_calc,
        summary_json=summary,
        payload_json=schedule,
    )
    return summary, schedule, version_calc


async def api_loans(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    if request.method == "GET":
        items = await list_loan_accounts(db_dsn, user_id)
        return _json_ok({"items": items})
    try:
        _loan_rate_limit(user_id, "loan_create")
    except web.HTTPTooManyRequests:
        return _json_error("RATE_LIMIT", "Слишком много запросов", status=429)
    payload = await _read_json(request)
    try:
        principal = _parse_decimal(payload.get("principal"), field="principal", min_value=Decimal("0.01"))
        current_principal = _parse_decimal(payload.get("current_principal"), field="current_principal", min_value=Decimal("0.01"))
        if current_principal > principal:
            raise ValueError("current_principal must be <= principal")
        annual_rate = _parse_decimal(payload.get("annual_rate") if payload.get("annual_rate") is not None else "0", field="annual_rate", min_value=Decimal("0"), max_value=Decimal("100"))
        payment_type = str(payload.get("payment_type") or "ANNUITY").strip().upper()
        if payment_type not in {"ANNUITY", "DIFFERENTIATED"}:
            raise ValueError("payment_type invalid")
        term_months = int(payload.get("term_months"))
        if term_months < 1 or term_months > 600:
            raise ValueError("term_months invalid")
        first_payment_date = _parse_date_ymd(str(payload.get("first_payment_date") or ""))
        issue_date = _parse_date_ymd(str(payload.get("issue_date"))) if payload.get("issue_date") else None
        if issue_date and first_payment_date <= issue_date:
            raise ValueError("first_payment_date must be after issue_date")
        currency = str(payload.get("currency") or "RUB").strip().upper()[:3]
        name = str(payload.get("name") or "").strip() or None
        accrual_mode = str(payload.get("accrual_mode") or "MONTHLY").strip().upper()
        if accrual_mode not in {"MONTHLY", "ACT_365"}:
            raise ValueError("accrual_mode must be MONTHLY or ACT_365")
        insurance_monthly = _parse_decimal(payload.get("insurance_monthly") if payload.get("insurance_monthly") is not None else "0", field="insurance_monthly", min_value=Decimal("0"))
        one_time_costs = _parse_decimal(payload.get("one_time_costs") if payload.get("one_time_costs") is not None else "0", field="one_time_costs", min_value=Decimal("0"))
        rate_periods = _normalize_loan_rate_periods(payload.get("rate_periods"), first_payment_date, term_months, annual_rate)
        base_rate = Decimal(rate_periods[0]["annual_rate"])
    except (ValueError, TypeError) as exc:
        return _json_error("VALIDATION_ERROR", str(exc), status=400)

    loan_id = await create_loan_account(
        db_dsn,
        user_id=user_id,
        name=name,
        principal=principal,
        current_principal=current_principal,
        annual_rate=base_rate,
        accrual_mode=accrual_mode,
        insurance_monthly=insurance_monthly,
        one_time_costs=one_time_costs,
        payment_type=payment_type,
        term_months=term_months,
        first_payment_date=first_payment_date,
        issue_date=issue_date,
        currency=currency,
    )
    # Persist floating-rate periods as RATE_CHANGE milestones.
    for idx, period in enumerate(rate_periods):
        await create_loan_event(
            db_dsn,
            user_id=user_id,
            loan_id=loan_id,
            event_type="RATE_CHANGE",
            event_date=period["start_date"],
            payload={
                "annual_rate": format(Decimal(period["annual_rate"]), "f"),
                "period_end_date": period["end_date"].isoformat(),
                "source": "RATE_PERIOD",
            },
            client_request_id=f"loan-create-rate-{loan_id}-{idx}",
        )
    return web.json_response({"loan_id": loan_id, "status": "ACTIVE"}, status=201)


async def api_loan_item(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        loan_id = int(request.match_info["loan_id"])
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    if request.method == "DELETE":
        ok = await archive_loan_account(db_dsn, user_id, loan_id)
        return _json_ok({"archived": ok})

    loan = await get_loan_account(db_dsn, user_id, loan_id)
    if not loan:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    events = await list_loan_events(db_dsn, user_id, loan_id)
    summary, _, version = await _compute_loan_cached(db_dsn, user_id, loan, events)
    return _json_ok({"loan": loan, "summary": summary, "version": version})


async def api_loan_schedule(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        loan_id = int(request.match_info["loan_id"])
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    loan = await get_loan_account(db_dsn, user_id, loan_id)
    if not loan:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    events = await list_loan_events(db_dsn, user_id, loan_id)
    summary, schedule, version = await _compute_loan_cached(db_dsn, user_id, loan, events)

    try:
        page = max(1, int((request.query.get("page") or "1").strip() or "1"))
        page_size = int((request.query.get("page_size") or "60").strip() or "60")
        page_size = min(120, max(1, page_size))
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid page or page_size", status=400)
    start = (page - 1) * page_size
    items = schedule[start:start + page_size]
    return _json_ok(
        {
            "version": version,
            "summary": summary,
            "page": page,
            "page_size": page_size,
            "total": len(schedule),
            "items": items,
        }
    )


async def api_loan_events_extra(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        _loan_rate_limit(user_id, "loan_event")
        loan_id = int(request.match_info["loan_id"])
    except web.HTTPTooManyRequests:
        return _json_error("RATE_LIMIT", "Слишком много запросов", status=429)
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    payload = await _read_json(request)
    try:
        event_date = _parse_date_ymd(str(payload.get("date") or ""))
        amount = _parse_decimal(payload.get("amount"), field="amount", min_value=Decimal("0.01"))
        mode = str(payload.get("mode") or "ONE_TIME").upper()
        strategy = str(payload.get("strategy") or "REDUCE_TERM").upper()
        if mode not in {"ONE_TIME", "MONTHLY", "WEEKLY", "BIWEEKLY", "YEARLY"}:
            raise ValueError("mode invalid")
        if strategy not in {"REDUCE_TERM", "REDUCE_PAYMENT"}:
            raise ValueError("strategy invalid")
        end_date = _parse_date_ymd(str(payload.get("end_date"))) if payload.get("end_date") else None
        if end_date and end_date < event_date:
            raise ValueError("end_date must be >= date")
    except (ValueError, TypeError) as exc:
        return _json_error("VALIDATION_ERROR", str(exc), status=400)

    req_id = (request.headers.get("Idempotency-Key") or "").strip() or None
    event_id, created = await create_loan_event(
        db_dsn,
        user_id=user_id,
        loan_id=loan_id,
        event_type="EXTRA_PAYMENT",
        event_date=event_date,
        payload={
            "amount": format(amount, "f"),
            "mode": mode,
            "strategy": strategy,
            "end_date": end_date.isoformat() if end_date else None,
        },
        client_request_id=req_id,
    )
    if event_id == 0:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    return _json_ok({"event_id": event_id, "created": created})


async def api_loan_events_rate(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        _loan_rate_limit(user_id, "loan_event")
        loan_id = int(request.match_info["loan_id"])
    except web.HTTPTooManyRequests:
        return _json_error("RATE_LIMIT", "Слишком много запросов", status=429)
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    payload = await _read_json(request)
    try:
        event_date = _parse_date_ymd(str(payload.get("date") or ""))
        annual_rate = _parse_decimal(payload.get("annual_rate"), field="annual_rate", min_value=Decimal("0"), max_value=Decimal("100"))
    except (ValueError, TypeError) as exc:
        return _json_error("VALIDATION_ERROR", str(exc), status=400)

    req_id = (request.headers.get("Idempotency-Key") or "").strip() or None
    event_id, created = await create_loan_event(
        db_dsn,
        user_id=user_id,
        loan_id=loan_id,
        event_type="RATE_CHANGE",
        event_date=event_date,
        payload={"annual_rate": format(annual_rate, "f")},
        client_request_id=req_id,
    )
    if event_id == 0:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    return _json_ok({"event_id": event_id, "created": created})


async def api_loan_events_holiday(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        _loan_rate_limit(user_id, "loan_event")
        loan_id = int(request.match_info["loan_id"])
    except web.HTTPTooManyRequests:
        return _json_error("RATE_LIMIT", "Слишком много запросов", status=429)
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    payload = await _read_json(request)
    try:
        start_date = _parse_date_ymd(str(payload.get("start_date") or ""))
        end_date = _parse_date_ymd(str(payload.get("end_date") or ""))
        if end_date < start_date:
            raise ValueError("holiday end_date before start_date")
        holiday_type = str(payload.get("holiday_type") or "INTEREST_ONLY").upper()
        if holiday_type not in {"INTEREST_ONLY", "PAUSE_CAPITALIZE"}:
            raise ValueError("holiday_type invalid")
    except (ValueError, TypeError) as exc:
        return _json_error("VALIDATION_ERROR", str(exc), status=400)

    req_id = (request.headers.get("Idempotency-Key") or "").strip() or None
    event_id, created = await create_loan_event(
        db_dsn,
        user_id=user_id,
        loan_id=loan_id,
        event_type="HOLIDAY",
        event_date=start_date,
        payload={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "holiday_type": holiday_type,
        },
        client_request_id=req_id,
    )
    if event_id == 0:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    return _json_ok({"event_id": event_id, "created": created})


async def api_loan_scenario_preview(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        _loan_rate_limit(user_id, "loan_preview")
        loan_id = int(request.match_info["loan_id"])
    except web.HTTPTooManyRequests:
        return _json_error("RATE_LIMIT", "Слишком много запросов", status=429)
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    payload = await _read_json(request)
    events_raw = payload.get("events") or []
    if not isinstance(events_raw, list):
        return _json_error("VALIDATION_ERROR", "events must be list", status=400)

    loan = await get_loan_account(db_dsn, user_id, loan_id)
    if not loan:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    db_events_rows = await list_loan_events(db_dsn, user_id, loan_id)
    base_summary, _, _ = await _compute_loan_cached(db_dsn, user_id, loan, db_events_rows)
    base_events = [ev for ev in (_loan_event_from_row(x) for x in db_events_rows) if ev is not None]

    try:
        for row in events_raw:
            if not isinstance(row, dict):
                continue
            tp = str(row.get("type") or "").upper()
            if tp == "EXTRA_PAYMENT":
                mode = str(row.get("mode") or "ONE_TIME").upper()
                if mode not in {"ONE_TIME", "MONTHLY", "WEEKLY", "BIWEEKLY", "YEARLY"}:
                    raise ValueError("mode invalid")
                base_events.append(
                    ExtraPaymentEvent(
                        date=_parse_date_ymd(str(row.get("date") or "")),
                        amount=_parse_decimal(row.get("amount"), field="amount", min_value=Decimal("0.01")),
                        mode=mode,  # type: ignore[arg-type]
                        strategy=str(row.get("strategy") or "REDUCE_TERM").upper(),  # type: ignore[arg-type]
                        end_date=_parse_date_ymd(str(row.get("end_date"))) if row.get("end_date") else None,
                    )
                )
            elif tp == "RATE_CHANGE":
                base_events.append(
                    RateChangeEvent(
                        date=_parse_date_ymd(str(row.get("date") or "")),
                        annual_rate=_parse_decimal(row.get("annual_rate"), field="annual_rate", min_value=Decimal("0"), max_value=Decimal("100")),
                    )
                )
            elif tp == "HOLIDAY":
                base_events.append(
                    HolidayEvent(
                        start_date=_parse_date_ymd(str(row.get("start_date") or "")),
                        end_date=_parse_date_ymd(str(row.get("end_date") or "")),
                        holiday_type=str(row.get("holiday_type") or "INTEREST_ONLY").upper(),  # type: ignore[arg-type]
                    )
                )
    except (ValueError, TypeError) as exc:
        return _json_error("VALIDATION_ERROR", str(exc), status=400)

    loan_input = _loan_row_to_input(loan)
    scenario_summary, scenario_schedule, version, _ = calculate_loan_schedule(loan_input, base_events)
    base_interest = Decimal(str(base_summary.get("total_interest") or "0"))
    scenario_interest = Decimal(str(scenario_summary.get("total_interest") or "0"))
    interest_saving = base_interest - scenario_interest
    return _json_ok(
        {
            "version": version,
            "base_summary": base_summary,
            "scenario_summary": scenario_summary,
            "interest_saving": format(interest_saving, "f"),
            "months_diff": int(base_summary.get("payments_count") or 0) - int(scenario_summary.get("payments_count") or 0),
            "schedule_preview": scenario_schedule[:24],
        }
    )


async def api_loan_tips(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        loan_id = int(request.match_info["loan_id"])
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    loan = await get_loan_account(db_dsn, user_id, loan_id)
    if not loan:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    events = await list_loan_events(db_dsn, user_id, loan_id)
    summary, schedule, _ = await _compute_loan_cached(db_dsn, user_id, loan, events)
    tips: list[dict[str, str]] = []
    if schedule:
        first = schedule[0]
        principal = Decimal(str(first.get("principal") or "0"))
        payment = Decimal(str(first.get("payment") or "0"))
        pct = int((Decimal("100") * (payment - principal) / payment)) if payment > 0 else 0
        tips.append(
            {
                "title": "Первые годы самые дорогие",
                "text": f"Сейчас доля процентов в платеже около {pct}%. Ранняя досрочка даёт максимальный эффект.",
            }
        )
    tips.append(
        {
            "title": "Сокращать срок обычно выгоднее",
            "text": "Если есть свободные деньги, стратегия REDUCE_TERM обычно экономит больше процентов, чем REDUCE_PAYMENT.",
        }
    )
    tips.append(
        {
            "title": "Держите платёж в день списания",
            "text": "Планируйте досрочку в дату планового платежа, чтобы быстрее уменьшать базу для следующих процентов.",
        }
    )
    return _json_ok({"summary": summary, "tips": tips})


def _render_schedule_csv(summary: dict[str, Any], schedule: list[dict[str, Any]]) -> str:
    rows = [
        "date,payment,interest,principal,balance,annual_rate,event",
    ]
    for r in schedule:
        rows.append(
            ",".join(
                [
                    str(r.get("date") or ""),
                    str(r.get("payment") or ""),
                    str(r.get("interest") or ""),
                    str(r.get("principal") or ""),
                    str(r.get("balance") or ""),
                    str(r.get("annual_rate") or ""),
                    str(r.get("event") or ""),
                ]
            )
        )
    rows.append("")
    rows.append(f"monthly_payment,{summary.get('monthly_payment')}")
    rows.append(f"total_interest,{summary.get('total_interest')}")
    rows.append(f"payoff_date,{summary.get('payoff_date')}")
    return "\n".join(rows)


def _render_simple_pdf(title: str, lines: list[str]) -> bytes:
    # Minimal valid PDF with one text page.
    safe_lines = [line.replace("(", "[").replace(")", "]") for line in lines]
    text_chunks = ["BT /F1 12 Tf 50 780 Td"]
    for idx, line in enumerate([title, *safe_lines]):
        if idx == 0:
            text_chunks.append(f"({line[:110]}) Tj")
        else:
            text_chunks.append(f"0 -16 Td ({line[:110]}) Tj")
    text_chunks.append("ET")
    stream = "\n".join(text_chunks).encode("latin-1", errors="ignore")
    objects = [
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n",
        b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n",
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj\n",
        b"4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n",
        b"5 0 obj << /Length " + str(len(stream)).encode("ascii") + b" >> stream\n" + stream + b"\nendstream endobj\n",
    ]
    pdf = b"%PDF-1.4\n"
    offsets = [0]
    for obj in objects:
        offsets.append(len(pdf))
        pdf += obj
    xref_pos = len(pdf)
    xref = [b"xref\n0 6\n0000000000 65535 f \n"]
    for off in offsets[1:]:
        xref.append(f"{off:010d} 00000 n \n".encode("ascii"))
    pdf += b"".join(xref)
    pdf += b"trailer << /Size 6 /Root 1 0 R >>\nstartxref\n"
    pdf += str(xref_pos).encode("ascii") + b"\n%%EOF\n"
    return pdf


async def api_loan_actual_payments(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        loan_id = int(request.match_info["loan_id"])
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)

    if request.method == "GET":
        items = await list_loan_actual_payments(db_dsn, user_id, loan_id)
        return _json_ok({"items": items})

    payload = await _read_json(request)
    try:
        payment_date = _parse_date_ymd(str(payload.get("payment_date") or payload.get("date") or ""))
        amount = _parse_decimal(payload.get("amount"), field="amount", min_value=Decimal("0.01"))
        principal_paid = _parse_decimal(payload.get("principal_paid") if payload.get("principal_paid") is not None else "0", field="principal_paid", min_value=Decimal("0"))
        interest_paid = _parse_decimal(payload.get("interest_paid") if payload.get("interest_paid") is not None else "0", field="interest_paid", min_value=Decimal("0"))
        if principal_paid + interest_paid > amount + Decimal("0.01"):
            raise ValueError("principal_paid + interest_paid must be <= amount")
        note = str(payload.get("note") or "").strip() or None
    except (ValueError, TypeError) as exc:
        return _json_error("VALIDATION_ERROR", str(exc), status=400)

    req_id = (request.headers.get("Idempotency-Key") or "").strip() or None
    payment_id, created = await create_loan_actual_payment(
        db_dsn,
        user_id=user_id,
        loan_id=loan_id,
        payment_date=payment_date,
        amount=amount,
        principal_paid=principal_paid,
        interest_paid=interest_paid,
        note=note,
        client_request_id=req_id,
    )
    if payment_id == 0:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    return _json_ok({"payment_id": payment_id, "created": created})


async def api_loan_refinance_preview(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        loan_id = int(request.match_info["loan_id"])
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    payload = await _read_json(request)
    loan = await get_loan_account(db_dsn, user_id, loan_id)
    if not loan:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    events = await list_loan_events(db_dsn, user_id, loan_id)
    base_summary, _, _ = await _compute_loan_cached(db_dsn, user_id, loan, events)
    try:
        new_rate = _parse_decimal(payload.get("new_annual_rate"), field="new_annual_rate", min_value=Decimal("0"), max_value=Decimal("100"))
        new_term = int(payload.get("new_term_months") or int(base_summary.get("payments_count") or 1))
        refinance_cost = _parse_decimal(payload.get("refinance_cost") if payload.get("refinance_cost") is not None else "0", field="refinance_cost", min_value=Decimal("0"))
    except (ValueError, TypeError) as exc:
        return _json_error("VALIDATION_ERROR", str(exc), status=400)

    loan_input = _loan_row_to_input(loan)
    loan_input.annual_rate = new_rate
    loan_input.term_months = max(1, new_term + month_diff(loan_input.first_payment_date, loan_input.calc_date or _today_msk()))
    scenario_summary, _, _, _ = calculate_loan_schedule(loan_input, [])
    base_cost = Decimal(str(base_summary.get("total_future_cost") or base_summary.get("total_paid") or "0"))
    new_cost = Decimal(str(scenario_summary.get("total_future_cost") or scenario_summary.get("total_paid") or "0")) + refinance_cost
    saving = base_cost - new_cost
    monthly_saving = Decimal(str(base_summary.get("monthly_payment") or "0")) - Decimal(str(scenario_summary.get("monthly_payment") or "0"))
    breakeven_months = int((refinance_cost / monthly_saving).to_integral_value(rounding=ROUND_HALF_EVEN)) if monthly_saving > 0 else None
    return _json_ok(
        {
            "base_summary": base_summary,
            "refinance_summary": scenario_summary,
            "refinance_cost": format(refinance_cost, "f"),
            "total_saving": format(saving, "f"),
            "monthly_saving": format(monthly_saving, "f"),
            "breakeven_months": breakeven_months,
        }
    )


async def api_loan_optimizer(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        loan_id = int(request.match_info["loan_id"])
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    payload = await _read_json(request)
    loan = await get_loan_account(db_dsn, user_id, loan_id)
    if not loan:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    loan_input = _loan_row_to_input(loan)
    goal_type = str(payload.get("goal_type") or "").upper()
    if goal_type not in {"CLOSE_BY_DATE", "PAYMENT_TARGET"}:
        return _json_error("VALIDATION_ERROR", "goal_type must be CLOSE_BY_DATE|PAYMENT_TARGET", status=400)

    low = Decimal("0")
    high = Decimal(str(loan_input.current_principal))
    best = high
    for _ in range(20):
        mid = (low + high) / Decimal("2")
        if goal_type == "CLOSE_BY_DATE":
            target_date = _parse_date_ymd(str(payload.get("target_date") or ""))
            events = [ExtraPaymentEvent(date=loan_input.calc_date or _today_msk(), amount=q_money(mid), mode="MONTHLY", strategy="REDUCE_TERM")]
            s, _, _, _ = calculate_loan_schedule(loan_input, events)
            ok = bool(s.get("payoff_date")) and str(s.get("payoff_date")) <= target_date.isoformat()
        else:
            target_payment = _parse_decimal(payload.get("target_payment"), field="target_payment", min_value=Decimal("0.01"))
            events = [ExtraPaymentEvent(date=loan_input.calc_date or _today_msk(), amount=q_money(mid), mode="ONE_TIME", strategy="REDUCE_PAYMENT")]
            s, _, _, _ = calculate_loan_schedule(loan_input, events)
            ok = Decimal(str(s.get("monthly_payment") or "0")) <= target_payment
        if ok:
            best = mid
            high = mid
        else:
            low = mid
    return _json_ok({"goal_type": goal_type, "recommended_extra": format(q_money(best), "f")})


async def api_loan_export(request: web.Request) -> web.StreamResponse:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        loan_id = int(request.match_info["loan_id"])
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    fmt = (request.query.get("format") or "csv").strip().lower()
    loan = await get_loan_account(db_dsn, user_id, loan_id)
    if not loan:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    events = await list_loan_events(db_dsn, user_id, loan_id)
    summary, schedule, _ = await _compute_loan_cached(db_dsn, user_id, loan, events)

    if fmt in {"excel", "csv"}:
        body = _render_schedule_csv(summary, schedule).encode("utf-8")
        resp = web.Response(body=body)
        resp.content_type = "text/csv"
        resp.headers["Content-Disposition"] = f'attachment; filename=\"loan_{loan_id}_schedule.csv\"'
        return resp
    if fmt == "pdf":
        lines = [
            f"Loan: {loan.get('name') or loan_id}",
            f"Current principal: {summary.get('remaining_balance')}",
            f"Monthly payment: {summary.get('monthly_payment')}",
            f"Payoff date: {summary.get('payoff_date')}",
        ]
        body = _render_simple_pdf("Loan report", lines)
        resp = web.Response(body=body)
        resp.content_type = "application/pdf"
        resp.headers["Content-Disposition"] = f'attachment; filename=\"loan_{loan_id}_report.pdf\"'
        return resp
    return _json_error("VALIDATION_ERROR", "format must be csv|excel|pdf", status=400)


async def api_loan_share_create(request: web.Request) -> web.Response:
    import secrets
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    try:
        loan_id = int(request.match_info["loan_id"])
    except (TypeError, ValueError):
        return _json_error("VALIDATION_ERROR", "invalid loan_id", status=400)
    payload = await _read_json(request)
    ttl_hours = int(payload.get("ttl_hours") or 72)
    ttl_hours = max(1, min(24 * 30, ttl_hours))
    token = secrets.token_urlsafe(16)
    ok = await create_loan_share_link(
        db_dsn,
        user_id=user_id,
        loan_id=loan_id,
        token=token,
        payload={"scenario": payload.get("scenario") or {}},
        expires_at=datetime.now(timezone.utc) + timedelta(hours=ttl_hours),
    )
    if not ok:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    return _json_ok({"token": token, "share_url": f"/api/miniapp/loans/share/{token}"})


async def api_loan_share_get(request: web.Request) -> web.Response:
    db_dsn = request.app[APP_DB_DSN]
    token = str(request.match_info.get("token") or "").strip()
    if not token:
        return _json_error("VALIDATION_ERROR", "token required", status=400)
    row = await get_loan_share_link(db_dsn, token)
    if not row:
        return _json_error("NOT_FOUND", "share link not found", status=404)
    if row.get("expires_at"):
        exp = datetime.fromisoformat(str(row["expires_at"]).replace("Z", "+00:00"))
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) > exp:
            return _json_error("NOT_FOUND", "share link expired", status=404)
    loan = await get_loan_account(db_dsn, int(row["user_id"]), int(row["loan_id"]))
    if not loan:
        return _json_error("NOT_FOUND", "loan not found", status=404)
    events = await list_loan_events(db_dsn, int(row["user_id"]), int(row["loan_id"]))
    summary, schedule, version = await _compute_loan_cached(db_dsn, int(row["user_id"]), loan, events)
    return _json_ok({"loan": loan, "summary": summary, "version": version, "preview": schedule[:24], "payload": row.get("payload") or {}})


async def api_loan_reminders(request: web.Request) -> web.Response:
    bot_token = request.app[APP_BOT_TOKEN]
    db_dsn = request.app[APP_DB_DSN]
    user_id = await _auth_user_id(request, bot_token)
    if request.method == "GET":
        return _json_ok(await get_loan_reminder_settings(db_dsn, user_id))
    payload = await _read_json(request)
    updated = await set_loan_reminder_settings(
        db_dsn,
        user_id=user_id,
        enabled=payload.get("enabled"),
        days_before=payload.get("days_before"),
    )
    return _json_ok(updated)


def attach_miniapp_routes(app: web.Application, db_dsn: str, bot_token: str) -> None:
    app[APP_DB_DSN] = db_dsn
    app[APP_BOT_TOKEN] = bot_token

    app.router.add_get("/miniapp", miniapp_index)
    app.router.add_get("/miniapp/{name}", miniapp_asset)

    app.router.add_get("/api/miniapp/me", api_me)
    app.router.add_get("/api/miniapp/portfolio", api_portfolio)
    app.router.add_get("/api/miniapp/search", api_search)
    app.router.add_post("/api/miniapp/trades", api_trade_post)
    app.router.add_post("/api/miniapp/asset_lookup", api_asset_lookup_post)
    app.router.add_get("/api/miniapp/top_movers", api_top_movers)
    app.router.add_get("/api/miniapp/usd_rub", api_usd_rub)
    app.router.add_post("/api/miniapp/price", api_price)
    app.router.add_post("/api/miniapp/portfolio/clear", api_portfolio_clear)
    app.router.add_get("/api/miniapp/settings/open_close", api_open_close_settings)
    app.router.add_post("/api/miniapp/settings/open_close", api_open_close_settings)
    app.router.add_get("/api/miniapp/articles", api_articles_list)
    app.router.add_get("/api/miniapp/articles/{text_code}", api_article_item)
    app.router.add_post("/api/miniapp/import/xml", api_import_xml)

    app.router.add_get("/api/miniapp/alerts", api_alerts_get)
    app.router.add_post("/api/miniapp/alerts", api_alerts_post)
    app.router.add_delete("/api/miniapp/alerts/{alert_id}", api_alerts_delete)

    app.router.add_get("/api/miniapp/mode", api_mode)
    app.router.add_post("/api/miniapp/mode", api_mode)
    app.router.add_get("/api/miniapp/budget/dashboard", api_budget_dashboard)
    app.router.add_get("/api/miniapp/budget/profile", api_budget_profile)
    app.router.add_post("/api/miniapp/budget/profile", api_budget_profile)
    app.router.add_get("/api/miniapp/budget/obligations", api_budget_obligations)
    app.router.add_post("/api/miniapp/budget/obligations", api_budget_obligations)
    app.router.add_get("/api/miniapp/budget/savings", api_budget_savings)
    app.router.add_post("/api/miniapp/budget/savings", api_budget_savings)
    app.router.add_post("/api/miniapp/budget/savings/{saving_id}", api_budget_saving_item)
    app.router.add_get("/api/miniapp/budget/incomes", api_budget_incomes)
    app.router.add_post("/api/miniapp/budget/incomes", api_budget_incomes)
    app.router.add_post("/api/miniapp/budget/incomes/{income_id}", api_budget_income_item)
    app.router.add_get("/api/miniapp/budget/expenses", api_budget_expenses)
    app.router.add_post("/api/miniapp/budget/expenses", api_budget_expenses)
    app.router.add_post("/api/miniapp/budget/expenses/{expense_id}", api_budget_expense_item)
    app.router.add_post("/api/miniapp/budget/reset", api_budget_reset)
    app.router.add_get("/api/miniapp/budget/settings/notifications", api_budget_notification_settings)
    app.router.add_post("/api/miniapp/budget/settings/notifications", api_budget_notification_settings)
    app.router.add_post("/api/miniapp/budget/funds/strategy", api_budget_fund_strategy)
    app.router.add_get("/api/miniapp/budget/funds", api_budget_funds)
    app.router.add_post("/api/miniapp/budget/funds", api_budget_funds)
    app.router.add_post("/api/miniapp/budget/funds/{fund_id}", api_budget_fund_item)
    app.router.add_post("/api/miniapp/budget/month-close", api_budget_month_close)
    app.router.add_get("/api/miniapp/budget/history", api_budget_history)

    app.router.add_get("/api/miniapp/loans", api_loans)
    app.router.add_post("/api/miniapp/loans", api_loans)
    app.router.add_get("/api/miniapp/loans/{loan_id}", api_loan_item)
    app.router.add_delete("/api/miniapp/loans/{loan_id}", api_loan_item)
    app.router.add_get("/api/miniapp/loans/{loan_id}/schedule", api_loan_schedule)
    app.router.add_post("/api/miniapp/loans/{loan_id}/events/extra-payment", api_loan_events_extra)
    app.router.add_post("/api/miniapp/loans/{loan_id}/events/rate-change", api_loan_events_rate)
    app.router.add_post("/api/miniapp/loans/{loan_id}/events/holiday", api_loan_events_holiday)
    app.router.add_get("/api/miniapp/loans/{loan_id}/actual-payments", api_loan_actual_payments)
    app.router.add_post("/api/miniapp/loans/{loan_id}/actual-payments", api_loan_actual_payments)
    app.router.add_post("/api/miniapp/loans/{loan_id}/scenarios/preview", api_loan_scenario_preview)
    app.router.add_post("/api/miniapp/loans/{loan_id}/refinance/preview", api_loan_refinance_preview)
    app.router.add_post("/api/miniapp/loans/{loan_id}/optimize", api_loan_optimizer)
    app.router.add_get("/api/miniapp/loans/{loan_id}/export", api_loan_export)
    app.router.add_post("/api/miniapp/loans/{loan_id}/share", api_loan_share_create)
    app.router.add_get("/api/miniapp/loans/{loan_id}/tips", api_loan_tips)
    app.router.add_get("/api/miniapp/loans/share/{token}", api_loan_share_get)
    app.router.add_get("/api/miniapp/loan-reminders/settings", api_loan_reminders)
    app.router.add_post("/api/miniapp/loan-reminders/settings", api_loan_reminders)
