import asyncio
import logging
import os
from datetime import date, datetime, timedelta
from json import JSONDecodeError
from pathlib import Path

import aiohttp
from aiohttp import web

from broker_import_service import import_broker_xml_trades
from common_utils import safe_float
from db import (
    add_trade,
    clear_user_portfolio,
    create_price_target_alert,
    disable_price_target_alert,
    ensure_user_alert_settings,
    get_budget_dashboard,
    get_budget_profile,
    get_active_app_text,
    get_user_last_mode,
    get_position_agg,
    get_user_alert_settings,
    get_user_positions,
    list_active_app_texts,
    list_active_price_target_alerts,
    list_budget_funds,
    list_budget_incomes,
    list_budget_obligations,
    list_budget_savings,
    set_user_last_mode,
    add_budget_income,
    add_budget_obligation,
    add_budget_saving,
    disable_budget_income,
    upsert_budget_profile,
    create_budget_fund,
    close_budget_month,
    reset_budget_data,
    update_budget_income,
    update_budget_fund,
    upsert_instrument,
    set_open_close_alert,
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
        user = parse_and_validate_init_data(bot_token, init_data)
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
    bot_token = request.app["bot_token"]
    user_id = await _auth_user_id(request, bot_token)
    return _json_ok({"user_id": user_id})


async def api_portfolio(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
                "as_of": datetime.utcnow().isoformat(),
            },
            "positions": sorted(out_positions, key=lambda x: float(x["value"]), reverse=True),
        }
    )


async def api_search(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
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
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
        trade_date = datetime.now().strftime("%d.%m.%Y")

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
    bot_token = request.app["bot_token"]
    _ = await _auth_user_id(request, bot_token)
    payload = await _read_json(request)

    secid = str(payload.get("secid") or "").strip().upper()
    if not secid:
        raise web.HTTPBadRequest(text="secid is required")
    boardid = str(payload.get("boardid") or "").strip() or None
    asset_type = str(payload.get("asset_type") or ASSET_TYPE_STOCK).strip().lower()
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL, ASSET_TYPE_FIAT}:
        raise web.HTTPBadRequest(text="invalid asset_type")

    now = datetime.now().date()
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
    bot_token = request.app["bot_token"]
    _ = await _auth_user_id(request, bot_token)

    day_str = (request.query.get("date") or "").strip()
    if day_str:
        try:
            day = date.fromisoformat(day_str)
        except ValueError as exc:
            raise web.HTTPBadRequest(text="invalid date, use YYYY-MM-DD") from exc
    else:
        day = datetime.now().date()

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
    bot_token = request.app["bot_token"]
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
    payload = {"secid": "USDRUB_TOM", "rate": rate, "as_of": datetime.utcnow().isoformat()}
    if rate is not None:
        _cache_set("usd_rub", payload)
    return _json_ok(payload)


async def api_price(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
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
    payload = {"secid": secid, "price": price, "as_of": datetime.utcnow().isoformat()}
    if price is not None:
        _cache_set(f"price:{asset_type}:{secid}:{boardid or ''}", payload)
    return _json_ok(payload)


async def api_portfolio_clear(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
    user_id = await _auth_user_id(request, bot_token)
    deleted = await clear_user_portfolio(db_dsn, user_id)
    return _json_ok({"deleted_trades": deleted})


async def api_open_close_settings(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
    _ = await _auth_user_id(request, bot_token)
    items = await list_active_app_texts(db_dsn)
    return _json_ok(items)


async def api_article_item(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
    _ = await _auth_user_id(request, bot_token)
    text_code = str(request.match_info.get("text_code") or "").strip()
    if not text_code:
        raise web.HTTPBadRequest(text="text_code is required")
    value = await get_active_app_text(db_dsn, text_code)
    if not value:
        raise web.HTTPNotFound(text="article not found")
    return _json_ok({"text_code": text_code, "value": value})


async def api_import_xml(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
    user_id = await _auth_user_id(request, bot_token)
    alerts = await list_active_price_target_alerts(db_dsn, user_id)
    return _json_ok(alerts)


async def api_alerts_post(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
    user_id = await _auth_user_id(request, bot_token)
    try:
        alert_id = int(request.match_info["alert_id"])
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="invalid alert_id") from exc
    ok = await disable_price_target_alert(db_dsn, user_id, alert_id)
    return _json_ok({"disabled": bool(ok)})


async def api_mode(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
    user_id = await _auth_user_id(request, bot_token)
    data = await get_budget_dashboard(db_dsn, user_id)
    return _json_ok(data)


async def api_budget_profile(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    return _json_ok(updated)


async def api_budget_obligations(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    return _json_ok({"id": item_id})


async def api_budget_savings(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    return _json_ok({"id": item_id})


async def api_budget_incomes(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    return _json_ok({"id": item_id})


async def api_budget_income_item(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    return _json_ok({"updated": ok})


async def api_budget_reset(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
    user_id = await _auth_user_id(request, bot_token)
    result = await reset_budget_data(db_dsn, user_id)
    return _json_ok(result)


def _calc_fund_strategy(
    target_amount: float,
    already_saved: float,
    target_month: str,
    income: float,
    obligations_total: float,
    expenses_base: float,
) -> dict:
    now = date.today()
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


async def api_budget_fund_strategy(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
    fund_id = await create_budget_fund(
        db_dsn,
        user_id=user_id,
        title=title,
        target_amount=target_amount,
        already_saved=already_saved,
        target_month=target_month,
        priority=priority,
    )
    return _json_ok({"id": fund_id})


async def api_budget_fund_item(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
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
        return _json_ok({"updated": ok})

    if action == "edit":
        kwargs = {}
        if payload.get("target_amount") is not None:
            try:
                kwargs["target_amount"] = _parse_money_text(payload.get("target_amount"))
            except ValueError as exc:
                raise web.HTTPBadRequest(text="invalid target_amount") from exc
        if payload.get("target_month") is not None:
            try:
                kwargs["target_month"] = _parse_month_key(str(payload.get("target_month")))
            except ValueError as exc:
                raise web.HTTPBadRequest(text=str(exc)) from exc
        ok = await update_budget_fund(db_dsn, user_id=user_id, fund_id=fund_id, **kwargs)
        return _json_ok({"updated": ok})

    if action == "autopilot":
        enabled = bool(payload.get("enabled"))
        ok = await update_budget_fund(db_dsn, user_id=user_id, fund_id=fund_id, autopilot_enabled=enabled)
        return _json_ok({"updated": ok, "autopilot_enabled": enabled})

    status = "paused" if action == "pause" else "deleted"
    ok = await update_budget_fund(db_dsn, user_id=user_id, fund_id=fund_id, status=status)
    return _json_ok({"updated": ok, "status": status})


async def api_budget_month_close(request: web.Request) -> web.Response:
    bot_token = request.app["bot_token"]
    db_dsn = request.app["db_dsn"]
    user_id = await _auth_user_id(request, bot_token)
    payload = await _read_json(request)

    month_raw = payload.get("month_key") or (date.today().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
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
    return _json_ok(result)


def attach_miniapp_routes(app: web.Application, db_dsn: str, bot_token: str) -> None:
    app["db_dsn"] = db_dsn
    app["bot_token"] = bot_token

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
    app.router.add_get("/api/miniapp/budget/incomes", api_budget_incomes)
    app.router.add_post("/api/miniapp/budget/incomes", api_budget_incomes)
    app.router.add_post("/api/miniapp/budget/incomes/{income_id}", api_budget_income_item)
    app.router.add_post("/api/miniapp/budget/reset", api_budget_reset)
    app.router.add_post("/api/miniapp/budget/funds/strategy", api_budget_fund_strategy)
    app.router.add_get("/api/miniapp/budget/funds", api_budget_funds)
    app.router.add_post("/api/miniapp/budget/funds", api_budget_funds)
    app.router.add_post("/api/miniapp/budget/funds/{fund_id}", api_budget_fund_item)
    app.router.add_post("/api/miniapp/budget/month-close", api_budget_month_close)
