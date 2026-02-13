import asyncio
import hashlib
import hmac
import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qsl

import aiohttp
from aiohttp import web

from broker_report_xml import parse_broker_report_xml
from db import (
    add_trade,
    clear_user_portfolio,
    create_price_target_alert,
    disable_price_target_alert,
    ensure_user_alert_settings,
    get_active_app_text,
    get_position_agg,
    get_user_alert_settings,
    get_user_positions,
    list_active_app_texts,
    list_active_price_target_alerts,
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

logger = logging.getLogger(__name__)

TRADE_SIDE_BUY = "buy"
TRADE_SIDE_SELL = "sell"
MAX_XML_UPLOAD_BYTES = 5 * 1024 * 1024
_api_cache: dict[str, tuple[float, dict]] = {}
_API_CACHE_TTL_SEC = int((os.getenv("MINIAPP_API_CACHE_TTL_SEC") or "900").strip() or "900")


def _cache_get(key: str) -> dict | None:
    row = _api_cache.get(key)
    if not row:
        return None
    ts, data = row
    if asyncio.get_running_loop().time() - ts > _API_CACHE_TTL_SEC:
        return None
    return data


def _cache_set(key: str, data: dict) -> None:
    _api_cache[key] = (asyncio.get_running_loop().time(), data)


class MiniAppAuthError(Exception):
    pass


def _parse_init_data(init_data: str) -> dict[str, str]:
    pairs = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=False))
    return {str(k): str(v) for k, v in pairs.items()}


def _calc_webapp_hash(bot_token: str, fields: dict[str, str]) -> str:
    data_check_arr = [f"{k}={v}" for k, v in sorted(fields.items()) if k != "hash"]
    data_check_string = "\n".join(data_check_arr)
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()


def parse_and_validate_init_data(bot_token: str, init_data: str) -> dict:
    fields = _parse_init_data(init_data)
    got_hash = fields.get("hash")
    if not got_hash:
        raise MiniAppAuthError("initData hash is missing")
    expected = _calc_webapp_hash(bot_token, fields)
    if not hmac.compare_digest(got_hash, expected):
        raise MiniAppAuthError("initData hash mismatch")
    user_raw = fields.get("user")
    if not user_raw:
        raise MiniAppAuthError("initData user is missing")
    try:
        return json.loads(user_raw)
    except json.JSONDecodeError as exc:
        raise MiniAppAuthError("initData user is invalid JSON") from exc


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
    async with aiohttp.ClientSession() as session:

        async def one(pos: dict) -> tuple[int, float | None]:
            iid = int(pos["id"])
            asset_type = pos.get("asset_type") or ASSET_TYPE_STOCK
            secid = str(pos.get("secid") or "")
            boardid = pos.get("boardid")
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


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _pick_stock_candidate_by_isin(cands: list[dict], isin: str) -> dict | None:
    isin_upper = isin.strip().upper()
    if not isin_upper:
        return cands[0] if cands else None
    for c in cands:
        if str(c.get("isin") or "").strip().upper() == isin_upper:
            return c
    return cands[0] if cands else None


async def miniapp_index(request: web.Request) -> web.Response:
    root = Path(__file__).resolve().parent / "miniapp"
    return web.FileResponse(root / "index.html")


async def miniapp_asset(request: web.Request) -> web.Response:
    root = Path(__file__).resolve().parent / "miniapp"
    name = (request.match_info.get("name") or "").strip()
    if "/" in name or ".." in name:
        raise web.HTTPNotFound()
    path = root / name
    if not path.exists() or not path.is_file():
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
    payload = await request.json()

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

    qty = _safe_float(payload.get("qty"), 0.0)
    price = _safe_float(payload.get("price"), 0.0)
    commission = max(0.0, _safe_float(payload.get("commission"), 0.0))
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
    except Exception:
        logger.exception("MiniApp trade price load failed secid=%s", secid)

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
    payload = await request.json()

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
            base = _safe_float(history[0][1], 0.0)
            end = _safe_float(current, 0.0) if current is not None else _safe_float(history[-1][1], 0.0)
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

    movers_sorted = sorted(movers, key=lambda x: _safe_float(x.get("pct"), -10**9), reverse=True)
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
    payload = await request.json()
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

    payload = await request.json()
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

    parsed_trades = parse_broker_report_xml(xml_bytes)
    if not parsed_trades:
        raise web.HTTPBadRequest(text="В выписке не найдены сделки")

    imported = 0
    duplicates = 0
    skipped = 0
    unresolved_isins: set[str] = set()
    stock_cache: dict[str, dict | None] = {}
    source_name = filename[:255]

    async with aiohttp.ClientSession() as session:
        for t in parsed_trades:
            secid = None
            boardid = ""
            shortname = (t.asset_name or "").strip() or None
            asset_type = t.asset_type

            if asset_type == ASSET_TYPE_METAL:
                secid = t.metal_secid
            else:
                cached = stock_cache.get(t.isin_reg)
                if cached is None and t.isin_reg not in stock_cache:
                    cands = await search_securities(session, t.isin_reg)
                    cached = _pick_stock_candidate_by_isin(cands, t.isin_reg)
                    stock_cache[t.isin_reg] = cached
                else:
                    cached = stock_cache.get(t.isin_reg)

                if cached:
                    secid = str(cached.get("secid") or "").strip() or None
                    boardid = str(cached.get("boardid") or "").strip()
                    if not shortname:
                        shortname = (cached.get("shortname") or cached.get("name") or "").strip() or None
                else:
                    unresolved_isins.add(t.isin_reg)

            if not secid:
                skipped += 1
                continue

            instrument_id = await upsert_instrument(
                db_dsn,
                secid=secid,
                isin=t.isin_reg,
                boardid=boardid,
                shortname=shortname,
                asset_type=asset_type,
            )
            was_inserted = await add_trade(
                db_dsn,
                user_id=user_id,
                instrument_id=instrument_id,
                trade_date=t.trade_date,
                qty=t.qty,
                price=t.price,
                commission=t.commission,
                external_trade_id=f"broker_xml:{t.trade_no}",
                import_source=source_name,
            )
            if was_inserted:
                imported += 1
            else:
                duplicates += 1

    return _json_ok(
        {
            "file": source_name,
            "rows": len(parsed_trades),
            "imported": imported,
            "duplicates": duplicates,
            "skipped": skipped,
            "unresolved_isins": sorted(unresolved_isins),
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
    payload = await request.json()

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
    except Exception as exc:
        raise web.HTTPBadRequest(text="target_price is required") from exc
    if target_price <= 0:
        raise web.HTTPBadRequest(text="target_price must be > 0")

    try:
        range_percent = float(payload.get("range_percent", 5.0))
    except Exception:
        range_percent = 5.0
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
    except Exception as exc:
        raise web.HTTPBadRequest(text="invalid alert_id") from exc
    ok = await disable_price_target_alert(db_dsn, user_id, alert_id)
    return _json_ok({"disabled": bool(ok)})


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
