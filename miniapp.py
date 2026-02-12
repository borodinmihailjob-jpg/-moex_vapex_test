import hashlib
import hmac
import json
import logging
import os
import asyncio
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl

import aiohttp
from aiohttp import web

from db import (
    create_price_target_alert,
    disable_price_target_alert,
    get_user_positions,
    list_active_price_target_alerts,
    upsert_instrument,
)
from moex_iss import (
    ASSET_TYPE_FIAT,
    ASSET_TYPE_METAL,
    ASSET_TYPE_STOCK,
    get_last_price_by_asset_type,
    get_last_price_fiat,
    search_fiat,
    search_metals,
    search_securities,
)

logger = logging.getLogger(__name__)


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
            except Exception:
                logger.exception("MiniApp price load failed secid=%s", secid)
                return iid, None

        rows = await asyncio.gather(*(one(p) for p in positions))
    for iid, px in rows:
        prices[iid] = px
    return prices


def _json_ok(payload: dict | list) -> web.Response:
    return web.json_response({"ok": True, "data": payload})


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
        pnl_pct = (value - total_cost_pos) / total_cost_pos * 100.0 if total_cost_pos > 1e-12 and last is not None else None
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
            }
        )

    for item in out_positions:
        if total_value > 0 and item["last"] is not None:
            item["share_pct"] = item["value"] / total_value * 100.0

    total_pnl_pct = (total_value - total_cost) / total_cost * 100.0 if total_cost > 1e-12 else 0.0
    return _json_ok(
        {
            "summary": {"total_value": total_value, "pnl_pct": total_pnl_pct, "as_of": datetime.utcnow().isoformat()},
            "positions": sorted(out_positions, key=lambda x: float(x["value"]), reverse=True),
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


def attach_miniapp_routes(app: web.Application, db_dsn: str, bot_token: str) -> None:
    app["db_dsn"] = db_dsn
    app["bot_token"] = bot_token
    app.router.add_get("/miniapp", miniapp_index)
    app.router.add_get("/miniapp/{name}", miniapp_asset)

    app.router.add_get("/api/miniapp/me", api_me)
    app.router.add_get("/api/miniapp/portfolio", api_portfolio)
    app.router.add_get("/api/miniapp/alerts", api_alerts_get)
    app.router.add_post("/api/miniapp/alerts", api_alerts_post)
    app.router.add_delete("/api/miniapp/alerts/{alert_id}", api_alerts_delete)
    app.router.add_get("/api/miniapp/search", api_search)
