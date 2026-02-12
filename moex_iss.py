import asyncio
import aiohttp
import logging
import os
from dataclasses import dataclass
from contextvars import ContextVar
from datetime import date, datetime
from zoneinfo import ZoneInfo

ISS_BASE = "https://iss.moex.com/iss"
ALGOPACK_BASE = "https://apim.moex.com/iss"
logger = logging.getLogger(__name__)
ISS_RETRIES = 3
ISS_RETRY_DELAY_SEC = 0.6
ISS_TIMEOUT = aiohttp.ClientTimeout(total=12, connect=4, sock_connect=4, sock_read=8)

ASSET_TYPE_STOCK = "stock"
ASSET_TYPE_METAL = "metal"
DELAYED_WARNING_TEXT = "(данные с задержкой в 15 минут)"
MSK_TZ = ZoneInfo("Europe/Moscow")


@dataclass
class DataSourceFlags:
    delayed_data_used: bool = False


_data_source_flags_var: ContextVar[DataSourceFlags | None] = ContextVar("moex_data_source_flags", default=None)


def reset_data_source_flags() -> None:
    _data_source_flags_var.set(DataSourceFlags())


def delayed_data_used() -> bool:
    flags = _data_source_flags_var.get()
    return bool(flags.delayed_data_used) if flags is not None else False


def mark_delayed_data_used() -> None:
    flags = _data_source_flags_var.get()
    if flags is None:
        flags = DataSourceFlags()
        _data_source_flags_var.set(flags)
    flags.delayed_data_used = True


def _get_algopack_api_key() -> str:
    for key in (
        "ALGOPACK_API_KEY",
        "MOEX_ALGOPACK_API_KEY",
        "MOEXALGOPACK_API_KEY",
        "MOEX_API_KEY",
    ):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return ""


async def _request_json(
    session: aiohttp.ClientSession,
    base: str,
    path: str,
    params: dict | None = None,
    headers: dict | None = None,
    source_name: str = "iss",
) -> dict:
    url = f"{base}{path}"
    last_exc: Exception | None = None
    for attempt in range(1, ISS_RETRIES + 1):
        try:
            async with session.get(url, params=params, timeout=ISS_TIMEOUT, headers=headers) as resp:
                if resp.status in {429, 500, 502, 503, 504}:
                    if attempt < ISS_RETRIES:
                        logger.warning(
                            "%s temporary HTTP status=%s url=%s attempt=%s/%s",
                            source_name.upper(),
                            resp.status,
                            url,
                            attempt,
                            ISS_RETRIES,
                        )
                        await asyncio.sleep(ISS_RETRY_DELAY_SEC * attempt)
                        continue
                resp.raise_for_status()
                return await resp.json()
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_exc = exc
            if attempt < ISS_RETRIES:
                logger.warning(
                    "%s transient error url=%s attempt=%s/%s error=%s",
                    source_name.upper(),
                    url,
                    attempt,
                    ISS_RETRIES,
                    exc.__class__.__name__,
                )
                await asyncio.sleep(ISS_RETRY_DELAY_SEC * attempt)
                continue
            logger.error(
                "%s request failed after retries: %s params=%s error=%s",
                source_name.upper(),
                url,
                params,
                exc.__class__.__name__,
            )
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{source_name.upper()} request failed unexpectedly: {url}")


async def iss_get_json(session: aiohttp.ClientSession, path: str, params: dict | None = None) -> dict:
    return await _request_json(session, ISS_BASE, path, params=params, source_name="iss")


async def algopack_get_json(session: aiohttp.ClientSession, path: str, params: dict | None = None) -> dict:
    token = _get_algopack_api_key()
    if not token:
        raise RuntimeError("ALGOPACK API key is not configured")
    return await _request_json(
        session,
        ALGOPACK_BASE,
        path,
        params=params,
        headers={"Authorization": f"Bearer {token}"},
        source_name="algopack",
    )


async def get_json_with_fallback_source(
    session: aiohttp.ClientSession,
    path: str,
    params: dict | None = None,
) -> tuple[dict, bool]:
    token = _get_algopack_api_key()
    if token:
        try:
            return await algopack_get_json(session, path, params=params), False
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
            logger.warning(
                "ALGOPACK failed, fallback to ISS path=%s error=%s",
                path,
                exc.__class__.__name__,
            )
    else:
        logger.debug("ALGOPACK API key is missing, using ISS fallback path=%s", path)

    mark_delayed_data_used()
    return await iss_get_json(session, path, params=params), True


async def get_json_with_fallback(session: aiohttp.ClientSession, path: str, params: dict | None = None) -> dict:
    data, _ = await get_json_with_fallback_source(session, path, params=params)
    return data

async def search_securities(session: aiohttp.ClientSession, query: str) -> list[dict]:
    """
    Поиск тикера/ISIN/названия через ISS.
    Делаем общий поиск и приоритизируем торгуемые акции (group=stock_shares, is_traded=1).
    """
    q = query.strip()
    if not q:
        return []

    params = {
        "q": q,
        "iss.meta": "off",
        "lang": "ru",
        "limit": 50,
    }
    data, delayed = await get_json_with_fallback_source(session, "/securities.json", params=params)
    all_results = _parse_securities_rows(data)
    if not all_results and not delayed:
        logger.warning("ALGOPACK search returned empty set for query=%r; retry via ISS", q)
        mark_delayed_data_used()
        data = await iss_get_json(session, "/securities.json", params=params)
        all_results = _parse_securities_rows(data)

    # Приоритет: акции, торгуемые сейчас.
    traded_shares = [x for x in all_results if x.get("group") == "stock_shares" and x.get("is_traded") == 1]
    if traded_shares:
        ranked = _rank_by_query(traded_shares, q)
        logger.info("ISS search query=%r traded_shares=%s total=%s", q, len(traded_shares), len(all_results))
        return ranked

    logger.info("ISS search query=%r fallback_total=%s", q, len(all_results))
    return _rank_by_query([x for x in all_results if x.get("group") == "stock_shares"], q)

async def search_metals(session: aiohttp.ClientSession, query: str) -> list[dict]:
    """
    Поиск биржевых металлов (например GLDRUB_TOM, SLVRUB_TOM) через ISS.
    """
    q = query.strip()
    if not q:
        return []

    params = {
        "q": q,
        "iss.meta": "off",
        "lang": "ru",
        "limit": 50,
    }
    data, delayed = await get_json_with_fallback_source(session, "/securities.json", params=params)
    all_results = _parse_securities_rows(data)
    if not all_results and not delayed:
        logger.warning("ALGOPACK metal search returned empty set for query=%r; retry via ISS", q)
        mark_delayed_data_used()
        data = await iss_get_json(session, "/securities.json", params=params)
        all_results = _parse_securities_rows(data)
    metals = [x for x in all_results if x.get("group") == "currency_metal" and x.get("is_traded") == 1]
    logger.info("ISS metal search query=%r results=%s total=%s", q, len(metals), len(all_results))
    return metals

async def get_last_price_stock_shares(session: aiohttp.ClientSession, secid: str, boardid: str | None = None) -> float | None:
    """
    Для MVP: берём marketdata.LAST для рынка shares (акции).
    """
    if boardid:
        path = f"/engines/stock/markets/shares/boards/{boardid}/securities/{secid}.json"
    else:
        path = f"/engines/stock/markets/shares/securities/{secid}.json"

    def parse_last(data: dict) -> float | None:
        md = data.get("marketdata", {})
        cols = md.get("columns", [])
        rows = md.get("data", [])
        if not rows:
            return None
        idx = {c: i for i, c in enumerate(cols)}
        if "LAST" not in idx:
            return None
        last = rows[0][idx["LAST"]]
        if last is None:
            return None
        return float(last)

    data, delayed = await get_json_with_fallback_source(session, path, params={"iss.meta": "off"})
    price = parse_last(data)
    if price is None and not delayed:
        logger.warning("ALGOPACK returned no LAST for secid=%s boardid=%s; retry via ISS", secid, boardid)
        mark_delayed_data_used()
        data = await iss_get_json(session, path, params={"iss.meta": "off"})
        price = parse_last(data)
    if price is None:
        logger.warning("No LAST marketdata for secid=%s boardid=%s", secid, boardid)
        return None
    logger.debug("Last price secid=%s boardid=%s last=%s", secid, boardid, price)
    return price

async def get_last_price_metal(session: aiohttp.ClientSession, secid: str, boardid: str | None = None) -> float | None:
    """
    Для металлов (currency_metal) берём marketdata.LAST на engine=currency, market=selt.
    """
    if boardid:
        path = f"/engines/currency/markets/selt/boards/{boardid}/securities/{secid}.json"
    else:
        path = f"/engines/currency/markets/selt/securities/{secid}.json"

    def parse_last(data: dict) -> float | None:
        md = data.get("marketdata", {})
        cols = md.get("columns", [])
        rows = md.get("data", [])
        if not rows:
            return None
        idx = {c: i for i, c in enumerate(cols)}
        if "LAST" not in idx:
            return None
        last = rows[0][idx["LAST"]]
        if last is None:
            return None
        return float(last)

    data, delayed = await get_json_with_fallback_source(session, path, params={"iss.meta": "off"})
    price = parse_last(data)
    if price is None and not delayed:
        logger.warning("ALGOPACK returned no metal LAST for secid=%s boardid=%s; retry via ISS", secid, boardid)
        mark_delayed_data_used()
        data = await iss_get_json(session, path, params={"iss.meta": "off"})
        price = parse_last(data)
    if price is None:
        logger.warning("No metal LAST marketdata for secid=%s boardid=%s", secid, boardid)
        return None
    logger.debug("Last metal price secid=%s boardid=%s last=%s", secid, boardid, price)
    return price


async def get_usd_rub_rate(session: aiohttp.ClientSession, secid: str = "USDRUB_TOM", boardid: str = "CETS") -> float | None:
    """
    Курс USD/RUB с валютного рынка MOEX (обычно USDRUB_TOM, board CETS).
    """
    path = f"/engines/currency/markets/selt/boards/{boardid}/securities/{secid}.json"

    def parse_last(data: dict) -> float | None:
        md = data.get("marketdata", {})
        cols = md.get("columns", [])
        rows = md.get("data", [])
        if not rows:
            return None
        idx = {c: i for i, c in enumerate(cols)}
        if "LAST" not in idx:
            return None
        last = rows[0][idx["LAST"]]
        if last is None:
            return None
        return float(last)

    data, delayed = await get_json_with_fallback_source(session, path, params={"iss.meta": "off"})
    price = parse_last(data)
    if price is None and not delayed:
        logger.warning("ALGOPACK returned no USD/RUB LAST for secid=%s boardid=%s; retry via ISS", secid, boardid)
        mark_delayed_data_used()
        data = await iss_get_json(session, path, params={"iss.meta": "off"})
        price = parse_last(data)
    if price is None:
        logger.warning("No USD/RUB LAST marketdata for secid=%s boardid=%s", secid, boardid)
        return None
    logger.debug("USD/RUB rate secid=%s boardid=%s last=%s", secid, boardid, price)
    return price


async def get_last_price_by_asset_type(
    session: aiohttp.ClientSession,
    secid: str,
    boardid: str | None,
    asset_type: str,
) -> float | None:
    if asset_type == ASSET_TYPE_METAL:
        return await get_last_price_metal(session, secid, boardid)
    return await get_last_price_stock_shares(session, secid, boardid)


async def get_stock_day_movers(session: aiohttp.ClientSession, boardid: str = "TQBR") -> list[dict]:
    """
    Возвращает список акций с изменением за текущую торговую сессию:
    OPEN (цена открытия) -> LAST (последняя цена).
    """
    path = f"/engines/stock/markets/shares/boards/{boardid}/securities.json"
    out: list[dict] = []
    params = {
        "iss.meta": "off",
        "securities.columns": "SECID,SHORTNAME",
        "marketdata.columns": "SECID,OPEN,LAST,VOLTODAY,VALTODAY",
    }
    data, delayed = await get_json_with_fallback_source(session, path, params=params)
    sec = data.get("securities", {})
    md = data.get("marketdata", {})
    sec_cols = sec.get("columns", [])
    sec_rows = sec.get("data", [])
    md_cols = md.get("columns", [])
    md_rows = md.get("data", [])
    if not md_rows and not delayed:
        logger.warning("ALGOPACK movers response is empty for board=%s; retry via ISS", boardid)
        mark_delayed_data_used()
        data = await iss_get_json(session, path, params=params)
        sec = data.get("securities", {})
        md = data.get("marketdata", {})
        sec_cols = sec.get("columns", [])
        sec_rows = sec.get("data", [])
        md_cols = md.get("columns", [])
        md_rows = md.get("data", [])
    if not md_rows:
        return out

    sec_idx = {str(c).upper(): i for i, c in enumerate(sec_cols)}
    md_idx = {str(c).upper(): i for i, c in enumerate(md_cols)}
    secid_i = sec_idx.get("SECID")
    shortname_i = sec_idx.get("SHORTNAME")
    md_secid_i = md_idx.get("SECID")
    open_i = md_idx.get("OPEN")
    last_i = md_idx.get("LAST")
    vol_i = md_idx.get("VOLTODAY")
    val_i = md_idx.get("VALTODAY")
    if md_secid_i is None or open_i is None or last_i is None:
        return out

    names: dict[str, str] = {}
    for row in sec_rows:
        if secid_i is None or secid_i >= len(row):
            continue
        secid = str(row[secid_i] or "").strip()
        if not secid:
            continue
        shortname = ""
        if shortname_i is not None and shortname_i < len(row):
            shortname = str(row[shortname_i] or "").strip()
        names[secid] = shortname

    for row in md_rows:
        secid = str(row[md_secid_i] or "").strip()
        if not secid:
            continue
        open_px = row[open_i] if open_i < len(row) else None
        last_px = row[last_i] if last_i < len(row) else None
        if open_px is None or last_px is None:
            continue
        try:
            open_f = float(open_px)
            last_f = float(last_px)
        except (TypeError, ValueError):
            continue
        vol_today = None
        if vol_i is not None and vol_i < len(row):
            raw_vol = row[vol_i]
            if raw_vol is not None:
                try:
                    vol_today = float(raw_vol)
                except (TypeError, ValueError):
                    vol_today = None
        val_today = None
        if val_i is not None and val_i < len(row):
            raw_val = row[val_i]
            if raw_val is not None:
                try:
                    val_today = float(raw_val)
                except (TypeError, ValueError):
                    val_today = None
        if open_f <= 0:
            continue
        pct = (last_f - open_f) / open_f * 100.0
        out.append(
            {
                "secid": secid,
                "shortname": names.get(secid) or secid,
                "open": open_f,
                "last": last_f,
                "pct": pct,
                "vol_today": vol_today,
                "val_today": val_today,
            }
        )

    return out


async def get_stock_movers_by_date(
    session: aiohttp.ClientSession,
    trade_date: date,
    boardid: str = "TQBR",
) -> list[dict]:
    """
    Возвращает список акций с дневным изменением и объемом торгов за выбранную дату.
    Для текущего дня: OPEN -> LAST и VOLTODAY из marketdata.
    Для прошлых дней: OPEN -> CLOSE и VOLUME из history.
    """
    if trade_date >= datetime.now(MSK_TZ).date():
        return await get_stock_day_movers(session, boardid=boardid)

    path = f"/history/engines/stock/markets/shares/boards/{boardid}/securities.json"
    out: list[dict] = []
    start = 0
    use_iss_only = False

    while True:
        params = {
            "iss.meta": "off",
            "from": trade_date.isoformat(),
            "till": trade_date.isoformat(),
            "start": start,
            "limit": 100,
            "securities.columns": "SECID,SHORTNAME",
            "history.columns": "SECID,OPEN,CLOSE,LEGALCLOSEPRICE,WAPRICE,VOLUME,VALUE",
        }
        if use_iss_only:
            data = await iss_get_json(session, path, params=params)
            delayed = True
        else:
            data, delayed = await get_json_with_fallback_source(session, path, params=params)
            if delayed:
                use_iss_only = True

        sec = data.get("securities", {})
        hist = data.get("history", {})
        sec_cols = sec.get("columns", [])
        sec_rows = sec.get("data", [])
        h_cols = hist.get("columns", [])
        h_rows = hist.get("data", [])

        if not h_rows and start == 0 and not delayed:
            logger.warning("ALGOPACK historical movers are empty for date=%s; retry via ISS", trade_date.isoformat())
            mark_delayed_data_used()
            data = await iss_get_json(session, path, params=params)
            use_iss_only = True
            sec = data.get("securities", {})
            hist = data.get("history", {})
            sec_cols = sec.get("columns", [])
            sec_rows = sec.get("data", [])
            h_cols = hist.get("columns", [])
            h_rows = hist.get("data", [])

        if not h_rows:
            break

        sec_idx = {str(c).upper(): i for i, c in enumerate(sec_cols)}
        h_idx = {str(c).upper(): i for i, c in enumerate(h_cols)}
        secid_i = sec_idx.get("SECID")
        shortname_i = sec_idx.get("SHORTNAME")
        h_secid_i = h_idx.get("SECID")
        open_i = h_idx.get("OPEN")
        close_i = h_idx.get("CLOSE")
        legal_i = h_idx.get("LEGALCLOSEPRICE")
        wap_i = h_idx.get("WAPRICE")
        vol_i = h_idx.get("VOLUME")
        val_i = h_idx.get("VALUE")
        if h_secid_i is None or open_i is None:
            break

        names: dict[str, str] = {}
        for row in sec_rows:
            if secid_i is None or secid_i >= len(row):
                continue
            secid = str(row[secid_i] or "").strip()
            if not secid:
                continue
            shortname = ""
            if shortname_i is not None and shortname_i < len(row):
                shortname = str(row[shortname_i] or "").strip()
            names[secid] = shortname

        for row in h_rows:
            secid = str(row[h_secid_i] or "").strip()
            if not secid:
                continue
            open_px = row[open_i] if open_i < len(row) else None
            close_px = row[close_i] if close_i is not None and close_i < len(row) else None
            if close_px is None and legal_i is not None and legal_i < len(row):
                close_px = row[legal_i]
            if close_px is None and wap_i is not None and wap_i < len(row):
                close_px = row[wap_i]
            if open_px is None or close_px is None:
                continue
            try:
                open_f = float(open_px)
                close_f = float(close_px)
            except (TypeError, ValueError):
                continue
            if open_f <= 0:
                continue
            vol_day = None
            if vol_i is not None and vol_i < len(row):
                raw = row[vol_i]
                if raw is not None:
                    try:
                        vol_day = float(raw)
                    except (TypeError, ValueError):
                        vol_day = None
            val_day = None
            if val_i is not None and val_i < len(row):
                raw = row[val_i]
                if raw is not None:
                    try:
                        val_day = float(raw)
                    except (TypeError, ValueError):
                        val_day = None
            pct = (close_f - open_f) / open_f * 100.0
            out.append(
                {
                    "secid": secid,
                    "shortname": names.get(secid) or secid,
                    "open": open_f,
                    "last": close_f,
                    "pct": pct,
                    "vol_today": vol_day,
                    "val_today": val_day,
                }
            )

        if len(h_rows) < 100:
            break
        start += len(h_rows)

    missing = [x for x in out if not str(x.get("shortname") or "").strip() or str(x.get("shortname")).strip() == str(x.get("secid")).strip()]
    if missing:
        try:
            names_map = await _load_board_shortnames(session, boardid)
            for row in out:
                secid = str(row.get("secid") or "").strip()
                if not secid:
                    continue
                shortname = str(row.get("shortname") or "").strip()
                if not shortname or shortname == secid:
                    row["shortname"] = names_map.get(secid) or shortname or secid
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
            logger.warning("Failed enriching shortnames for board=%s", boardid)

    return out


async def get_stock_snapshot(
    session: aiohttp.ClientSession,
    secid: str,
    boardid: str = "TQBR",
) -> dict | None:
    path = f"/engines/stock/markets/shares/boards/{boardid}/securities/{secid}.json"
    params = {
        "iss.meta": "off",
        "securities.columns": "SECID,SHORTNAME,NAME",
        "marketdata.columns": "SECID,OPEN,LAST,BID,OFFER,VOLTODAY,VALTODAY",
    }
    data, delayed = await get_json_with_fallback_source(session, path, params=params)
    sec = data.get("securities", {})
    md = data.get("marketdata", {})
    sec_cols = sec.get("columns", [])
    sec_rows = sec.get("data", [])
    md_cols = md.get("columns", [])
    md_rows = md.get("data", [])
    if not md_rows and not delayed:
        mark_delayed_data_used()
        data = await iss_get_json(session, path, params=params)
        sec = data.get("securities", {})
        md = data.get("marketdata", {})
        sec_cols = sec.get("columns", [])
        sec_rows = sec.get("data", [])
        md_cols = md.get("columns", [])
        md_rows = md.get("data", [])
    if not md_rows:
        return None

    md_idx = {str(c).upper(): i for i, c in enumerate(md_cols)}
    sec_idx = {str(c).upper(): i for i, c in enumerate(sec_cols)}
    row = md_rows[0]
    sec_row = sec_rows[0] if sec_rows else []

    def pick_float(i: int | None) -> float | None:
        if i is None or i >= len(row):
            return None
        v = row[i]
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    shortname = None
    si = sec_idx.get("SHORTNAME")
    if si is not None and si < len(sec_row):
        shortname = str(sec_row[si] or "").strip() or None
    if not shortname:
        ni = sec_idx.get("NAME")
        if ni is not None and ni < len(sec_row):
            shortname = str(sec_row[ni] or "").strip() or None

    return {
        "secid": secid,
        "boardid": boardid,
        "shortname": shortname or secid,
        "open": pick_float(md_idx.get("OPEN")),
        "last": pick_float(md_idx.get("LAST")),
        "bid": pick_float(md_idx.get("BID")),
        "offer": pick_float(md_idx.get("OFFER")),
        "vol_today": pick_float(md_idx.get("VOLTODAY")),
        "val_today": pick_float(md_idx.get("VALTODAY")),
    }


async def get_stock_avg_daily_volume(
    session: aiohttp.ClientSession,
    secid: str,
    boardid: str = "TQBR",
    days: int = 20,
) -> float | None:
    path = f"/history/engines/stock/markets/shares/boards/{boardid}/securities/{secid}.json"
    till = date.today()
    from_dt = till.fromordinal(till.toordinal() - max(5, int(days * 2)))
    params = {
        "iss.meta": "off",
        "from": from_dt.isoformat(),
        "till": till.isoformat(),
        "history.columns": "TRADEDATE,VOLUME",
    }
    data, delayed = await get_json_with_fallback_source(session, path, params=params)
    hist = data.get("history", {})
    cols = hist.get("columns", [])
    rows = hist.get("data", [])
    if not rows and not delayed:
        mark_delayed_data_used()
        data = await iss_get_json(session, path, params=params)
        hist = data.get("history", {})
        cols = hist.get("columns", [])
        rows = hist.get("data", [])
    if not rows:
        return None
    idx = {str(c).upper(): i for i, c in enumerate(cols)}
    vol_i = idx.get("VOLUME")
    if vol_i is None:
        return None
    values: list[float] = []
    for r in rows:
        if vol_i >= len(r):
            continue
        v = r[vol_i]
        if v is None:
            continue
        try:
            f = float(v)
            if f >= 0:
                values.append(f)
        except (TypeError, ValueError):
            continue
    if not values:
        return None
    take = values[-days:] if len(values) > days else values
    return sum(take) / len(take)


async def _load_board_shortnames(session: aiohttp.ClientSession, boardid: str) -> dict[str, str]:
    path = f"/engines/stock/markets/shares/boards/{boardid}/securities.json"
    params = {
        "iss.meta": "off",
        "securities.columns": "SECID,SHORTNAME,NAME",
    }
    data = await get_json_with_fallback(session, path, params=params)
    sec = data.get("securities", {})
    cols = sec.get("columns", [])
    rows = sec.get("data", [])
    if not rows:
        return {}
    idx = {str(c).upper(): i for i, c in enumerate(cols)}
    secid_i = idx.get("SECID")
    short_i = idx.get("SHORTNAME")
    name_i = idx.get("NAME")
    if secid_i is None:
        return {}
    out: dict[str, str] = {}
    for row in rows:
        if secid_i >= len(row):
            continue
        secid = str(row[secid_i] or "").strip()
        if not secid:
            continue
        shortname = ""
        if short_i is not None and short_i < len(row):
            shortname = str(row[short_i] or "").strip()
        if not shortname and name_i is not None and name_i < len(row):
            shortname = str(row[name_i] or "").strip()
        if shortname:
            out[secid] = shortname
    return out

def _history_path_by_asset_type(secid: str, boardid: str | None, asset_type: str) -> str:
    if asset_type == ASSET_TYPE_METAL:
        if boardid:
            return f"/history/engines/currency/markets/selt/boards/{boardid}/securities/{secid}.json"
        return f"/history/engines/currency/markets/selt/securities/{secid}.json"
    if boardid:
        return f"/history/engines/stock/markets/shares/boards/{boardid}/securities/{secid}.json"
    return f"/history/engines/stock/markets/shares/securities/{secid}.json"

async def get_history_prices_by_asset_type(
    session: aiohttp.ClientSession,
    secid: str,
    boardid: str | None,
    asset_type: str,
    from_date: date,
    till_date: date,
) -> list[tuple[date, float]]:
    """
    Возвращает список (trade_date, close_price) за период [from_date, till_date].
    Для цены берется приоритет: CLOSE -> LEGALCLOSEPRICE -> WAPRICE.
    """
    path = _history_path_by_asset_type(secid, boardid, asset_type)
    start = 0
    out: list[tuple[date, float]] = []
    use_iss_only = False

    while True:
        params = {
            "iss.meta": "off",
            "from": from_date.isoformat(),
            "till": till_date.isoformat(),
            "start": start,
            "history.columns": "TRADEDATE,CLOSE,LEGALCLOSEPRICE,WAPRICE",
        }
        if use_iss_only:
            data = await iss_get_json(session, path, params=params)
            delayed = True
        else:
            data, delayed = await get_json_with_fallback_source(session, path, params=params)
            if delayed:
                use_iss_only = True
        hist = data.get("history", {})
        cols = hist.get("columns", [])
        rows = hist.get("data", [])
        if not rows and start == 0 and not delayed:
            logger.warning("ALGOPACK history response is empty for secid=%s; retry via ISS", secid)
            mark_delayed_data_used()
            data = await iss_get_json(session, path, params=params)
            use_iss_only = True
            hist = data.get("history", {})
            cols = hist.get("columns", [])
            rows = hist.get("data", [])
        if not rows:
            break

        idx = {str(c).upper(): i for i, c in enumerate(cols)}
        dt_i = idx.get("TRADEDATE")
        close_i = idx.get("CLOSE")
        legal_i = idx.get("LEGALCLOSEPRICE")
        wap_i = idx.get("WAPRICE")
        if dt_i is None:
            break

        for row in rows:
            dt_raw = row[dt_i]
            if not dt_raw:
                continue
            px = None
            if close_i is not None and close_i < len(row):
                px = row[close_i]
            if px is None and legal_i is not None and legal_i < len(row):
                px = row[legal_i]
            if px is None and wap_i is not None and wap_i < len(row):
                px = row[wap_i]
            if px is None:
                continue
            try:
                trade_day = date.fromisoformat(str(dt_raw))
                out.append((trade_day, float(px)))
            except (TypeError, ValueError):
                continue

        if len(rows) < 100:
            break
        start += len(rows)

    out.sort(key=lambda x: x[0])
    return out


async def get_moex_index_return_percent(
    session: aiohttp.ClientSession,
    from_date: date,
    till_date: date,
    secid: str = "IMOEX",
) -> float | None:
    """
    Доходность индекса MOEX за период [from_date, till_date] в процентах.
    """
    path = f"/history/engines/stock/markets/index/securities/{secid}.json"
    start = 0
    points: list[tuple[date, float]] = []
    use_iss_only = False

    while True:
        params = {
            "iss.meta": "off",
            "from": from_date.isoformat(),
            "till": till_date.isoformat(),
            "start": start,
        }
        if use_iss_only:
            data = await iss_get_json(session, path, params=params)
            delayed = True
        else:
            data, delayed = await get_json_with_fallback_source(session, path, params=params)
            if delayed:
                use_iss_only = True

        hist = data.get("history", {})
        cols = hist.get("columns", [])
        rows = hist.get("data", [])
        if not rows and start == 0 and not delayed:
            logger.warning("ALGOPACK index history is empty for secid=%s; retry via ISS", secid)
            mark_delayed_data_used()
            data = await iss_get_json(session, path, params=params)
            use_iss_only = True
            hist = data.get("history", {})
            cols = hist.get("columns", [])
            rows = hist.get("data", [])
        if not rows:
            break

        idx = {str(c).upper(): i for i, c in enumerate(cols)}
        dt_i = idx.get("TRADEDATE")
        if dt_i is None:
            break
        price_i = None
        for candidate in ("CLOSE", "CLOSEVALUE", "LEGALCLOSEPRICE", "CURRENTVALUE", "WAPRICE"):
            found = idx.get(candidate)
            if found is not None:
                price_i = found
                break
        if price_i is None:
            break

        for row in rows:
            if dt_i >= len(row) or price_i >= len(row):
                continue
            dt_raw = row[dt_i]
            px_raw = row[price_i]
            if not dt_raw or px_raw is None:
                continue
            try:
                points.append((date.fromisoformat(str(dt_raw)), float(px_raw)))
            except (TypeError, ValueError):
                continue

        if len(rows) < 100:
            break
        start += len(rows)

    if len(points) < 2:
        return None

    points.sort(key=lambda x: x[0])
    first = float(points[0][1])
    last = float(points[-1][1])
    if first <= 0:
        return None
    return (last - first) / first * 100.0

def _parse_securities_rows(data: dict) -> list[dict]:
    sec = data.get("securities", {})
    cols = sec.get("columns", [])
    rows = sec.get("data", [])
    if not rows:
        return []

    idx = {str(c).upper(): i for i, c in enumerate(cols)}

    def get(row: list, name: str):
        i = idx.get(name.upper())
        return row[i] if i is not None and i < len(row) else None

    out = []
    for r in rows[:50]:
        out.append(
            {
                "secid": get(r, "SECID"),
                "shortname": get(r, "SHORTNAME"),
                "name": get(r, "NAME"),
                "isin": get(r, "ISIN"),
                "boardid": get(r, "PRIMARYBOARDID") or get(r, "PRIMARY_BOARDID"),
                "is_traded": get(r, "IS_TRADED"),
                "group": get(r, "GROUP"),
            }
        )
    uniq: dict[tuple[str, str | None], dict] = {}
    for x in out:
        if x.get("secid"):
            uniq[(x["secid"], x.get("boardid"))] = x
    return list(uniq.values())

def _rank_by_query(items: list[dict], query: str) -> list[dict]:
    q = query.strip().lower()
    if not q:
        return items

    def score(x: dict) -> tuple[int, int, str]:
        secid = (x.get("secid") or "").lower()
        isin = (x.get("isin") or "").lower()
        shortname = (x.get("shortname") or "").lower()
        name = (x.get("name") or "").lower()

        # Меньше score -> выше в выдаче
        if secid == q or isin == q:
            return (0, len(secid), secid)
        if shortname == q or name == q:
            return (1, len(shortname or name), secid)
        if secid.startswith(q) or isin.startswith(q):
            return (2, len(secid), secid)
        if shortname.startswith(q) or name.startswith(q):
            return (3, len(shortname or name), secid)
        if q in shortname or q in name:
            return (4, len(shortname or name), secid)
        return (5, len(secid), secid)

    return sorted(items, key=score)
