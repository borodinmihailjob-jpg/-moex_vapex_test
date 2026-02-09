import aiohttp
import logging
from datetime import date

BASE = "https://iss.moex.com/iss"
logger = logging.getLogger(__name__)

ASSET_TYPE_STOCK = "stock"
ASSET_TYPE_METAL = "metal"

async def iss_get_json(session: aiohttp.ClientSession, path: str, params: dict | None = None) -> dict:
    url = f"{BASE}{path}"
    try:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception:
        logger.exception("ISS request failed: %s params=%s", url, params)
        raise

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

    data = await iss_get_json(session, path, params={"iss.meta": "off"})
    md = data.get("marketdata", {})
    cols = md.get("columns", [])
    rows = md.get("data", [])
    if not rows:
        logger.warning("No marketdata rows for secid=%s boardid=%s", secid, boardid)
        return None

    idx = {c: i for i, c in enumerate(cols)}
    if "LAST" not in idx:
        logger.warning("LAST column is missing for secid=%s boardid=%s", secid, boardid)
        return None

    last = rows[0][idx["LAST"]]
    if last is None:
        logger.warning("LAST is null for secid=%s boardid=%s", secid, boardid)
        return None
    price = float(last)
    logger.info("ISS last price secid=%s boardid=%s last=%s", secid, boardid, price)
    return price

async def get_last_price_metal(session: aiohttp.ClientSession, secid: str, boardid: str | None = None) -> float | None:
    """
    Для металлов (currency_metal) берём marketdata.LAST на engine=currency, market=selt.
    """
    if boardid:
        path = f"/engines/currency/markets/selt/boards/{boardid}/securities/{secid}.json"
    else:
        path = f"/engines/currency/markets/selt/securities/{secid}.json"

    data = await iss_get_json(session, path, params={"iss.meta": "off"})
    md = data.get("marketdata", {})
    cols = md.get("columns", [])
    rows = md.get("data", [])
    if not rows:
        logger.warning("No marketdata rows for metal secid=%s boardid=%s", secid, boardid)
        return None

    idx = {c: i for i, c in enumerate(cols)}
    if "LAST" not in idx:
        logger.warning("LAST column is missing for metal secid=%s boardid=%s", secid, boardid)
        return None

    last = rows[0][idx["LAST"]]
    if last is None:
        logger.warning("LAST is null for metal secid=%s boardid=%s", secid, boardid)
        return None
    price = float(last)
    logger.info("ISS last metal price secid=%s boardid=%s last=%s", secid, boardid, price)
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

    while True:
        params = {
            "iss.meta": "off",
            "from": from_date.isoformat(),
            "till": till_date.isoformat(),
            "start": start,
            "history.columns": "TRADEDATE,CLOSE,LEGALCLOSEPRICE,WAPRICE",
        }
        data = await iss_get_json(session, path, params=params)
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
            except Exception:
                continue

        if len(rows) < 100:
            break
        start += len(rows)

    out.sort(key=lambda x: x[0])
    return out

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
