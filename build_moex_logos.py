import asyncio
import json
from datetime import datetime, timezone

import aiohttp

ISS_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/securities.json"
LOGO_URL_TEMPLATE = "https://eodhd.com/img/logos/MCX/{ticker}.png"
OUTPUT_FILE = "moex_logos.json"


async def fetch_moex_share_tickers(session: aiohttp.ClientSession) -> list[str]:
    tickers: list[str] = []
    seen: set[str] = set()

    params = {
        "iss.meta": "off",
        "securities.columns": "SECID,BOARDID",
    }
    async with session.get(ISS_URL, params=params) as resp:
        resp.raise_for_status()
        data = await resp.json()

    sec = data.get("securities", {})
    cols = sec.get("columns", [])
    rows = sec.get("data", [])
    idx = {str(c).upper(): i for i, c in enumerate(cols)}
    secid_i = idx.get("SECID")
    board_i = idx.get("BOARDID")

    for row in rows:
        if secid_i is None or secid_i >= len(row):
            continue
        secid = str(row[secid_i] or "").strip().upper()
        if not secid or secid in seen:
            continue
        boardid = str(row[board_i] or "").strip().upper() if board_i is not None and board_i < len(row) else ""
        if boardid not in {"TQBR", "TQTF", "TQTD", "SMAL"}:
            continue
        seen.add(secid)
        tickers.append(secid)

    tickers.sort()
    return tickers


async def check_logo(session: aiohttp.ClientSession, ticker: str, sem: asyncio.Semaphore) -> tuple[str, bool]:
    url = LOGO_URL_TEMPLATE.format(ticker=ticker)
    async with sem:
        for attempt in range(4):
            try:
                async with session.head(url, allow_redirects=True) as resp:
                    if resp.status == 200:
                        return ticker, True
                    if resp.status in {404}:
                        return ticker, False
                    if resp.status not in {429, 500, 502, 503, 504}:
                        async with session.get(url, allow_redirects=True) as get_resp:
                            return ticker, get_resp.status == 200
                await asyncio.sleep(0.2 * (attempt + 1))
            except Exception:
                await asyncio.sleep(0.2 * (attempt + 1))
        return ticker, False


async def main() -> None:
    timeout = aiohttp.ClientTimeout(total=20, connect=5, sock_connect=5, sock_read=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tickers = await fetch_moex_share_tickers(session)

        sem = asyncio.Semaphore(8)
        checks = await asyncio.gather(*(check_logo(session, t, sem) for t in tickers))

    available = sorted([t for t, ok in checks if ok])
    missing = sorted([t for t, ok in checks if not ok])

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "MOEX ISS traded stock_shares + EODHD MCX logos",
        "logo_size_hint": "small PNG icons from EODHD",
        "default_logo_url": "https://dummyimage.com/32x32/e6e6e6/8a8a8a.png&text=%20",
        "total_moex_tickers_checked": len(tickers),
        "logos_found": len(available),
        "logos_missing": len(missing),
        "logos": {t: LOGO_URL_TEMPLATE.format(ticker=t) for t in available},
        "missing_tickers": missing,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


if __name__ == "__main__":
    asyncio.run(main())
