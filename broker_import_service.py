from __future__ import annotations

from dataclasses import dataclass

import aiohttp

from broker_report_xml import parse_broker_report_xml
from common_utils import pick_stock_candidate_by_isin
from db import add_trade, upsert_instrument
from moex_iss import ASSET_TYPE_METAL, search_securities


@dataclass(frozen=True)
class BrokerImportResult:
    file: str
    rows: int
    imported: int
    duplicates: int
    skipped: int
    unresolved_isins: tuple[str, ...]


async def import_broker_xml_trades(
    db_dsn: str,
    user_id: int,
    file_name: str,
    xml_bytes: bytes,
) -> BrokerImportResult:
    parsed_trades = parse_broker_report_xml(xml_bytes)
    if not parsed_trades:
        raise ValueError("В выписке не найдены сделки в блоке trades_finished.")

    imported = 0
    duplicates = 0
    skipped = 0
    unresolved_isins: set[str] = set()
    stock_cache: dict[str, dict | None] = {}
    source_name = (file_name or "broker_report.xml")[:255]

    async with aiohttp.ClientSession() as session:
        for trade in parsed_trades:
            secid = None
            boardid = ""
            shortname = (trade.asset_name or "").strip() or None
            asset_type = trade.asset_type

            if asset_type == ASSET_TYPE_METAL:
                secid = trade.metal_secid
            else:
                cached = stock_cache.get(trade.isin_reg)
                if cached is None and trade.isin_reg not in stock_cache:
                    candidates = await search_securities(session, trade.isin_reg)
                    cached = pick_stock_candidate_by_isin(candidates, trade.isin_reg)
                    stock_cache[trade.isin_reg] = cached
                else:
                    cached = stock_cache.get(trade.isin_reg)
                if cached:
                    secid = str(cached.get("secid") or "").strip() or None
                    boardid = str(cached.get("boardid") or "").strip()
                    if not shortname:
                        shortname = (cached.get("shortname") or cached.get("name") or "").strip() or None
                else:
                    unresolved_isins.add(trade.isin_reg)

            if not secid:
                skipped += 1
                continue

            instrument_id = await upsert_instrument(
                db_dsn,
                secid=secid,
                isin=trade.isin_reg,
                boardid=boardid,
                shortname=shortname,
                asset_type=asset_type,
            )
            was_inserted = await add_trade(
                db_dsn,
                user_id=user_id,
                instrument_id=instrument_id,
                trade_date=trade.trade_date,
                qty=trade.qty,
                price=trade.price,
                commission=trade.commission,
                external_trade_id=f"broker_xml:{trade.trade_no}",
                import_source=source_name,
            )
            if was_inserted:
                imported += 1
            else:
                duplicates += 1

    return BrokerImportResult(
        file=source_name,
        rows=len(parsed_trades),
        imported=imported,
        duplicates=duplicates,
        skipped=skipped,
        unresolved_isins=tuple(sorted(unresolved_isins)),
    )
