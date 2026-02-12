#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _ok(msg: str) -> None:
    print(f"[OK] {msg}")


def _fail(msg: str) -> None:
    print(f"[FAIL] {msg}")


def check_imports() -> None:
    modules = [
        "db",
        "moex_iss",
        "broker_report_xml",
        "portfolio_cards",
        "main",
    ]
    for name in modules:
        importlib.import_module(name)
        _ok(f"import {name}")


def check_png_render() -> None:
    from portfolio_cards import build_portfolio_map_png, build_portfolio_share_card_png

    tiles = [
        {"secid": "SBER", "shortname": "Сбербанк", "value": 120000.0, "weight": 120000.0, "pnl_pct": 12.4},
        {"secid": "GAZP", "shortname": "Газпром", "value": 70000.0, "weight": 70000.0, "pnl_pct": -3.1},
    ]
    map_png = build_portfolio_map_png(tiles)
    if not map_png.startswith(b"\x89PNG"):
        raise RuntimeError("portfolio map is not a PNG")
    if len(map_png) < 10_000:
        raise RuntimeError("portfolio map PNG is unexpectedly small")
    _ok("portfolio map PNG render")

    share_png = build_portfolio_share_card_png(
        composition_rows=[
            {"instrument_id": 1, "secid": "SBER", "name_ru": "Сбербанк", "share_pct": 62.5, "ret_30d": 7.8},
            {"instrument_id": 2, "secid": "GAZP", "name_ru": "Газпром", "share_pct": 37.5, "ret_30d": -1.2},
        ],
        portfolio_return_30d=4.2,
        moex_return_30d=1.9,
        top_gainers=[{"shortname": "Сбербанк", "secid": "SBER", "pnl_pct": 12.4}],
        top_losers=[{"shortname": "Газпром", "secid": "GAZP", "pnl_pct": -3.1}],
    )
    if not share_png.startswith(b"\x89PNG"):
        raise RuntimeError("share card is not a PNG")
    if len(share_png) < 10_000:
        raise RuntimeError("share card PNG is unexpectedly small")
    _ok("share card PNG render")


def check_xml_parser() -> None:
    from broker_report_xml import parse_broker_report_xml

    xml = """<?xml version=\"1.0\" encoding=\"utf-8\"?>
<report_broker>
  <trades_finished>
    <trade>
      <trade_no>123</trade_no>
      <db_time>2026-02-01T12:34:56</db_time>
      <isin_reg>RU0009029540</isin_reg>
      <p_name>Сбербанк</p_name>
      <qty>10</qty>
      <Price>300.5</Price>
      <bank_tax>1.25</bank_tax>
    </trade>
  </trades_finished>
</report_broker>
"""
    rows = parse_broker_report_xml(xml.encode("utf-8"))
    if len(rows) != 1:
        raise RuntimeError(f"unexpected parsed rows: {len(rows)}")
    row = rows[0]
    if row.trade_no != "123" or row.price <= 0:
        raise RuntimeError("parsed trade content mismatch")
    _ok("broker XML parser")


async def check_moex_flags() -> None:
    from moex_iss import delayed_data_used, mark_delayed_data_used, reset_data_source_flags

    reset_data_source_flags()
    if delayed_data_used():
        raise RuntimeError("delayed flag should be false after reset")

    async def child_mark() -> None:
        mark_delayed_data_used()

    await asyncio.gather(child_mark())
    if not delayed_data_used():
        raise RuntimeError("delayed flag did not propagate from gathered task")
    _ok("delayed-data flag propagation")


async def check_optional_db() -> None:
    from db import close_pools, init_db, list_users_with_alerts

    dsn = (os.getenv("DATABASE_URL") or "").strip()
    if not dsn:
        print("[SKIP] DB smoke: DATABASE_URL is empty")
        return
    await init_db(dsn)
    users = await list_users_with_alerts(dsn)
    if not isinstance(users, list):
        raise RuntimeError("list_users_with_alerts should return list")
    _ok("DB init + simple query")
    await close_pools()


async def check_optional_network() -> None:
    import aiohttp
    from moex_iss import get_history_prices_by_asset_type, get_last_price_by_asset_type

    async with aiohttp.ClientSession() as session:
        last = await get_last_price_by_asset_type(session, "SBER", "TQBR", "stock")
        if last is not None and last <= 0:
            raise RuntimeError("unexpected non-positive last price")
        history = await get_history_prices_by_asset_type(
            session=session,
            secid="SBER",
            boardid="TQBR",
            asset_type="stock",
            from_date=date.today() - timedelta(days=10),
            till_date=date.today(),
        )
        if history and history[-1][1] <= 0:
            raise RuntimeError("unexpected non-positive history price")
    _ok("MOEX network smoke")


async def _run(args: argparse.Namespace) -> int:
    try:
        check_imports()
        check_png_render()
        check_xml_parser()
        await check_moex_flags()
        if args.with_db:
            await check_optional_db()
        if args.with_network:
            await check_optional_network()
    except Exception as exc:
        _fail(f"{exc.__class__.__name__}: {exc}")
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke checks for MOEX Portfolio Bot")
    parser.add_argument("--with-db", action="store_true", help="Run DB checks (requires DATABASE_URL)")
    parser.add_argument("--with-network", action="store_true", help="Run live MOEX API checks")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
