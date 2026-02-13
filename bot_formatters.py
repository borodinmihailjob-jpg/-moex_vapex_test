from __future__ import annotations

from datetime import datetime

from moex_iss import ASSET_TYPE_METAL


def money(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")


def money_signed(x: float) -> str:
    if x > 0:
        return f"+{money(x)}"
    if x < 0:
        return f"-{money(abs(x))}"
    return money(0.0)


def rub_amount(x: float | None) -> str:
    if x is None:
        return "н/д"
    try:
        return money(float(x))
    except (TypeError, ValueError):
        return "н/д"


def parse_ddmmyyyy(value: str) -> str | None:
    d = (value or "").strip()
    if len(d) != 10 or d[2] != "." or d[5] != ".":
        return None
    dd, mm, yyyy = d[:2], d[3:5], d[6:10]
    if not (dd.isdigit() and mm.isdigit() and yyyy.isdigit()):
        return None
    try:
        datetime.strptime(d, "%d.%m.%Y")
    except ValueError:
        return None
    return d


def board_mode_ru(boardid: str | None, asset_type: str) -> str:
    b = (boardid or "").strip().upper()
    stock_modes = {
        "TQBR": "Основной режим торгов акциями (Т+)",
        "TQTF": "Режим торгов ETF (Т+)",
        "TQTD": "Режим торгов депозитарными расписками (Т+)",
        "TQIF": "Режим торгов паями БПИФ/ПИФ (Т+)",
    }
    metal_modes = {
        "CETS": "Валютный рынок (сделки с драгоценными металлами)",
        "TOM": "Поставка TOM (расчеты завтра)",
    }

    if asset_type == ASSET_TYPE_METAL:
        if b in metal_modes:
            return metal_modes[b]
        return f"Режим торгов металлами ({b or 'не указан'})"

    if b in stock_modes:
        return stock_modes[b]
    return f"Режим торгов ({b or 'не указан'})"


def pnl_emoji(pnl_amount: float) -> str:
    return "📈" if pnl_amount >= 0 else "📉"


def fmt_pct(pct: float) -> str:
    return f"{pct:+.2f}%"


def pnl_label(pnl_amount: float, pnl_percent: float | None) -> str:
    if pnl_amount > 0:
        emoji = "📈"
    elif pnl_amount < 0:
        emoji = "📉"
    else:
        emoji = "➖"

    if pnl_percent is None:
        return f"{emoji} P&L: {money_signed(pnl_amount)} RUB"
    return f"{emoji} P&L: {pnl_percent:+.2f}% ({money_signed(pnl_amount)} RUB)"
