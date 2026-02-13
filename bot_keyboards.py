from __future__ import annotations

from aiogram.types import KeyboardButton, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from bot_formatters import money
from moex_iss import ASSET_TYPE_FIAT, ASSET_TYPE_METAL, ASSET_TYPE_STOCK

TRADE_SIDE_BUY = "buy"
TRADE_SIDE_SELL = "sell"


def _candidate_title(cand: dict, with_available: bool = False) -> str:
    secid = (cand.get("secid") or "").strip()
    boardid = (cand.get("boardid") or "").strip()
    display_name = (cand.get("shortname") or cand.get("name") or "").strip()
    if display_name and boardid:
        title = f"{secid} - {display_name} ({boardid})"
    elif display_name:
        title = f"{secid} - {display_name}"
    elif boardid:
        title = f"{secid} ({boardid})"
    else:
        title = secid
    if with_available and cand.get("available_qty") is not None:
        unit = "гр" if cand.get("asset_type") == ASSET_TYPE_METAL else "шт"
        title = f"{title} | доступно {float(cand['available_qty']):g} {unit}"
    return title[:64]


async def make_candidates_kb(cands: list[dict]):
    kb = InlineKeyboardBuilder()
    for i, cand in enumerate(cands):
        kb.button(text=_candidate_title(cand, with_available=True), callback_data=f"pick:{i}")
    kb.button(text="⬅️ Назад", callback_data="back:query")
    kb.adjust(1)
    return kb.as_markup()


async def make_asset_type_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Акции", callback_data=f"atype:{ASSET_TYPE_STOCK}")
    kb.button(text="🥇 Металл", callback_data=f"atype:{ASSET_TYPE_METAL}")
    kb.button(text="⬅️ Назад", callback_data="back:side")
    kb.adjust(1)
    return kb.as_markup()


async def make_trade_side_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🟢 Покупка", callback_data=f"side:{TRADE_SIDE_BUY}")
    kb.button(text="🔴 Продажа", callback_data=f"side:{TRADE_SIDE_SELL}")
    kb.adjust(1)
    return kb.as_markup()


async def make_lookup_candidates_kb(cands: list[dict]):
    kb = InlineKeyboardBuilder()
    for i, cand in enumerate(cands):
        kb.button(text=_candidate_title(cand), callback_data=f"lpick:{i}")
    kb.button(text="⬅️ Назад", callback_data="lback:query")
    kb.adjust(1)
    return kb.as_markup()


async def make_lookup_asset_type_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Акции", callback_data=f"latype:{ASSET_TYPE_STOCK}")
    kb.button(text="🥇 Металл", callback_data=f"latype:{ASSET_TYPE_METAL}")
    kb.adjust(1)
    return kb.as_markup()


async def make_alert_asset_type_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Акции", callback_data=f"aatype:{ASSET_TYPE_STOCK}")
    kb.button(text="🥇 Металлы", callback_data=f"aatype:{ASSET_TYPE_METAL}")
    kb.button(text="💵 Фиат", callback_data=f"aatype:{ASSET_TYPE_FIAT}")
    kb.adjust(1)
    return kb.as_markup()


async def make_alert_search_back_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="aaback:asset_type")
    return kb.as_markup()


async def make_alert_candidates_kb(cands: list[dict]):
    kb = InlineKeyboardBuilder()
    for i, cand in enumerate(cands):
        kb.button(text=_candidate_title(cand), callback_data=f"aapick:{i}")
    kb.button(text="⬅️ Назад", callback_data="aaback:query")
    kb.adjust(1)
    return kb.as_markup()


async def make_alert_range_confirm_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да, ±5%", callback_data="aarange:yes")
    kb.button(text="Только точное значение", callback_data="aarange:no")
    kb.adjust(1)
    return kb.as_markup()


async def make_alerts_list_kb(alerts: list[dict]):
    kb = InlineKeyboardBuilder()
    for alert in alerts:
        secid = alert.get("secid") or "?"
        shortname = (alert.get("shortname") or "").strip()
        target_price = float(alert.get("target_price") or 0.0)
        range_percent = float(alert.get("range_percent") or 0.0)
        label = f"{shortname} ({secid})" if shortname else secid
        if range_percent > 0:
            text = f"🔔 {label}: {money(target_price)} ±{range_percent:g}%"
        else:
            text = f"🔔 {label}: {money(target_price)}"
        kb.button(text=text[:64], callback_data=f"talert:{int(alert['id'])}")
    kb.adjust(1)
    return kb.as_markup()


async def make_alert_disable_confirm_kb(alert_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="Отключить", callback_data=f"talertoff:{alert_id}")
    kb.button(text="Отмена", callback_data="talertlist")
    kb.adjust(1)
    return kb.as_markup()


async def make_date_mode_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="Сегодня", callback_data="date:today")
    kb.button(text="Ввести дату", callback_data="date:manual")
    kb.adjust(1)
    return kb.as_markup()


async def make_search_back_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="back:asset_type")
    return kb.as_markup()


async def make_lookup_search_back_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="lback:asset_type")
    return kb.as_markup()


async def make_qty_back_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="back:instrument")
    return kb.as_markup()


async def make_price_back_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="back:qty")
    return kb.as_markup()


async def make_confirm_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="💾 Сохранить", callback_data="confirm:save")
    kb.button(text="✏️ Редактировать", callback_data="confirm:edit")
    kb.adjust(1)
    return kb.as_markup()


async def make_edit_step_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="Дата", callback_data="edit:date")
    kb.button(text="Покупка/продажа", callback_data="edit:side")
    kb.button(text="Тип актива", callback_data="edit:asset_type")
    kb.button(text="Инструмент", callback_data="edit:instrument")
    kb.button(text="Количество", callback_data="edit:qty")
    kb.button(text="Цена за единицу", callback_data="edit:price")
    kb.adjust(1)
    return kb.as_markup()


async def make_portfolio_map_mode_kb(
    self_callback: str = "pmap:self",
    share_callback: str = "pmap:share",
):
    kb = InlineKeyboardBuilder()
    kb.button(text="🧩 Карта для себя", callback_data=self_callback)
    kb.button(text="📤 Поделиться картой портфеля", callback_data=share_callback)
    kb.adjust(1)
    return kb.as_markup()


def make_main_menu_kb(
    btn_add_trade: str,
    btn_portfolio: str,
    btn_asset_lookup: str,
    btn_portfolio_map: str,
    btn_top_movers: str,
    btn_usd_rub: str,
    btn_why_invest: str,
    btn_alerts: str,
) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=btn_add_trade), KeyboardButton(text=btn_portfolio)],
            [KeyboardButton(text=btn_asset_lookup), KeyboardButton(text=btn_portfolio_map)],
            [KeyboardButton(text=btn_top_movers), KeyboardButton(text=btn_usd_rub)],
            [KeyboardButton(text=btn_why_invest)],
            [KeyboardButton(text=btn_alerts)],
        ],
        resize_keyboard=True,
    )


async def make_clear_portfolio_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🗑️ Да, очистить", callback_data="pfclear:yes")
    kb.button(text="Отмена", callback_data="pfclear:no")
    kb.adjust(1)
    return kb.as_markup()
