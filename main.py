import os
import logging
import asyncio
import html
import io
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo
import aiohttp
from aiohttp import web
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from aiogram.filters import Command, StateFilter
from aiogram.types import CallbackQuery, Message, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from db import (
    acquire_single_instance_lock,
    release_single_instance_lock,
    close_pools,
    clear_user_portfolio,
    init_db,
    upsert_instrument,
    add_trade,
    get_position_agg,
    get_instrument,
    get_user_positions,
    ensure_user_alert_settings,
    set_periodic_alert,
    set_drop_alert,
    set_open_close_alert,
    get_user_alert_settings,
    list_users_with_alerts,
    update_periodic_last_sent_at,
    update_open_sent_date,
    update_midday_sent_date,
    update_main_close_sent_date,
    update_close_sent_date,
    update_day_open_value,
    get_price_alert_states_bulk,
    set_price_alert_states_bulk,
    list_active_position_instruments,
    upsert_price_cache_bulk,
    get_price_cache_map,
    get_active_app_text,
    list_active_app_texts,
)
from portfolio_cards import build_portfolio_map_png, build_portfolio_share_card_png
from moex_iss import (
    ASSET_TYPE_METAL,
    ASSET_TYPE_STOCK,
    DELAYED_WARNING_TEXT,
    delayed_data_used,
    get_moex_index_return_percent,
    get_stock_movers_by_date,
    get_history_prices_by_asset_type,
    get_last_price_by_asset_type,
    reset_data_source_flags,
    search_metals,
    search_securities,
)
from broker_report_xml import parse_broker_report_xml

load_dotenv()


def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()

MSK_TZ = ZoneInfo("Europe/Moscow")
MOEX_OPEN_HOUR = int(_env("MOEX_OPEN_HOUR") or "10")
MOEX_OPEN_MINUTE = int(_env("MOEX_OPEN_MINUTE") or "0")
TRADING_DAY_OPEN_HOUR = int(_env("TRADING_DAY_OPEN_HOUR") or "6")
TRADING_DAY_OPEN_MINUTE = int(_env("TRADING_DAY_OPEN_MINUTE") or "50")
TRADING_DAY_MIDDAY_HOUR = int(_env("TRADING_DAY_MIDDAY_HOUR") or "14")
TRADING_DAY_MIDDAY_MINUTE = int(_env("TRADING_DAY_MIDDAY_MINUTE") or "30")
TRADING_DAY_MAIN_CLOSE_HOUR_ENV = _env("TRADING_DAY_MAIN_CLOSE_HOUR")
TRADING_DAY_MAIN_CLOSE_MINUTE_ENV = _env("TRADING_DAY_MAIN_CLOSE_MINUTE")
TRADING_DAY_EVENING_CLOSE_HOUR = int(_env("TRADING_DAY_EVENING_CLOSE_HOUR") or "23")
TRADING_DAY_EVENING_CLOSE_MINUTE = int(_env("TRADING_DAY_EVENING_CLOSE_MINUTE") or "50")
MOEX_EVENT_WINDOW_MIN = 5
MAX_BROKER_XML_SIZE_BYTES = 5 * 1024 * 1024
PRICE_FETCH_CONCURRENCY = 20
PRICE_FETCH_BATCH_SIZE = 100
USER_ALERTS_CONCURRENCY = 10
BTN_ADD_TRADE = "Добавить сделку"
BTN_PORTFOLIO = "Стоимость портфеля"
BTN_ALERTS = "Настройки уведомлений"
BTN_WHY_INVEST = "Зачем инвестировать"
BTN_ASSET_LOOKUP = "Поиск цены"
BTN_PORTFOLIO_MAP = "Карта портфеля"
BTN_TOP_MOVERS = "Топ роста/падения"
CB_PORTFOLIO_MAP_SELF = "pmap:self"
CB_PORTFOLIO_MAP_SHARE = "pmap:share"
TRADE_SIDE_BUY = "buy"
TRADE_SIDE_SELL = "sell"


def get_trading_day_main_close_time(now_msk: datetime) -> tuple[int, int]:
    if TRADING_DAY_MAIN_CLOSE_HOUR_ENV and TRADING_DAY_MAIN_CLOSE_MINUTE_ENV:
        return int(TRADING_DAY_MAIN_CLOSE_HOUR_ENV), int(TRADING_DAY_MAIN_CLOSE_MINUTE_ENV)
    switch_date = date(2026, 3, 23)
    if now_msk.date() >= switch_date:
        return 19, 0
    return 18, 50


def setup_logging() -> None:
    project_root = Path(__file__).resolve().parent
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    file_handler = RotatingFileHandler(
        logs_dir / "bot.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    console_handler = logging.StreamHandler()
    file_handler.setFormatter(fmt)
    console_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)

setup_logging()
logger = logging.getLogger(__name__)

BOT_TOKEN = _env("BOT_TOKEN") or _env("TELEGRAM_BOT_TOKEN")
DB_DSN = _env("DATABASE_URL") or _env("DB_DSN") or _env("DB_PATH")

class AddTradeFlow(StatesGroup):
    waiting_date_mode = State()
    waiting_date_manual = State()
    waiting_side = State()
    waiting_asset_type = State()
    waiting_query = State()
    waiting_pick = State()
    waiting_qty = State()
    waiting_price = State()
    waiting_confirm = State()
    waiting_edit_step = State()
    waiting_more = State()

class AssetLookupFlow(StatesGroup):
    waiting_asset_type = State()
    waiting_query = State()
    waiting_pick = State()

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


def _ru_weekday_short(d: date) -> str:
    names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    return names[d.weekday()]


def _top_movers_date_options(base_date: date) -> list[tuple[str, date]]:
    return [
        ("Текущая", base_date),
        ("Вчера", base_date - timedelta(days=1)),
        ("Позавчера", base_date - timedelta(days=2)),
    ]


async def make_top_movers_dates_kb(selected: date | None = None):
    base = datetime.now(MSK_TZ).date()
    options = _top_movers_date_options(base)
    kb = InlineKeyboardBuilder()
    for label, d in options:
        mark = "• " if selected and selected == d else ""
        text = f"{mark}{label} ({_ru_weekday_short(d)} {d.strftime('%d.%m')})"
        kb.button(text=text[:64], callback_data=f"tmdate:{d.isoformat()}")
    kb.adjust(1)
    return kb.as_markup()


def build_top_movers_text(movers: list[dict], selected_date: date) -> str:
    now_msk = datetime.now(MSK_TZ)
    open_label = f"{MOEX_OPEN_HOUR:02d}:{MOEX_OPEN_MINUTE:02d}"
    asof_label = now_msk.strftime("%H:%M")

    gainers = sorted(movers, key=lambda x: x["pct"], reverse=True)[:10]
    losers = sorted([m for m in movers if m["pct"] < 0], key=lambda x: x["pct"])[:5]

    today_msk = now_msk.date()
    if selected_date == today_msk:
        period_line = f"Период: {open_label}–{asof_label} МСК"
    else:
        period_line = f"Дата: {selected_date.strftime('%d.%m.%Y')}"

    lines = [
        "Топ акций за сессию MOEX (TQBR)",
        period_line,
        "",
        "📈 Топ-10 роста:",
    ]
    for i, m in enumerate(gainers, 1):
        lines.append(
            f"{i}. {m['secid']} ({m['shortname']}) — {m['pct']:+.2f}% "
            f"({money(m['open'])} → {money(m['last'])}) | "
            f"Объём торгов за день: {rub_amount(m.get('val_today'))} RUB"
        )

    lines.extend(["", "📉 Топ-5 падения:"])
    if not losers:
        lines.append("За выбранную дату падения не обнаружены.")
    else:
        for i, m in enumerate(losers, 1):
            lines.append(
                f"{i}. {m['secid']} ({m['shortname']}) — {m['pct']:+.2f}% "
                f"({money(m['open'])} → {money(m['last'])}) | "
                f"Объём торгов за день: {rub_amount(m.get('val_today'))} RUB"
            )
    return "\n".join(lines)


async def safe_edit_text(message: Message | None, text: str, reply_markup=None) -> None:
    if message is None:
        return
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        # Benign Telegram response when the message text is unchanged.
        if "message is not modified" in str(exc).lower():
            return
        raise
    except TelegramNetworkError:
        logger.warning("Telegram network error during edit_text; falling back to answer()")
        try:
            await message.answer(text, reply_markup=reply_markup)
        except Exception:
            logger.exception("Failed fallback answer after edit_text network error")


def _article_button_text(button_name: str, text_code: str) -> str:
    raw = str(button_name or "").strip()
    if raw:
        return raw[:64]
    raw = str(text_code or "").strip()
    if not raw:
        return "Статья"
    label = raw.replace("_", " ").replace("-", " ").strip().title()
    return label[:64]


async def make_articles_kb():
    items = await list_active_app_texts(DB_DSN)
    kb = InlineKeyboardBuilder()
    for item in items:
        text_code = item["text_code"]
        button_name = item.get("button_name") or ""
        kb.button(text=_article_button_text(button_name, text_code), callback_data=f"article:{text_code}")
    kb.adjust(1)
    return kb.as_markup(), items

async def make_candidates_kb(cands: list[dict]):
    kb = InlineKeyboardBuilder()
    for i, c in enumerate(cands):
        secid = (c.get("secid") or "").strip()
        boardid = (c.get("boardid") or "").strip()
        display_name = (c.get("shortname") or c.get("name") or "").strip()
        available_qty = c.get("available_qty")
        if display_name and boardid:
            title = f"{secid} - {display_name} ({boardid})"
        elif display_name:
            title = f"{secid} - {display_name}"
        elif boardid:
            title = f"{secid} ({boardid})"
        else:
            title = secid
        if available_qty is not None:
            unit = "гр" if c.get("asset_type") == ASSET_TYPE_METAL else "шт"
            title = f"{title} | доступно {float(available_qty):g} {unit}"
        kb.button(text=title[:64], callback_data=f"pick:{i}")
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
    for i, c in enumerate(cands):
        secid = (c.get("secid") or "").strip()
        boardid = (c.get("boardid") or "").strip()
        display_name = (c.get("shortname") or c.get("name") or "").strip()
        if display_name and boardid:
            title = f"{secid} - {display_name} ({boardid})"
        elif display_name:
            title = f"{secid} - {display_name}"
        elif boardid:
            title = f"{secid} ({boardid})"
        else:
            title = secid
        kb.button(text=title[:64], callback_data=f"lpick:{i}")
    kb.button(text="⬅️ Назад", callback_data="lback:query")
    kb.adjust(1)
    return kb.as_markup()

async def make_lookup_asset_type_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Акции", callback_data=f"latype:{ASSET_TYPE_STOCK}")
    kb.button(text="🥇 Металл", callback_data=f"latype:{ASSET_TYPE_METAL}")
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

async def make_portfolio_map_mode_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🧩 Карта для себя", callback_data=CB_PORTFOLIO_MAP_SELF)
    kb.button(text="📤 Поделиться картой портфеля", callback_data=CB_PORTFOLIO_MAP_SHARE)
    kb.adjust(1)
    return kb.as_markup()

def make_main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ADD_TRADE), KeyboardButton(text=BTN_PORTFOLIO)],
            [KeyboardButton(text=BTN_ASSET_LOOKUP), KeyboardButton(text=BTN_PORTFOLIO_MAP)],
            [KeyboardButton(text=BTN_TOP_MOVERS)],
            [KeyboardButton(text=BTN_WHY_INVEST)],
            [KeyboardButton(text=BTN_ALERTS)],
        ],
        resize_keyboard=True,
    )

def today_ddmmyyyy() -> str:
    return datetime.now(MSK_TZ).strftime("%d.%m.%Y")

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

def build_trade_preview(data: dict) -> str:
    chosen = data["chosen"]
    asset_type = data.get("asset_type") or ASSET_TYPE_STOCK
    trade_side = data.get("trade_side") or TRADE_SIDE_BUY
    side_label = "Покупка" if trade_side == TRADE_SIDE_BUY else "Продажа"
    qty_unit = "гр" if asset_type == ASSET_TYPE_METAL else "шт"
    qty = abs(float(data["qty"]))
    price = data["price"]
    total = qty * price
    return (
        "Проверь сделку:\n\n"
        f"Дата: {data['trade_date']}\n"
        f"Операция: {side_label}\n"
        f"Тип актива: {'Металл' if asset_type == ASSET_TYPE_METAL else 'Акции'}\n"
        f"Инструмент: {chosen['secid']} ({chosen.get('shortname') or ''})\n"
        f"Количество: {qty:g} {qty_unit}\n"
        f"Цена за единицу: {money(price)} RUB\n"
        f"Сумма: {money(total)} RUB\n"
    )

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

def pnl_emoji(pnl_amount: float) -> str:
    return "📈" if pnl_amount >= 0 else "📉"

def fmt_pct(pct: float) -> str:
    return f"{pct:+.2f}%"


def append_delayed_warning(text: str) -> str:
    if delayed_data_used():
        return f"{text}\n{DELAYED_WARNING_TEXT}"
    return text


async def build_asset_dynamics_text(chosen: dict, asset_type: str) -> str:
    secid = chosen.get("secid") or "UNKNOWN"
    boardid = chosen.get("boardid")
    name = (chosen.get("shortname") or chosen.get("name") or secid).strip()
    today = date.today()
    periods = [
        ("За неделю", 7),
        ("За месяц", 30),
        ("За 6 месяцев", 182),
        ("За год", 365),
    ]

    reset_data_source_flags()
    async with aiohttp.ClientSession() as session:
        current = await get_last_price_by_asset_type(session, secid, boardid, asset_type)
        lines = [f"{name} ({secid})"]
        lines.append(f"Текущая цена: {money(current)} RUB" if current is not None else "Текущая цена: нет данных")
        lines.extend(["", "Динамика:"])
        for label, days in periods:
            history = await get_history_prices_by_asset_type(
                session,
                secid=secid,
                boardid=boardid,
                asset_type=asset_type,
                from_date=today - timedelta(days=days),
                till_date=today,
            )
            if not history:
                lines.append(f"{label}: нет данных")
                continue

            base_price = history[0][1]
            end_price = current if current is not None else history[-1][1]
            if base_price <= 0:
                lines.append(f"{label}: нет данных")
                continue
            delta = end_price - base_price
            pct = (delta / base_price) * 100.0
            emoji = "📈" if delta >= 0 else "📉"
            lines.append(
                f"{label}: {emoji} {fmt_pct(pct)} ({money_signed(delta)} RUB)"
            )
    return append_delayed_warning("\n".join(lines))

def _cache_age_seconds(updated_at: datetime | None, now_utc: datetime) -> float | None:
    if updated_at is None:
        return None
    dt = updated_at
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (now_utc - dt.astimezone(timezone.utc)).total_seconds())


async def _fetch_prices_limited(rows: list[dict]) -> list[tuple[dict, float | None]]:
    if not rows:
        return []

    sem = asyncio.Semaphore(PRICE_FETCH_CONCURRENCY)
    out: list[tuple[dict, float | None]] = []

    async with aiohttp.ClientSession() as session:
        async def load_price(row: dict) -> tuple[dict, float | None]:
            async with sem:
                try:
                    last = await get_last_price_by_asset_type(
                        session,
                        row["secid"],
                        row.get("boardid"),
                        row.get("asset_type") or ASSET_TYPE_STOCK,
                    )
                    return row, last
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
                    logger.warning(
                        "Failed to load price secid=%s boardid=%s",
                        row.get("secid"),
                        row.get("boardid"),
                    )
                    return row, None

        for i in range(0, len(rows), PRICE_FETCH_BATCH_SIZE):
            batch = rows[i:i + PRICE_FETCH_BATCH_SIZE]
            out.extend(await asyncio.gather(*(load_price(row) for row in batch)))
    return out

async def refresh_price_cache_once() -> None:
    instruments = await list_active_position_instruments(DB_DSN)
    if not instruments:
        return

    priced = await _fetch_prices_limited(instruments)
    now_utc = datetime.now(timezone.utc)
    cache_rows = [
        (int(row["instrument_id"]), float(last))
        for row, last in priced
        if last is not None
    ]
    await upsert_price_cache_bulk(DB_DSN, cache_rows, now_utc)

async def _load_prices_for_positions(positions: list[dict]) -> dict[int, float | None]:
    now_utc = datetime.now(timezone.utc)
    instrument_ids = [int(pos["id"]) for pos in positions]
    cache = await get_price_cache_map(DB_DSN, instrument_ids)

    prices: dict[int, float | None] = {}
    missing_positions: list[dict] = []
    for pos in positions:
        iid = int(pos["id"])
        rec = cache.get(iid)
        if rec:
            age = _cache_age_seconds(rec.get("updated_at"), now_utc)
            if age is not None and age <= 120:
                prices[iid] = float(rec["last_price"])
                continue
        missing_positions.append(pos)

    if not missing_positions:
        return prices

    loaded = await _fetch_prices_limited(missing_positions)
    cache_rows: list[tuple[int, float]] = []
    for pos, last in loaded:
        iid = int(pos["id"])
        prices[iid] = last
        if last is not None:
            cache_rows.append((iid, float(last)))
    await upsert_price_cache_bulk(DB_DSN, cache_rows, now_utc)
    return prices

async def build_portfolio_report(user_id: int) -> tuple[str, float | None, list[dict]]:
    positions = await get_user_positions(DB_DSN, user_id)
    if not positions:
        return ("Портфель пуст.", None, [])
    reset_data_source_flags()
    prices = await _load_prices_for_positions(positions)

    total_value_known = 0.0
    total_cost_known = 0.0
    unknown_prices = 0
    lines = []

    for pos in positions:
        last = prices.get(int(pos["id"]))
        qty = pos["total_qty"]
        ticker = str(pos["secid"]).strip()
        asset_name_raw = (pos.get("shortname") or ticker).strip()
        asset_name = html.escape(asset_name_raw)
        ticker_safe = html.escape(ticker)
        unit = "гр" if (pos.get("asset_type") == ASSET_TYPE_METAL) else "акции"
        total_cost = float(pos.get("total_cost") or 0.0)

        if last is None:
            unknown_prices += 1
            lines.append(
                f"• {asset_name} - {ticker_safe} - {qty:g} {unit} - Общая стоимость актива: нет данных - P&L: нет данных"
            )
            continue

        value = qty * last
        pnl = value - total_cost
        pnl_pct = (pnl / total_cost * 100.0) if abs(total_cost) > 1e-12 else None

        total_value_known += value
        total_cost_known += total_cost
        emoji = pnl_emoji(pnl)
        if pnl_pct is None:
            pnl_tail = f"{emoji} {money_signed(pnl)} RUB"
        else:
            pnl_tail = f"{emoji} {pnl_pct:+.2f}% {money_signed(pnl)} RUB"
        lines.append(
            f"• {asset_name} - {ticker_safe} - {qty:g} {unit} - Общая стоимость актива: <b>{money(value)}</b> RUB - P&L {pnl_tail}"
        )

    total_pnl = total_value_known - total_cost_known
    total_pnl_pct = (total_pnl / total_cost_known * 100.0) if abs(total_cost_known) > 1e-12 else None
    total_emoji = pnl_emoji(total_pnl)
    if total_pnl_pct is None:
        total_pnl_text = f"{total_emoji} <b>{money_signed(total_pnl)} RUB</b>"
    else:
        total_pnl_text = f"{total_emoji} {total_pnl_pct:+.2f}% <b>{money_signed(total_pnl)} RUB</b>"
    footer = (
        f"💰 Итоговая стоимость активов по всем тикерам: <b>{money(total_value_known)}</b> RUB\n"
        f"P&L: {total_pnl_text}"
    )
    if unknown_prices:
        footer += f"\nНет рыночной цены для {unknown_prices} инструментов, они не включены в итог."
    if delayed_data_used():
        footer += f"\n{DELAYED_WARNING_TEXT}"

    text = "💼 Портфель:\n" + "\n".join(lines) + "\n\n" + footer
    return (text, total_value_known, positions)


async def _load_sell_candidates(user_id: int, asset_type: str) -> list[dict]:
    positions = await get_user_positions(DB_DSN, user_id)
    out: list[dict] = []
    for pos in positions:
        if (pos.get("asset_type") or ASSET_TYPE_STOCK) != asset_type:
            continue
        qty = float(pos.get("total_qty") or 0.0)
        if qty <= 1e-12:
            continue
        out.append(
            {
                "secid": pos.get("secid"),
                "shortname": pos.get("shortname"),
                "name": pos.get("shortname"),
                "isin": pos.get("isin"),
                "boardid": pos.get("boardid"),
                "asset_type": pos.get("asset_type"),
                "available_qty": qty,
            }
        )
    out.sort(key=lambda x: str(x.get("secid") or ""))
    return out


def _pick_stock_candidate_by_isin(cands: list[dict], isin: str) -> dict | None:
    isin_upper = isin.strip().upper()
    if not isin_upper:
        return cands[0] if cands else None
    for c in cands:
        if str(c.get("isin") or "").strip().upper() == isin_upper:
            return c
    return cands[0] if cands else None


async def _import_broker_xml_trades(user_id: int, file_name: str, xml_bytes: bytes) -> str:
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
                DB_DSN,
                secid=secid,
                isin=t.isin_reg,
                boardid=boardid,
                shortname=shortname,
                asset_type=asset_type,
            )
            was_inserted = await add_trade(
                DB_DSN,
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

    lines = [
        f"Импорт завершен: {source_name}",
        f"Сделок в выписке: {len(parsed_trades)}",
        f"Добавлено: {imported}",
        f"Пропущено как дубликаты: {duplicates}",
        f"Пропущено (не удалось сопоставить инструмент): {skipped}",
    ]
    if unresolved_isins:
        show = ", ".join(sorted(unresolved_isins)[:12])
        tail = "" if len(unresolved_isins) <= 12 else f" и еще {len(unresolved_isins) - 12}"
        lines.append(f"Не сопоставлены ISIN: {show}{tail}")
    return "\n".join(lines)

async def cmd_start(message: Message):
    logger.info("User %s started bot", message.from_user.id if message.from_user else None)
    await message.answer(
        "Привет! Я помогу тебе учитывать сделки и следить за портфелем на MOEX 📈\n"
        "Покажу текущую стоимость, доходность и динамику по инструментам.\n"
        "💼 Портфель\n"
        "/add_trade — добавить сделку (покупка/продажа)\n"
        "/portfolio — стоимость портфеля и P&L\n"
        "/portfolio_map — выбрать режим карты: «для себя» или «поделиться»\n"
        "/asset_lookup — цена инструмента и динамика (неделя/месяц/6 мес/год)\n"
        "/clear_portfolio — удалить все сделки и очистить портфель\n"
        "🚀 Рынок сегодня\n"
        "/top_movers — лидеры роста и падения за выбранную сессию\n"
        "🔔 Отчёты дня\n"
        "/trading_day_on — включить отчёт по итогам торгов (открытие/закрытие)\n"
        "/trading_day_off — выключить отчёт\n"
        "📥 Импорт сделок\n"
        "/import_broker_xml — загрузить XML брокерской выписки и импортировать сделки (Доступен только АльфаБанк)\n"
        "📚 Полезное\n"
        "/why_invest — зачем инвестировать и почему важна дисциплина\n",
        reply_markup=make_main_menu_kb(),
    )

async def cmd_set_interval(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return

    parts = (message.text or "").strip().split()
    if len(parts) != 2:
        await message.answer("Использование: /set_interval <минуты>, например /set_interval 30")
        return
    try:
        interval = int(parts[1])
        if interval < 1 or interval > 1440:
            raise ValueError
    except ValueError:
        await message.answer("Интервал должен быть целым числом от 1 до 1440 минут.")
        return

    await set_periodic_alert(DB_DSN, user_id, True, interval)
    await message.answer(f"Готово. Периодические уведомления включены: каждые {interval} мин.")


async def cmd_top_movers(message: Message):
    await message.answer(
        "Выбери дату для топа роста/падения:",
        reply_markup=await make_top_movers_dates_kb(selected=None),
    )


async def on_top_movers_date_pick(call: CallbackQuery):
    raw = (call.data or "").split(":", 1)[1] if ":" in (call.data or "") else ""
    try:
        selected = date.fromisoformat(raw)
    except ValueError:
        await call.answer("Некорректная дата", show_alert=True)
        return

    reset_data_source_flags()
    async with aiohttp.ClientSession() as session:
        movers = await get_stock_movers_by_date(session, selected, boardid="TQBR")

    if not movers:
        await safe_edit_text(
            call.message,
            f"Нет данных по акциям TQBR за {selected.strftime('%d.%m.%Y')}.",
            reply_markup=await make_top_movers_dates_kb(selected=selected),
        )
        await call.answer()
        return

    text = append_delayed_warning(build_top_movers_text(movers, selected))
    await safe_edit_text(
        call.message,
        text,
        reply_markup=await make_top_movers_dates_kb(selected=selected),
    )
    await call.answer()


async def make_clear_portfolio_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🗑️ Да, очистить", callback_data="pfclear:yes")
    kb.button(text="Отмена", callback_data="pfclear:no")
    kb.adjust(1)
    return kb.as_markup()

async def cmd_clear_portfolio(message: Message):
    await message.answer(
        "Это удалит все ваши сделки и обнулит портфель. Действие необратимо.\n"
        "Подтвердить очистку?",
        reply_markup=await make_clear_portfolio_kb(),
    )

async def on_clear_portfolio_confirm(call: CallbackQuery):
    user_id = call.from_user.id if call.from_user else None
    if not user_id:
        await call.answer("Не удалось определить пользователя", show_alert=True)
        return
    deleted = await clear_user_portfolio(DB_DSN, user_id)
    await call.message.edit_text(f"Портфель очищен. Удалено сделок: {deleted}.")
    await call.answer()

async def on_clear_portfolio_cancel(call: CallbackQuery):
    await call.message.edit_text("Очистка портфеля отменена.")
    await call.answer()


async def cmd_import_broker_xml(message: Message):
    await message.answer(
        "Пришлите XML выписку брокера (файл .xml), и я автоматически импортирую сделки в ваш портфель.\n"
        "Повторная загрузка той же выписки не продублирует уже импортированные сделки."
    )


async def on_broker_xml_document(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return

    doc = message.document
    if doc is None:
        return
    file_name = (doc.file_name or "").strip()
    file_name_l = file_name.lower()
    if not file_name_l.endswith(".xml"):
        await message.answer("Поддерживается только XML файл брокерской выписки.")
        return
    if doc.file_size and doc.file_size > MAX_BROKER_XML_SIZE_BYTES:
        await message.answer("Файл слишком большой. Максимальный размер — 5 МБ.")
        return

    progress = await message.answer("Загружаю и анализирую выписку...")
    try:
        tg_file = await message.bot.get_file(doc.file_id)
        buf = io.BytesIO()
        await message.bot.download_file(tg_file.file_path, destination=buf)
        summary = await _import_broker_xml_trades(user_id, file_name, buf.getvalue())
        await progress.edit_text(summary)
    except ValueError as exc:
        await progress.edit_text(f"Не удалось импортировать выписку: {exc}")
    except Exception:
        logger.exception("Failed to import broker XML user=%s file=%s", user_id, file_name)
        await progress.edit_text("Не удалось импортировать выписку из-за внутренней ошибки.")

async def cmd_interval_off(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await set_periodic_alert(DB_DSN, user_id, False, None)
    await message.answer("Периодические уведомления выключены.")

async def cmd_set_drop_alert(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return

    parts = (message.text or "").strip().split()
    if len(parts) != 2:
        await message.answer("Использование: /set_drop_alert <процент>, например /set_drop_alert 7.5")
        return
    try:
        percent = float(parts[1].replace(",", "."))
        if percent <= 0 or percent >= 100:
            raise ValueError
    except ValueError:
        await message.answer("Процент должен быть числом от 0 до 100, например 7.5")
        return

    await set_drop_alert(DB_DSN, user_id, True, percent)
    await message.answer(f"Готово. Алерт падения включен: при падении на {percent:g}% и более от вашей средней цены.")

async def cmd_drop_alert_off(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await set_drop_alert(DB_DSN, user_id, False, None)
    await message.answer("Алерт падения выключен.")

async def _set_trading_day_report_mode(message: Message, enabled: bool, reply_text: str) -> None:
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await set_open_close_alert(DB_DSN, user_id, enabled)
    await message.answer(reply_text)


async def cmd_market_reports_on(message: Message):
    await _set_trading_day_report_mode(message, True, "Отчеты на открытии и закрытии биржи включены (время МСК).")

async def cmd_market_reports_off(message: Message):
    await _set_trading_day_report_mode(message, False, "Отчеты на открытии и закрытии биржи выключены.")


async def cmd_trading_day_on(message: Message):
    await _set_trading_day_report_mode(
        message,
        True,
        "Дневной отчет включен.\n"
        "Я пришлю состояние портфеля в 4 точки по МСК:\n"
        "• открытие биржи\n"
        "• середина торгового дня\n"
        "• закрытие основной сессии\n"
        "• закрытие вечерней сессии"
    )


async def cmd_trading_day_off(message: Message):
    await _set_trading_day_report_mode(message, False, "Дневной отчет выключен.")

async def cmd_alerts_status(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await ensure_user_alert_settings(DB_DSN, user_id)
    s = await get_user_alert_settings(DB_DSN, user_id)
    text = (
        "Текущие настройки уведомлений:\n"
        f"Периодические: {'вкл' if s['periodic_enabled'] else 'выкл'}"
        f"{f', каждые {s['periodic_interval_min']} мин' if s['periodic_enabled'] else ''}\n"
        f"Алерт падения: {'вкл' if s['drop_alert_enabled'] else 'выкл'}"
        f"{f', порог {s['drop_percent']:g}%' if s['drop_alert_enabled'] else ''}\n"
        f"Открытие/закрытие биржи: {'вкл' if s['open_close_enabled'] else 'выкл'}"
    )
    await message.answer(text)

async def on_menu_add_trade(message: Message, state: FSMContext):
    await cmd_add_trade(message, state)

async def on_menu_portfolio(message: Message):
    await cmd_portfolio(message)

async def on_menu_portfolio_map(message: Message):
    await cmd_portfolio_map(message)

async def on_menu_alerts_status(message: Message):
    await cmd_alerts_status(message)

async def on_menu_top_movers(message: Message):
    await cmd_top_movers(message)

async def cmd_why_invest(message: Message):
    try:
        markup, items = await make_articles_kb()
    except Exception:
        logger.exception("Failed loading article list")
        markup, items = None, []

    if not items:
        try:
            text = await get_active_app_text(DB_DSN, "why_invest")
        except Exception:
            logger.exception("Failed loading fallback why_invest text from app_texts")
            text = None
        await message.answer(text or "Для раздела пока нет активных материалов.")
        return

    await message.answer("Выбери интересующую статью:", reply_markup=markup)


async def on_article_pick(call: CallbackQuery):
    text_code = (call.data or "").split(":", 1)[1] if ":" in (call.data or "") else ""
    if not text_code:
        await call.answer("Некорректный выбор", show_alert=True)
        return
    try:
        text = await get_active_app_text(DB_DSN, text_code)
    except Exception:
        logger.exception("Failed loading article text_code=%s", text_code)
        text = None
    if not text:
        await call.answer("Статья недоступна", show_alert=True)
        return
    await safe_edit_text(call.message, text)
    await call.answer()

async def on_menu_asset_lookup(message: Message, state: FSMContext):
    await cmd_asset_lookup(message, state)

async def cmd_asset_lookup(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AssetLookupFlow.waiting_asset_type)
    await message.answer("Выбери тип инструмента:", reply_markup=await make_lookup_asset_type_kb())

async def on_lookup_asset_type_pick(call: CallbackQuery, state: FSMContext):
    asset_type = call.data.split(":", 1)[1]
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
        await call.answer("Неизвестный тип инструмента", show_alert=True)
        return
    await state.update_data(asset_type=asset_type, cands=None)
    await state.set_state(AssetLookupFlow.waiting_query)
    if asset_type == ASSET_TYPE_METAL:
        text = "Введи тикер или название металла (например: GLDRUB_TOM):"
    else:
        text = "Введи тикер, ISIN или название компании:"
    await call.message.edit_text(text, reply_markup=await make_lookup_search_back_kb())
    await call.answer()

async def on_lookup_back_to_asset_type(call: CallbackQuery, state: FSMContext):
    await state.update_data(cands=None)
    await state.set_state(AssetLookupFlow.waiting_asset_type)
    await call.message.edit_text("Выбери тип инструмента:", reply_markup=await make_lookup_asset_type_kb())
    await call.answer()

async def on_lookup_back_to_query(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    asset_type = data.get("asset_type")
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
        await state.set_state(AssetLookupFlow.waiting_asset_type)
        await call.message.edit_text("Выбери тип инструмента:", reply_markup=await make_lookup_asset_type_kb())
        await call.answer()
        return
    await state.update_data(cands=None)
    await state.set_state(AssetLookupFlow.waiting_query)
    if asset_type == ASSET_TYPE_METAL:
        text = "Введи тикер или название металла (например: GLDRUB_TOM):"
    else:
        text = "Введи тикер, ISIN или название компании:"
    await call.message.edit_text(text, reply_markup=await make_lookup_search_back_kb())
    await call.answer()

async def on_lookup_query(message: Message, state: FSMContext):
    q = (message.text or "").strip()
    if not q:
        await message.answer("Введи запрос текстом.")
        return
    data = await state.get_data()
    asset_type = data.get("asset_type") or ASSET_TYPE_STOCK
    reset_data_source_flags()
    async with aiohttp.ClientSession() as session:
        if asset_type == ASSET_TYPE_METAL:
            cands = await search_metals(session, q)
        else:
            cands = await search_securities(session, q)
    if not cands:
        await message.answer("Ничего не найдено. Попробуй другой запрос или нажми «Назад».", reply_markup=await make_lookup_search_back_kb())
        return
    await state.update_data(cands=cands)
    await state.set_state(AssetLookupFlow.waiting_pick)
    await message.answer(append_delayed_warning("Выбери инструмент:"), reply_markup=await make_lookup_candidates_kb(cands))

async def on_lookup_pick(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cands = data.get("cands") or []
    try:
        idx = int(call.data.split(":")[1])
    except (TypeError, ValueError):
        await call.answer("Некорректный выбор", show_alert=True)
        return
    if idx < 0 or idx >= len(cands):
        await call.answer("Инструмент не найден", show_alert=True)
        return
    chosen = cands[idx]
    asset_type = data.get("asset_type") or ASSET_TYPE_STOCK
    text = await build_asset_dynamics_text(chosen, asset_type)
    await call.message.edit_text(text)
    await state.clear()
    await call.answer()

async def cmd_portfolio(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return

    text, _, positions = await build_portfolio_report(user_id)
    if not positions:
        await message.answer("Портфель пуст. Добавьте сделки через /add_trade.")
        return
    if len(text) <= 3500:
        await message.answer(text, parse_mode="HTML")
        return

    lines = text.splitlines()
    header = lines[0] if lines else "Портфель:"
    body_lines = lines[1:] if len(lines) > 1 else []
    await message.answer(header, parse_mode="HTML")
    chunk = []
    chunk_len = 0
    for line in body_lines:
        line_len = len(line) + 1
        if chunk_len + line_len > 3500 and chunk:
            await message.answer("\n".join(chunk), parse_mode="HTML")
            chunk = []
            chunk_len = 0
        chunk.append(line)
        chunk_len += line_len
    if chunk:
        await message.answer("\n".join(chunk), parse_mode="HTML")


async def _build_portfolio_map_rows(user_id: int) -> tuple[list[dict], int]:
    positions = await get_user_positions(DB_DSN, user_id)
    if not positions:
        return [], 0

    prices = await _load_prices_for_positions(positions)
    rows: list[dict] = []
    unknown_prices = 0
    for pos in positions:
        qty = float(pos.get("total_qty") or 0.0)
        if qty <= 1e-12:
            continue
        last = prices.get(int(pos["id"]))
        if last is None:
            unknown_prices += 1
            continue
        total_cost = float(pos.get("total_cost") or 0.0)
        value = qty * float(last)
        if value <= 0:
            continue
        pnl_pct = (value - total_cost) / total_cost * 100.0 if abs(total_cost) > 1e-12 else None
        rows.append(
            {
                "instrument_id": int(pos["id"]),
                "secid": str(pos.get("secid") or "").strip() or "UNKNOWN",
                "shortname": (pos.get("shortname") or "").strip(),
                "boardid": pos.get("boardid"),
                "asset_type": pos.get("asset_type") or ASSET_TYPE_STOCK,
                "qty": qty,
                "last": float(last),
                "value": float(value),
                "pnl_pct": pnl_pct,
            }
        )
    rows.sort(key=lambda x: float(x["value"]), reverse=True)
    return rows, unknown_prices


async def _compute_portfolio_return_30d(
    rows: list[dict],
) -> tuple[float | None, dict[int, float]]:
    if not rows:
        return None, {}
    from_date = datetime.now(MSK_TZ).date() - timedelta(days=30)
    till_date = datetime.now(MSK_TZ).date()
    sem = asyncio.Semaphore(PRICE_FETCH_CONCURRENCY)
    base_price_map: dict[int, float] = {}

    async with aiohttp.ClientSession() as session:
        async def load_base(row: dict) -> None:
            async with sem:
                try:
                    history = await get_history_prices_by_asset_type(
                        session,
                        secid=row["secid"],
                        boardid=row.get("boardid"),
                        asset_type=row.get("asset_type") or ASSET_TYPE_STOCK,
                        from_date=from_date,
                        till_date=till_date,
                    )
                    if history and history[0][1] > 0:
                        base_price_map[int(row["instrument_id"])] = float(history[0][1])
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
                    logger.warning("Failed loading 30d history for secid=%s", row.get("secid"))

        await asyncio.gather(*(load_base(row) for row in rows))

    base_total = 0.0
    current_total = 0.0
    for row in rows:
        iid = int(row["instrument_id"])
        base_price = base_price_map.get(iid)
        if base_price is None or base_price <= 0:
            continue
        qty = float(row["qty"])
        base_total += qty * base_price
        current_total += qty * float(row["last"])
    if base_total <= 1e-12:
        return None, base_price_map
    return (current_total - base_total) / base_total * 100.0, base_price_map


async def cmd_portfolio_map(message: Message):
    await message.answer("Выбери режим карты портфеля:", reply_markup=await make_portfolio_map_mode_kb())


async def on_portfolio_map_self(call: CallbackQuery):
    user_id = call.from_user.id if call.from_user else None
    if not user_id or call.message is None:
        await call.answer("Не удалось определить пользователя", show_alert=True)
        return

    reset_data_source_flags()
    rows, unknown_prices = await _build_portfolio_map_rows(user_id)
    if not rows:
        await safe_edit_text(call.message, "Нет рыночных данных по инструментам для построения карты.")
        await call.answer()
        return

    tiles = [
        {
            "secid": row["secid"],
            "shortname": row["shortname"],
            "value": row["value"],
            "weight": row["value"],
            "pnl_pct": row["pnl_pct"],
        }
        for row in rows
    ]
    image_bytes = await asyncio.to_thread(build_portfolio_map_png, tiles)
    caption = f"Карта портфеля ({len(tiles)} инструментов: акции и металлы)"
    if unknown_prices:
        caption += f"\nИнструментов без рыночной цены: {unknown_prices}"
    caption = append_delayed_warning(caption)
    await call.message.answer_document(
        document=BufferedInputFile(image_bytes, filename="portfolio_map.png"),
        caption=caption,
    )
    await call.answer()


async def on_portfolio_map_share(call: CallbackQuery):
    user_id = call.from_user.id if call.from_user else None
    if not user_id or call.message is None:
        await call.answer("Не удалось определить пользователя", show_alert=True)
        return

    reset_data_source_flags()
    rows, _ = await _build_portfolio_map_rows(user_id)
    if not rows:
        await safe_edit_text(call.message, "Нет данных для share-карточки. Добавьте сделки через /add_trade.")
        await call.answer()
        return

    total_value = sum(float(row["value"]) for row in rows)
    composition_rows = []
    for row in rows:
        share_pct = (float(row["value"]) / total_value * 100.0) if total_value > 0 else 0.0
        composition_rows.append({"instrument_id": int(row["instrument_id"]), "secid": row["secid"], "name_ru": row["shortname"], "share_pct": share_pct})

    top_gainers = sorted(
        [r for r in rows if r.get("pnl_pct") is not None],
        key=lambda x: float(x["pnl_pct"]),
        reverse=True,
    )[:3]
    top_losers = sorted(
        [r for r in rows if r.get("pnl_pct") is not None and float(r["pnl_pct"]) < 0],
        key=lambda x: float(x["pnl_pct"]),
    )[:3]

    portfolio_return_30d, base_price_map = await _compute_portfolio_return_30d(rows)
    rows_by_id = {int(row["instrument_id"]): row for row in rows}
    for item in composition_rows:
        iid = int(item["instrument_id"])
        row = rows_by_id.get(iid)
        if row is None:
            item["ret_30d"] = None
            continue
        base_price = base_price_map.get(iid)
        if base_price is None or base_price <= 0:
            item["ret_30d"] = None
            continue
        item["ret_30d"] = (float(row["last"]) - float(base_price)) / float(base_price) * 100.0
    composition_rows.sort(
        key=lambda x: float(x["ret_30d"]) if x.get("ret_30d") is not None else -10**9,
        reverse=True,
    )
    from_date = datetime.now(MSK_TZ).date() - timedelta(days=30)
    till_date = datetime.now(MSK_TZ).date()
    moex_return_30d = None
    try:
        async with aiohttp.ClientSession() as session:
            moex_return_30d = await get_moex_index_return_percent(session, from_date, till_date)
    except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
        logger.warning("Failed loading IMOEX return for share card")

    image_bytes = await asyncio.to_thread(
        build_portfolio_share_card_png,
        composition_rows=composition_rows,
        portfolio_return_30d=portfolio_return_30d,
        moex_return_30d=moex_return_30d,
        top_gainers=top_gainers,
        top_losers=top_losers,
    )
    caption = append_delayed_warning("Share-карточка портфеля (без раскрытия сумм)")
    await call.message.answer_document(
        document=BufferedInputFile(image_bytes, filename="portfolio_share_card.png"),
        caption=caption,
    )
    await call.answer()

def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None

async def process_user_alerts(bot: Bot, user_id: int, now_utc: datetime):
    settings = await get_user_alert_settings(DB_DSN, user_id)
    positions = await get_user_positions(DB_DSN, user_id)
    if not positions:
        return

    if settings["periodic_enabled"]:
        last = _parse_iso_utc(settings.get("periodic_last_sent_at"))
        due = (last is None) or ((now_utc - last).total_seconds() >= settings["periodic_interval_min"] * 60)
        if due:
            text, _, _ = await build_portfolio_report(user_id)
            await bot.send_message(user_id, f"Периодический отчет:\n\n{text}", parse_mode="HTML")
            await update_periodic_last_sent_at(DB_DSN, user_id, now_utc.isoformat())

    if settings["drop_alert_enabled"]:
        drop_percent = settings["drop_percent"]
        reset_data_source_flags()
        prices = await _load_prices_for_positions(positions)
        instrument_ids = [int(pos["id"]) for pos in positions]
        prev_state_map = await get_price_alert_states_bulk(DB_DSN, user_id, instrument_ids)
        state_updates: list[tuple[int, bool, str | None]] = []
        for pos in positions:
            avg = pos.get("avg_price") or 0.0
            if avg <= 0:
                continue
            instrument_id = int(pos["id"])
            last = prices.get(instrument_id)
            if last is None:
                continue
            threshold = avg * (1 - drop_percent / 100.0)
            is_below = last <= threshold
            prev_below = bool(prev_state_map.get(instrument_id, False))
            if is_below and not prev_below:
                fall_pct = (1 - (last / avg)) * 100
                company = pos.get("shortname") or pos["secid"]
                await bot.send_message(
                    user_id,
                    append_delayed_warning(
                        f"⚠️ Сильное падение цены\n"
                        f"{company} ({pos['secid']})\n"
                        f"Текущая цена: {money(last)} RUB\n"
                        f"Средняя цена: {money(avg)} RUB\n"
                        f"Падение: {fall_pct:.2f}% (порог {drop_percent:g}%)"
                    ),
                )
                state_updates.append((instrument_id, True, now_utc.isoformat()))
            elif (not is_below) and prev_below:
                state_updates.append((instrument_id, False, None))
        await set_price_alert_states_bulk(DB_DSN, user_id, state_updates)

    if settings["open_close_enabled"]:
        now_msk = now_utc.astimezone(MSK_TZ)
        if now_msk.weekday() < 5:
            today = now_msk.date().isoformat()
            now_min_of_day = now_msk.hour * 60 + now_msk.minute
            open_min_of_day = TRADING_DAY_OPEN_HOUR * 60 + TRADING_DAY_OPEN_MINUTE
            midday_min_of_day = TRADING_DAY_MIDDAY_HOUR * 60 + TRADING_DAY_MIDDAY_MINUTE
            main_close_hour, main_close_minute = get_trading_day_main_close_time(now_msk)
            main_close_min_of_day = main_close_hour * 60 + main_close_minute
            close_min_of_day = TRADING_DAY_EVENING_CLOSE_HOUR * 60 + TRADING_DAY_EVENING_CLOSE_MINUTE
            if (
                open_min_of_day <= now_min_of_day < open_min_of_day + MOEX_EVENT_WINDOW_MIN
                and settings.get("open_last_sent_date") != today
            ):
                text, open_value, _ = await build_portfolio_report(user_id)
                await bot.send_message(
                    user_id,
                    (
                        f"Открытие торгов (МСК):\n"
                        f"Баланс портфеля на открытии: <b>{money(open_value or 0.0)}</b> RUB\n\n"
                        f"{text}"
                    ),
                    parse_mode="HTML",
                )
                await update_open_sent_date(DB_DSN, user_id, today)
                await update_day_open_value(DB_DSN, user_id, today, open_value)
            if (
                midday_min_of_day <= now_min_of_day < midday_min_of_day + MOEX_EVENT_WINDOW_MIN
                and settings.get("midday_last_sent_date") != today
            ):
                text, midday_value, _ = await build_portfolio_report(user_id)
                await bot.send_message(
                    user_id,
                    (
                        f"Середина торгового дня (МСК):\n"
                        f"Баланс портфеля: <b>{money(midday_value or 0.0)}</b> RUB\n\n"
                        f"{text}"
                    ),
                    parse_mode="HTML",
                )
                await update_midday_sent_date(DB_DSN, user_id, today)
            if (
                main_close_min_of_day <= now_min_of_day < main_close_min_of_day + MOEX_EVENT_WINDOW_MIN
                and settings.get("main_close_last_sent_date") != today
            ):
                text, main_close_value, _ = await build_portfolio_report(user_id)
                await bot.send_message(
                    user_id,
                    (
                        f"Закрытие основной сессии (МСК):\n"
                        f"Баланс портфеля: <b>{money(main_close_value or 0.0)}</b> RUB\n\n"
                        f"{text}"
                    ),
                    parse_mode="HTML",
                )
                await update_main_close_sent_date(DB_DSN, user_id, today)
            if (
                close_min_of_day <= now_min_of_day < close_min_of_day + MOEX_EVENT_WINDOW_MIN
                and settings.get("close_last_sent_date") != today
            ):
                text, close_value, _ = await build_portfolio_report(user_id)
                open_value = settings.get("day_open_value")
                open_date = settings.get("day_open_value_date")
                if open_value is not None and open_date == today and close_value is not None:
                    day_pnl = close_value - float(open_value)
                    day_pnl_text = money_signed(day_pnl)
                    close_header = (
                        f"Закрытие вечерней сессии (МСК):\n"
                        f"Баланс на открытии: <b>{money(float(open_value))}</b> RUB\n"
                        f"Баланс на закрытии: <b>{money(close_value)}</b> RUB\n"
                        f"Результат за торговый день: <b>{day_pnl_text}</b> RUB\n\n"
                    )
                else:
                    close_header = (
                        f"Закрытие вечерней сессии (МСК):\n"
                        f"Баланс портфеля на закрытии: <b>{money(close_value or 0.0)}</b> RUB\n"
                        "Результат за торговый день: нет данных (не найден снимок открытия).\n\n"
                    )
                await bot.send_message(user_id, close_header + text, parse_mode="HTML")
                await update_close_sent_date(DB_DSN, user_id, today)

async def notifications_worker(bot: Bot):
    logger.info("Notifications worker started")
    while True:
        now_utc = datetime.now(timezone.utc)
        try:
            await refresh_price_cache_once()
            users = await list_users_with_alerts(DB_DSN)
            sem = asyncio.Semaphore(USER_ALERTS_CONCURRENCY)

            async def run_user(uid: int) -> None:
                async with sem:
                    try:
                        await process_user_alerts(bot, uid, now_utc)
                    except Exception:
                        logger.exception("Failed processing alerts user=%s", uid)

            await asyncio.gather(*(run_user(uid) for uid in users))
        except Exception:
            logger.exception("Notifications worker loop failed")
        await asyncio.sleep(60)

async def start_health_server():
    """
    Render web services expect the app to bind to $PORT.
    For Telegram long-polling bot we expose a tiny health endpoint.
    """
    port = os.getenv("PORT")
    if not port:
        return None

    async def healthz(_request: web.Request):
        return web.json_response({"ok": True, "service": "moex_portfolio_bot"})

    app = web.Application()
    app.router.add_get("/", healthz)
    app.router.add_get("/healthz", healthz)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=int(port))
    await site.start()
    logger.info("Health server started on port %s", port)
    return runner

async def cmd_add_trade(message: Message, state: FSMContext):
    logger.info("User %s started add_trade flow", message.from_user.id if message.from_user else None)
    await state.clear()
    await state.set_state(AddTradeFlow.waiting_date_mode)
    await message.answer("Выбери дату сделки:", reply_markup=await make_date_mode_kb())

async def on_date_mode_pick(call: CallbackQuery, state: FSMContext):
    mode = call.data.split(":", 1)[1]
    if mode == "today":
        d = today_ddmmyyyy()
        await state.update_data(trade_date=d)
        await state.set_state(AddTradeFlow.waiting_side)
        await call.message.edit_text(
            f"Дата сделки: {d}\n\nВыбери тип сделки:",
            reply_markup=await make_trade_side_kb(),
        )
    elif mode == "manual":
        await state.set_state(AddTradeFlow.waiting_date_manual)
        await call.message.edit_text("Введи дату сделки в формате dd.mm.yyyy (например: 08.02.2026):")
    else:
        await call.answer("Неизвестный выбор даты", show_alert=True)
        return
    await call.answer()

async def on_date_manual(message: Message, state: FSMContext):
    d = parse_ddmmyyyy(message.text or "")
    if d is None:
        await message.answer("Формат даты: dd.mm.yyyy. Пример: 08.02.2026")
        return
    await state.update_data(trade_date=d)
    await state.set_state(AddTradeFlow.waiting_side)
    await message.answer(f"Дата сделки: {d}\n\nВыбери тип сделки:", reply_markup=await make_trade_side_kb())

async def on_trade_side_pick(call: CallbackQuery, state: FSMContext):
    trade_side = call.data.split(":", 1)[1]
    if trade_side not in {TRADE_SIDE_BUY, TRADE_SIDE_SELL}:
        await call.answer("Неизвестный тип сделки", show_alert=True)
        return
    await state.update_data(trade_side=trade_side, asset_type=None, cands=None, chosen=None, qty=None, price=None)
    await state.set_state(AddTradeFlow.waiting_asset_type)
    side_label = "Покупка" if trade_side == TRADE_SIDE_BUY else "Продажа"
    await safe_edit_text(call.message, f"Тип сделки: {side_label}\n\nЧто добавляем?", reply_markup=await make_asset_type_kb())
    await call.answer()

async def on_back_to_side(call: CallbackQuery, state: FSMContext):
    await state.update_data(asset_type=None, cands=None, chosen=None, qty=None, price=None)
    await state.set_state(AddTradeFlow.waiting_side)
    await safe_edit_text(call.message, "Выбери тип сделки:", reply_markup=await make_trade_side_kb())
    await call.answer()

async def on_asset_type_pick(call: CallbackQuery, state: FSMContext):
    asset_type = call.data.split(":", 1)[1]
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
        await call.answer("Неизвестный тип инструмента", show_alert=True)
        return

    data = await state.get_data()
    trade_side = data.get("trade_side")
    if trade_side not in {TRADE_SIDE_BUY, TRADE_SIDE_SELL}:
        await state.set_state(AddTradeFlow.waiting_side)
        await safe_edit_text(call.message, "Сначала выбери тип сделки:", reply_markup=await make_trade_side_kb())
        await call.answer()
        return

    side_label = "Покупка" if trade_side == TRADE_SIDE_BUY else "Продажа"
    await state.update_data(asset_type=asset_type, cands=None, chosen=None, qty=None, price=None)

    if trade_side == TRADE_SIDE_SELL:
        user_id = call.from_user.id if call.from_user else None
        if not user_id:
            await call.answer("Не удалось определить пользователя", show_alert=True)
            return
        cands = await _load_sell_candidates(user_id, asset_type)
        if not cands:
            asset_label = "металлов" if asset_type == ASSET_TYPE_METAL else "акций"
            await state.set_state(AddTradeFlow.waiting_asset_type)
            await safe_edit_text(
                call.message,
                f"У вас нет позиций {asset_label} для продажи.\n\nВыберите другой тип актива:",
                reply_markup=await make_asset_type_kb(),
            )
            await call.answer()
            return
        await state.update_data(cands=cands)
        await state.set_state(AddTradeFlow.waiting_pick)
        if asset_type == ASSET_TYPE_METAL:
            prompt = f"Выбрано: {side_label}, Металл\n\nВыбери инструмент из текущего портфеля:"
        else:
            prompt = f"Выбрано: {side_label}, Акции\n\nВыбери инструмент из текущего портфеля:"
        await safe_edit_text(call.message, prompt, reply_markup=await make_candidates_kb(cands))
        await call.answer()
        return

    await state.set_state(AddTradeFlow.waiting_query)
    if asset_type == ASSET_TYPE_METAL:
        prompt = f"Выбрано: {side_label}, Металл\n\nВведи тикер или название металла (например: GLDRUB_TOM):"
    else:
        prompt = f"Выбрано: {side_label}, Акции\n\nВведи тикер, ISIN или название компании (например: SBER, RU0009029540, Сбербанк):"

    await safe_edit_text(call.message, prompt, reply_markup=await make_search_back_kb())
    await call.answer()

async def on_back_to_asset_type(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    trade_side = data.get("trade_side")
    if trade_side not in {TRADE_SIDE_BUY, TRADE_SIDE_SELL}:
        await state.set_state(AddTradeFlow.waiting_side)
        await safe_edit_text(call.message, "Сначала выбери тип сделки:", reply_markup=await make_trade_side_kb())
        await call.answer()
        return
    await state.update_data(cands=None, chosen=None)
    await state.set_state(AddTradeFlow.waiting_asset_type)
    await safe_edit_text(call.message, "Что добавляем?", reply_markup=await make_asset_type_kb())
    await call.answer()

async def on_back_to_query(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    asset_type = data.get("asset_type")
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
        await state.set_state(AddTradeFlow.waiting_asset_type)
        await call.message.edit_text("Что добавляем?", reply_markup=await make_asset_type_kb())
        await call.answer()
        return
    await state.update_data(cands=None, chosen=None)
    await state.set_state(AddTradeFlow.waiting_query)
    if asset_type == ASSET_TYPE_METAL:
        prompt = "Введи тикер или название металла (например: GLDRUB_TOM):"
    else:
        prompt = "Введи тикер, ISIN или название компании (например: SBER, RU0009029540, Сбербанк):"
    await call.message.edit_text(prompt, reply_markup=await make_search_back_kb())
    await call.answer()

async def on_back_to_instrument(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    asset_type = data.get("asset_type")
    trade_side = data.get("trade_side") or TRADE_SIDE_BUY
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
        await state.set_state(AddTradeFlow.waiting_asset_type)
        await call.message.edit_text("Сначала выбери тип актива:", reply_markup=await make_asset_type_kb())
        await call.answer()
        return

    if trade_side == TRADE_SIDE_SELL:
        user_id = call.from_user.id if call.from_user else None
        if not user_id:
            await call.answer("Не удалось определить пользователя", show_alert=True)
            return
        cands = await _load_sell_candidates(user_id, asset_type)
        if not cands:
            await state.update_data(cands=None, chosen=None, qty=None, price=None)
            await state.set_state(AddTradeFlow.waiting_asset_type)
            await safe_edit_text(call.message, "Позиции для продажи не найдены. Выберите тип актива:", reply_markup=await make_asset_type_kb())
            await call.answer()
            return
        await state.update_data(cands=cands, chosen=None, qty=None, price=None)
        await state.set_state(AddTradeFlow.waiting_pick)
        await safe_edit_text(call.message, "Выбери инструмент из текущего портфеля:", reply_markup=await make_candidates_kb(cands))
        await call.answer()
        return

    await state.update_data(cands=None, chosen=None, qty=None, price=None)
    await state.set_state(AddTradeFlow.waiting_query)
    if asset_type == ASSET_TYPE_METAL:
        prompt = "Введи тикер или название металла (например: GLDRUB_TOM):"
    else:
        prompt = "Введи тикер, ISIN или название компании (например: SBER, RU0009029540, Сбербанк):"
    await safe_edit_text(call.message, prompt, reply_markup=await make_search_back_kb())
    await call.answer()

async def on_back_to_qty(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    asset_type = data.get("asset_type") or ASSET_TYPE_STOCK
    if not data.get("chosen"):
        await state.set_state(AddTradeFlow.waiting_query)
        if asset_type == ASSET_TYPE_METAL:
            prompt = "Сначала выбери инструмент. Введи тикер или название металла:"
        else:
            prompt = "Сначала выбери инструмент. Введи тикер, ISIN или название компании:"
        await call.message.edit_text(prompt, reply_markup=await make_search_back_kb())
        await call.answer()
        return
    await state.update_data(price=None)
    await state.set_state(AddTradeFlow.waiting_qty)
    qty_prompt = "Введи количество граммов металла (например 5.5):" if asset_type == ASSET_TYPE_METAL else "Введи количество акций (например 10):"
    await call.message.edit_text(qty_prompt, reply_markup=await make_qty_back_kb())
    await call.answer()

async def on_query(message: Message, state: FSMContext):
    q = (message.text or "").strip()
    if not q:
        await message.answer("Введи тикер, ISIN или название компании текстом.")
        return

    data = await state.get_data()
    trade_side = data.get("trade_side") or TRADE_SIDE_BUY
    if trade_side == TRADE_SIDE_SELL:
        await message.answer("Для продажи выбери инструмент из текущего портфеля кнопками.")
        return
    asset_type = data.get("asset_type") or ASSET_TYPE_STOCK

    reset_data_source_flags()
    async with aiohttp.ClientSession() as session:
        if asset_type == ASSET_TYPE_METAL:
            cands = await search_metals(session, q)
        else:
            cands = await search_securities(session, q)

    if not cands:
        logger.info("Search returned no candidates for query=%r user=%s", q, message.from_user.id if message.from_user else None)
        await message.answer(
            "Ничего не нашёл. Попробуй другой запрос или нажми «Назад».",
            reply_markup=await make_search_back_kb(),
        )
        return

    logger.info("Search returned %s candidates for query=%r user=%s", len(cands), q, message.from_user.id if message.from_user else None)
    await state.update_data(cands=cands)
    await state.set_state(AddTradeFlow.waiting_pick)
    await message.answer(
        append_delayed_warning(
            "Нашёл варианты.\n"
            "Формат кнопки: Тикер - Название (режим торгов).\n"
            "Выбери нужный инструмент:"
        ),
        reply_markup=await make_candidates_kb(cands),
    )

async def on_pick(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cands = data.get("cands") or []
    try:
        idx = int(call.data.split(":")[1])
    except (TypeError, ValueError):
        await call.answer("Некорректный выбор", show_alert=True)
        return
    if idx < 0 or idx >= len(cands):
        await call.answer("Инструмент не найден в списке", show_alert=True)
        return
    chosen = cands[idx]
    logger.info("User %s picked %s (%s)", call.from_user.id if call.from_user else None, chosen["secid"], chosen.get("boardid"))
    await state.update_data(chosen=chosen, qty=None, price=None)
    await state.set_state(AddTradeFlow.waiting_qty)
    data = await state.get_data()
    asset_type = data.get("asset_type") or ASSET_TYPE_STOCK
    qty_prompt = "Введи количество граммов металла (например 5.5):" if asset_type == ASSET_TYPE_METAL else "Введи количество акций (например 10):"
    display_name = (chosen.get("shortname") or chosen.get("name") or "Не указано").strip()
    isin = chosen.get("isin") or "Не указано"
    board_ru = board_mode_ru(chosen.get("boardid"), asset_type)
    ticker = chosen.get("secid") or "Не указано"

    await call.message.edit_text(
        f"Выбрано:\n"
        f"Наименование: {display_name}\n"
        f"ISIN: {isin}\n"
        f"Режим торгов: {board_ru}\n"
        f"Тикер: {ticker}\n\n"
        f"{qty_prompt}",
        reply_markup=await make_qty_back_kb(),
    )
    await call.answer()

async def on_qty(message: Message, state: FSMContext):
    try:
        qty = float((message.text or "").replace(",", ".").strip())
        if qty <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Введите число > 0, например 10")
        return
    data = await state.get_data()
    trade_side = data.get("trade_side") or TRADE_SIDE_BUY
    if trade_side == TRADE_SIDE_SELL:
        chosen = data.get("chosen") or {}
        available_qty = float(chosen.get("available_qty") or 0.0)
        if qty - available_qty > 1e-12:
            unit = "гр" if (data.get("asset_type") == ASSET_TYPE_METAL) else "шт"
            await message.answer(f"Нельзя продать больше, чем есть в портфеле. Доступно: {available_qty:g} {unit}.")
            return
    signed_qty = -qty if trade_side == TRADE_SIDE_SELL else qty
    await state.update_data(qty=signed_qty, price=None)
    await state.set_state(AddTradeFlow.waiting_price)
    await message.answer("Введи стоимость одной единицы:", reply_markup=await make_price_back_kb())

async def on_price(message: Message, state: FSMContext):
    try:
        price = float((message.text or "").replace(",", ".").strip())
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Введите число > 0, например 285.4")
        return
    await state.update_data(price=price)
    data = await state.get_data()
    await state.set_state(AddTradeFlow.waiting_confirm)
    await message.answer(build_trade_preview(data), reply_markup=await make_confirm_kb())

async def on_confirm_save(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id if call.from_user else None
    if not user_id or call.message is None:
        await call.answer("Не удалось определить пользователя", show_alert=True)
        return

    data = await state.get_data()
    chosen = data["chosen"]
    asset_type = data.get("asset_type") or ASSET_TYPE_STOCK
    trade_date = data["trade_date"]
    qty = data["qty"]
    trade_side = data.get("trade_side") or TRADE_SIDE_BUY
    price = data["price"]
    commission = 0.0
    instrument_id = await upsert_instrument(
        DB_DSN,
        secid=chosen["secid"],
        isin=chosen.get("isin"),
        boardid=chosen.get("boardid"),
        shortname=chosen.get("shortname"),
        asset_type=asset_type,
    )
    if trade_side == TRADE_SIDE_SELL:
        total_qty_now, _, _ = await get_position_agg(DB_DSN, user_id, instrument_id)
        if abs(float(qty)) - float(total_qty_now) > 1e-12:
            qty_unit = "гр" if asset_type == ASSET_TYPE_METAL else "шт"
            await call.message.answer(
                f"Продажа отклонена: доступно только {total_qty_now:g} {qty_unit}, "
                f"а вы указали {abs(float(qty)):g} {qty_unit}."
            )
            await call.answer()
            return
    await add_trade(DB_DSN, user_id, instrument_id, trade_date, qty, price, commission)

    total_qty, total_cost, avg_price = await get_position_agg(DB_DSN, user_id, instrument_id)
    instr = await get_instrument(DB_DSN, instrument_id)
    logger.info(
        "Trade saved user=%s secid=%s qty=%s price=%s commission=%s",
        user_id,
        instr["secid"] if instr else None,
        qty,
        price,
        commission,
    )

    reset_data_source_flags()
    async with aiohttp.ClientSession() as session:
        last = await get_last_price_by_asset_type(
            session,
            instr["secid"],
            instr.get("boardid"),
            instr.get("asset_type") or ASSET_TYPE_STOCK,
        )

    if last is None:
        text_price = "Текущую цену не удалось получить."
    else:
        current_value = total_qty * last
        pnl = current_value - total_cost
        text_price = (
            f"Текущая цена: {money(last)} RUB\n"
            f"Текущая стоимость позиции: {money(current_value)} RUB\n"
            f"P&L: {money(pnl)} RUB"
        )
    text_price = append_delayed_warning(text_price)
    qty_unit = "гр" if (instr.get("asset_type") == ASSET_TYPE_METAL) else "шт"

    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Новая сделка (выбрать тикер)", callback_data="new_trade")
    kb.button(text="✅ Завершить ввод сделок", callback_data="done")
    kb.adjust(1)

    await state.set_state(AddTradeFlow.waiting_more)

    await call.message.answer(
        "Сделка сохранена ✅\n\n"
        f"{instr['secid']} ({instr.get('shortname') or ''})\n"
        f"Дата сделки: {trade_date}\n"
        f"Операция: {'Покупка' if trade_side == TRADE_SIDE_BUY else 'Продажа'}\n"
        f"Количество в сделке: {abs(float(qty)):g} {qty_unit}\n"
        f"Всего в позиции: {total_qty:g} {qty_unit}\n"
        f"Вложено: {money(total_cost)} RUB\n"
        f"Средняя цена: {money(avg_price)} RUB\n\n"
        f"{text_price}\n\n"
        "Добавим новую сделку или закончим ввод?",
        reply_markup=kb.as_markup()
    )
    await call.answer()

async def on_confirm_edit(call: CallbackQuery, state: FSMContext):
    await state.set_state(AddTradeFlow.waiting_edit_step)
    await call.message.edit_text(
        "С какого шага редактировать?",
        reply_markup=await make_edit_step_kb(),
    )
    await call.answer()

async def on_edit_step(call: CallbackQuery, state: FSMContext):
    step = call.data.split(":", 1)[1]
    data = await state.get_data()
    asset_type = data.get("asset_type")
    chosen = data.get("chosen")

    if step == "date":
        await state.update_data(trade_date=None, trade_side=None, asset_type=None, cands=None, chosen=None, qty=None, price=None)
        await state.set_state(AddTradeFlow.waiting_date_mode)
        await call.message.edit_text("Выбери дату сделки:", reply_markup=await make_date_mode_kb())
    elif step == "side":
        await state.update_data(trade_side=None, asset_type=None, cands=None, chosen=None, qty=None, price=None)
        await state.set_state(AddTradeFlow.waiting_side)
        await call.message.edit_text("Выбери тип сделки:", reply_markup=await make_trade_side_kb())
    elif step == "asset_type":
        await state.update_data(asset_type=None, cands=None, chosen=None, qty=None, price=None)
        await state.set_state(AddTradeFlow.waiting_asset_type)
        await call.message.edit_text("Что добавляем?", reply_markup=await make_asset_type_kb())
    elif step == "instrument":
        if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
            await state.update_data(asset_type=None, cands=None, chosen=None, qty=None, price=None)
            await state.set_state(AddTradeFlow.waiting_asset_type)
            await call.message.edit_text("Сначала выбери тип актива:", reply_markup=await make_asset_type_kb())
        else:
            trade_side = data.get("trade_side") or TRADE_SIDE_BUY
            if trade_side == TRADE_SIDE_SELL:
                user_id = call.from_user.id if call.from_user else None
                if not user_id:
                    await call.answer("Не удалось определить пользователя", show_alert=True)
                    return
                cands = await _load_sell_candidates(user_id, asset_type)
                if not cands:
                    await state.update_data(cands=None, chosen=None, qty=None, price=None)
                    await state.set_state(AddTradeFlow.waiting_asset_type)
                    await safe_edit_text(call.message, "Позиции для продажи не найдены. Выберите тип актива:", reply_markup=await make_asset_type_kb())
                else:
                    await state.update_data(cands=cands, chosen=None, qty=None, price=None)
                    await state.set_state(AddTradeFlow.waiting_pick)
                    await safe_edit_text(call.message, "Выбери инструмент из текущего портфеля:", reply_markup=await make_candidates_kb(cands))
            else:
                await state.update_data(cands=None, chosen=None, qty=None, price=None)
                await state.set_state(AddTradeFlow.waiting_query)
                if asset_type == ASSET_TYPE_METAL:
                    prompt = "Введи тикер или название металла (например: GLDRUB_TOM):"
                else:
                    prompt = "Введи тикер, ISIN или название компании (например: SBER, RU0009029540, Сбербанк):"
                await safe_edit_text(call.message, prompt, reply_markup=await make_search_back_kb())
    elif step == "qty":
        if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
            await state.update_data(asset_type=None, cands=None, chosen=None, qty=None, price=None)
            await state.set_state(AddTradeFlow.waiting_asset_type)
            await call.message.edit_text("Сначала выбери тип актива:", reply_markup=await make_asset_type_kb())
        elif not chosen:
            await state.set_state(AddTradeFlow.waiting_query)
            if asset_type == ASSET_TYPE_METAL:
                prompt = "Инструмент не выбран. Введи тикер или название металла:"
            else:
                prompt = "Инструмент не выбран. Введи тикер, ISIN или название компании:"
            await call.message.edit_text(prompt, reply_markup=await make_search_back_kb())
        else:
            await state.update_data(qty=None, price=None)
            await state.set_state(AddTradeFlow.waiting_qty)
            qty_prompt = "Введи количество граммов металла (например 5.5):" if asset_type == ASSET_TYPE_METAL else "Введи количество акций (например 10):"
            await call.message.edit_text(qty_prompt, reply_markup=await make_qty_back_kb())
    elif step == "price":
        if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
            await state.update_data(asset_type=None, cands=None, chosen=None, qty=None, price=None)
            await state.set_state(AddTradeFlow.waiting_asset_type)
            await call.message.edit_text("Сначала выбери тип актива:", reply_markup=await make_asset_type_kb())
        elif not chosen:
            await state.set_state(AddTradeFlow.waiting_query)
            if asset_type == ASSET_TYPE_METAL:
                prompt = "Инструмент не выбран. Введи тикер или название металла:"
            else:
                prompt = "Инструмент не выбран. Введи тикер, ISIN или название компании:"
            await call.message.edit_text(prompt, reply_markup=await make_search_back_kb())
        elif data.get("qty") is None:
            await state.set_state(AddTradeFlow.waiting_qty)
            qty_prompt = "Сначала введи количество граммов металла:" if asset_type == ASSET_TYPE_METAL else "Сначала введи количество акций:"
            await call.message.edit_text(qty_prompt)
        else:
            await state.update_data(price=None)
            await state.set_state(AddTradeFlow.waiting_price)
            await call.message.edit_text("Введи стоимость одной единицы:", reply_markup=await make_price_back_kb())
    else:
        await call.answer("Неизвестный шаг редактирования", show_alert=True)
        return
    await call.answer()

async def on_new_trade(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(AddTradeFlow.waiting_date_mode)
    await call.message.edit_text("Выбери дату сделки:", reply_markup=await make_date_mode_kb())
    await call.answer()

async def on_done(call: CallbackQuery, state: FSMContext):
    logger.info("User %s finished add_trade flow", call.from_user.id if call.from_user else None)
    await state.clear()
    await call.message.edit_text("Готово ✅ Можешь добавить другую бумагу: /add_trade")
    await call.answer()

async def main():
    if not BOT_TOKEN:
        raise RuntimeError(
            "Не найден токен бота в переменных окружения. "
            "Ожидается BOT_TOKEN или TELEGRAM_BOT_TOKEN (Render -> Environment)."
        )
    if not DB_DSN:
        raise RuntimeError("Не найден DATABASE_URL (PostgreSQL DSN) в .env")
    if not (DB_DSN.startswith("postgresql://") or DB_DSN.startswith("postgres://")):
        raise RuntimeError(
            "Неверный DATABASE_URL: ожидается PostgreSQL DSN, например "
            "postgresql://user:password@host:5432/database"
        )

    await init_db(DB_DSN)

    lock_name = "moex_portfolio_bot_polling"
    while True:
        locked = await acquire_single_instance_lock(DB_DSN, lock_name)
        if locked:
            logger.info("Acquired single-instance polling lock: %s", lock_name)
            break
        logger.warning("Another bot instance is polling. Waiting 15 seconds for lock: %s", lock_name)
        await asyncio.sleep(15)

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    worker_task = asyncio.create_task(notifications_worker(bot))
    health_runner = await start_health_server()

    dp.message.register(cmd_start, Command("start"), StateFilter("*"))
    dp.message.register(cmd_add_trade, Command("add_trade"), StateFilter("*"))
    dp.message.register(cmd_portfolio, Command("portfolio"), StateFilter("*"))
    dp.message.register(cmd_portfolio_map, Command("portfolio_map"), StateFilter("*"))
    dp.message.register(cmd_top_movers, Command("top_movers"), StateFilter("*"))
    dp.message.register(cmd_clear_portfolio, Command("clear_portfolio"), StateFilter("*"))
    dp.message.register(cmd_asset_lookup, Command("asset_lookup"), StateFilter("*"))
    dp.message.register(cmd_import_broker_xml, Command("import_broker_xml"), StateFilter("*"))
    dp.message.register(cmd_why_invest, Command("why_invest"), StateFilter("*"))
    dp.message.register(cmd_set_interval, Command("set_interval"), StateFilter("*"))
    dp.message.register(cmd_interval_off, Command("interval_off"), StateFilter("*"))
    dp.message.register(cmd_set_drop_alert, Command("set_drop_alert"), StateFilter("*"))
    dp.message.register(cmd_drop_alert_off, Command("drop_alert_off"), StateFilter("*"))
    dp.message.register(cmd_trading_day_on, Command("trading_day_on"), StateFilter("*"))
    dp.message.register(cmd_trading_day_off, Command("trading_day_off"), StateFilter("*"))
    dp.message.register(cmd_market_reports_on, Command("market_reports_on"), StateFilter("*"))
    dp.message.register(cmd_market_reports_off, Command("market_reports_off"), StateFilter("*"))
    dp.message.register(cmd_alerts_status, Command("alerts_status"), StateFilter("*"))
    dp.callback_query.register(on_top_movers_date_pick, StateFilter("*"), F.data.startswith("tmdate:"))
    dp.callback_query.register(on_portfolio_map_self, StateFilter("*"), F.data == CB_PORTFOLIO_MAP_SELF)
    dp.callback_query.register(on_portfolio_map_share, StateFilter("*"), F.data == CB_PORTFOLIO_MAP_SHARE)
    dp.message.register(on_menu_add_trade, StateFilter("*"), F.text == BTN_ADD_TRADE)
    dp.message.register(on_menu_portfolio, StateFilter("*"), F.text == BTN_PORTFOLIO)
    dp.message.register(on_menu_portfolio_map, StateFilter("*"), F.text == BTN_PORTFOLIO_MAP)
    dp.message.register(on_menu_alerts_status, StateFilter("*"), F.text == BTN_ALERTS)
    dp.message.register(on_menu_asset_lookup, StateFilter("*"), F.text == BTN_ASSET_LOOKUP)
    dp.message.register(on_menu_top_movers, StateFilter("*"), F.text == BTN_TOP_MOVERS)
    dp.message.register(cmd_why_invest, StateFilter("*"), F.text == BTN_WHY_INVEST)
    dp.message.register(on_broker_xml_document, StateFilter("*"), F.document)

    dp.callback_query.register(on_lookup_asset_type_pick, AssetLookupFlow.waiting_asset_type, F.data.startswith("latype:"))
    dp.callback_query.register(on_lookup_back_to_asset_type, AssetLookupFlow.waiting_query, F.data == "lback:asset_type")
    dp.callback_query.register(on_lookup_back_to_asset_type, AssetLookupFlow.waiting_pick, F.data == "lback:asset_type")
    dp.callback_query.register(on_lookup_back_to_query, AssetLookupFlow.waiting_pick, F.data == "lback:query")
    dp.message.register(on_lookup_query, AssetLookupFlow.waiting_query)
    dp.callback_query.register(on_lookup_pick, AssetLookupFlow.waiting_pick, F.data.startswith("lpick:"))
    dp.callback_query.register(on_article_pick, StateFilter("*"), F.data.startswith("article:"))

    dp.callback_query.register(on_trade_side_pick, AddTradeFlow.waiting_side, F.data.startswith("side:"))
    dp.callback_query.register(on_asset_type_pick, AddTradeFlow.waiting_asset_type, F.data.startswith("atype:"))
    dp.callback_query.register(on_date_mode_pick, AddTradeFlow.waiting_date_mode, F.data.startswith("date:"))
    dp.callback_query.register(on_back_to_side, AddTradeFlow.waiting_asset_type, F.data == "back:side")
    dp.callback_query.register(on_back_to_asset_type, AddTradeFlow.waiting_query, F.data == "back:asset_type")
    dp.callback_query.register(on_back_to_asset_type, AddTradeFlow.waiting_pick, F.data == "back:asset_type")
    dp.callback_query.register(on_back_to_query, AddTradeFlow.waiting_pick, F.data == "back:query")
    dp.callback_query.register(on_back_to_instrument, AddTradeFlow.waiting_qty, F.data == "back:instrument")
    dp.callback_query.register(on_back_to_qty, AddTradeFlow.waiting_price, F.data == "back:qty")
    dp.message.register(on_date_manual, AddTradeFlow.waiting_date_manual)
    dp.message.register(on_query, AddTradeFlow.waiting_query)
    dp.callback_query.register(on_pick, AddTradeFlow.waiting_pick, F.data.startswith("pick:"))
    dp.message.register(on_qty, AddTradeFlow.waiting_qty)
    dp.message.register(on_price, AddTradeFlow.waiting_price)
    dp.callback_query.register(on_confirm_save, AddTradeFlow.waiting_confirm, F.data == "confirm:save")
    dp.callback_query.register(on_confirm_edit, AddTradeFlow.waiting_confirm, F.data == "confirm:edit")
    dp.callback_query.register(on_edit_step, AddTradeFlow.waiting_edit_step, F.data.startswith("edit:"))
    dp.callback_query.register(on_clear_portfolio_confirm, StateFilter("*"), F.data == "pfclear:yes")
    dp.callback_query.register(on_clear_portfolio_cancel, StateFilter("*"), F.data == "pfclear:no")

    dp.callback_query.register(on_new_trade, AddTradeFlow.waiting_more, F.data == "new_trade")
    dp.callback_query.register(on_done, AddTradeFlow.waiting_more, F.data == "done")

    logger.info("Bot started polling")
    try:
        await dp.start_polling(bot)
    finally:
        await release_single_instance_lock()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        if health_runner is not None:
            await health_runner.cleanup()
        await close_pools()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        logger.exception("Bot crashed")
        raise
