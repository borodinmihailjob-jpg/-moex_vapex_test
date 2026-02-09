import os
import logging
import asyncio
import html
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import aiohttp
from aiohttp import web
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.types import CallbackQuery, Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from db import (
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
    update_close_sent_date,
    get_price_alert_state,
    set_price_alert_state,
)
from moex_iss import (
    ASSET_TYPE_METAL,
    ASSET_TYPE_STOCK,
    get_last_price_by_asset_type,
    search_metals,
    search_securities,
)

MSK_TZ = ZoneInfo("Europe/Moscow")
MOEX_OPEN_HOUR = 10
MOEX_OPEN_MINUTE = 0
MOEX_CLOSE_HOUR = 18
MOEX_CLOSE_MINUTE = 50
MOEX_EVENT_WINDOW_MIN = 5
BTN_ADD_TRADE = "Добавить сделку"
BTN_PORTFOLIO = "Стоимость портфеля"
BTN_ALERTS = "Настройки уведомлений"
BTN_WHY_INVEST = "Зачем инвестировать"

WHY_INVEST_TEXT = (
    "Зачем инвестировать? Чтобы деньги работали быстрее инфляции, и результат зависел не от "
    "«угадайки», а от дисциплины.\n\n"
    "📌 Пример на данных 22 лет (март 2003 → февраль 2025)\n"
    "Инструмент: индекс Мосбиржи полной доходности MCFTRR (дивиденды учтены, налоги на дивиденды тоже).\n"
    "Взносы: старт 2000 ₽/мес в 2003, каждый год рост на инфляцию → к 2025 около 12000 ₽/мес. "
    "Всего внесено около 1,54 млн ₽.\n\n"
    "База сравнения:\n"
    "• Средняя инфляция ~8,5%/год. Чтобы «просто сохранить покупательную способность», "
    "нужно было иметь ≈3,227 млн ₽ к концу периода.\n"
    "• Если все это время держать на вкладе (переоткрытие раз в 3 месяца): итог ≈3,971 млн ₽ (~9,5% годовых).\n\n"
    "Три инвестора (все с одинаковыми взносами):\n"
    "1. Худший тайминг («покупал на хаях» 8 раз: 2006, 2008, 2011, 2015, 2017, 2020, 2021, 2024) "
    "→ ≈3,5 млн ₽ (9,2% годовых). Даже так — выше инфляции, но хуже вклада.\n"
    "2. «Идеальный таймер» (ловил падения ≥30% и покупал «в самый низ», 5 входов: 2008, 2011, 2020, 2022, 2024) "
    "→ ≈5,8 млн ₽ (лучший, но так почти никто не умеет стабильно).\n"
    "3. Регулярные покупки каждый месяц («как зарплата → в портфель») → ≈5,16 млн ₽. "
    "Это сильно выше вклада и всего на ~12,5% хуже «идеального тайминга».\n\n"
    "✅ Вывод для обычного человека:\n"
    "Лучшее, что реально повторить — покупать регулярно и долго. «Угадать дно» почти невозможно, "
    "а дисциплина дает результат: 5,16 млн ₽ vs 3,97 млн ₽ на вкладе на одном и том же горизонте."
)

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

load_dotenv()
def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()

BOT_TOKEN = _env("BOT_TOKEN") or _env("TELEGRAM_BOT_TOKEN")
DB_DSN = _env("DATABASE_URL") or _env("DB_DSN") or _env("DB_PATH")

class AddTradeFlow(StatesGroup):
    waiting_date_mode = State()
    waiting_date_manual = State()
    waiting_asset_type = State()
    waiting_query = State()
    waiting_pick = State()
    waiting_qty = State()
    waiting_price = State()
    waiting_confirm = State()
    waiting_edit_step = State()
    waiting_more = State()

def money(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")

def money_signed(x: float) -> str:
    if x > 0:
        return f"+{money(x)}"
    if x < 0:
        return f"-{money(abs(x))}"
    return money(0.0)

async def make_candidates_kb(cands: list[dict]):
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
        kb.button(text=title[:64], callback_data=f"pick:{i}")
    kb.button(text="⬅️ Назад", callback_data="back:query")
    kb.adjust(1)
    return kb.as_markup()

async def make_asset_type_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Акции", callback_data=f"atype:{ASSET_TYPE_STOCK}")
    kb.button(text="🥇 Металл", callback_data=f"atype:{ASSET_TYPE_METAL}")
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
    kb.button(text="Тип актива", callback_data="edit:asset_type")
    kb.button(text="Инструмент", callback_data="edit:instrument")
    kb.button(text="Количество", callback_data="edit:qty")
    kb.button(text="Цена за единицу", callback_data="edit:price")
    kb.adjust(1)
    return kb.as_markup()

def make_main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ADD_TRADE), KeyboardButton(text=BTN_PORTFOLIO)],
            [KeyboardButton(text=BTN_ALERTS), KeyboardButton(text=BTN_WHY_INVEST)],
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
    qty_unit = "гр" if asset_type == ASSET_TYPE_METAL else "шт"
    qty = data["qty"]
    price = data["price"]
    total = qty * price
    return (
        "Проверь сделку:\n\n"
        f"Дата: {data['trade_date']}\n"
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

async def build_portfolio_report(user_id: int) -> tuple[str, float | None, list[dict]]:
    positions = await get_user_positions(DB_DSN, user_id)
    if not positions:
        return ("Портфель пуст.", None, [])

    async with aiohttp.ClientSession() as session:
        async def load_price(pos: dict):
            try:
                last = await get_last_price_by_asset_type(
                    session,
                    pos["secid"],
                    pos.get("boardid"),
                    pos.get("asset_type") or ASSET_TYPE_STOCK,
                )
                return pos, last
            except Exception:
                logger.exception("Failed to load price secid=%s boardid=%s", pos["secid"], pos.get("boardid"))
                return pos, None

        priced = await asyncio.gather(*(load_price(pos) for pos in positions))

    total_value_known = 0.0
    total_cost_known = 0.0
    unknown_prices = 0
    lines = []

    for pos, last in priced:
        qty = pos["total_qty"]
        ticker = str(pos["secid"]).strip()
        asset_name_raw = (pos.get("shortname") or ticker).strip()
        asset_name = html.escape(asset_name_raw)
        ticker_safe = html.escape(ticker)
        unit = "гр" if (pos.get("asset_type") == ASSET_TYPE_METAL) else "акции"
        total_cost = float(pos.get("total_cost") or 0.0)

        if last is None:
            unknown_prices += 1
            lines.append(f"{asset_name} - {ticker_safe} - {qty:g} {unit} - Общая стоимость актива: нет данных - P&L: нет данных")
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
            f"{asset_name} - {ticker_safe} - {qty:g} {unit} - Общая стоимость актива: <b>{money(value)}</b> RUB - P&L {pnl_tail}"
        )

    total_pnl = total_value_known - total_cost_known
    total_pnl_pct = (total_pnl / total_cost_known * 100.0) if abs(total_cost_known) > 1e-12 else None
    total_emoji = pnl_emoji(total_pnl)
    if total_pnl_pct is None:
        total_pnl_text = f"{total_emoji} <b>{money_signed(total_pnl)} RUB</b>"
    else:
        total_pnl_text = f"{total_emoji} {total_pnl_pct:+.2f}% <b>{money_signed(total_pnl)} RUB</b>"
    footer = (
        f"Итоговая стоимость активов по всем тикерам: <b>{money(total_value_known)}</b> RUB\n"
        f"P&L: {total_pnl_text}"
    )
    if unknown_prices:
        footer += f"\nНет рыночной цены для {unknown_prices} инструментов, они не включены в итог."

    text = "Портфель:\n" + "\n".join(lines) + "\n\n" + footer
    return (text, total_value_known, positions)

async def cmd_start(message: Message):
    logger.info("User %s started bot", message.from_user.id if message.from_user else None)
    await message.answer(
        "Привет! Это MVP портфельного бота.\n\n"
        "Команды:\n"
        "/add_trade — добавить сделку (дата → актив → инструмент → количество → цена)\n"
        "/portfolio — показать текущую стоимость портфеля\n"
        "/why_invest — зачем инвестировать (пример и сравнение)\n"
        "/set_interval <минуты> — периодические уведомления по портфелю\n"
        "/interval_off — выключить периодические уведомления\n"
        "/set_drop_alert <процент> — алерт при сильном падении цены\n"
        "/drop_alert_off — выключить алерт падения\n"
        "/market_reports_on — отчет на открытии и закрытии биржи\n"
        "/market_reports_off — выключить отчеты открытия/закрытия\n"
        "/alerts_status — показать текущие настройки уведомлений\n",
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
    except Exception:
        await message.answer("Интервал должен быть целым числом от 1 до 1440 минут.")
        return

    await set_periodic_alert(DB_DSN, user_id, True, interval)
    await message.answer(f"Готово. Периодические уведомления включены: каждые {interval} мин.")

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
    except Exception:
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

async def cmd_market_reports_on(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await set_open_close_alert(DB_DSN, user_id, True)
    await message.answer("Отчеты на открытии и закрытии биржи включены (время МСК).")

async def cmd_market_reports_off(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await set_open_close_alert(DB_DSN, user_id, False)
    await message.answer("Отчеты на открытии и закрытии биржи выключены.")

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

async def on_menu_alerts_status(message: Message):
    await cmd_alerts_status(message)

async def cmd_why_invest(message: Message):
    await message.answer(WHY_INVEST_TEXT)

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

async def build_portfolio_snapshot(user_id: int) -> tuple[str, float | None, list[dict]]:
    return await build_portfolio_report(user_id)

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
    except Exception:
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
            text, _, _ = await build_portfolio_snapshot(user_id)
            await bot.send_message(user_id, f"Периодический отчет:\n\n{text}", parse_mode="HTML")
            await update_periodic_last_sent_at(DB_DSN, user_id, now_utc.isoformat())

    if settings["drop_alert_enabled"]:
        drop_percent = settings["drop_percent"]
        async with aiohttp.ClientSession() as session:
            for pos in positions:
                avg = pos.get("avg_price") or 0.0
                if avg <= 0:
                    continue
                last = await get_last_price_by_asset_type(
                    session,
                    pos["secid"],
                    pos.get("boardid"),
                    pos.get("asset_type") or ASSET_TYPE_STOCK,
                )
                if last is None:
                    continue
                threshold = avg * (1 - drop_percent / 100.0)
                is_below = last <= threshold
                prev_below = await get_price_alert_state(DB_DSN, user_id, pos["id"])
                if is_below and not prev_below:
                    fall_pct = (1 - (last / avg)) * 100
                    company = pos.get("shortname") or pos["secid"]
                    await bot.send_message(
                        user_id,
                        (
                            f"⚠️ Сильное падение цены\n"
                            f"{company} ({pos['secid']})\n"
                            f"Текущая цена: {money(last)} RUB\n"
                            f"Средняя цена: {money(avg)} RUB\n"
                            f"Падение: {fall_pct:.2f}% (порог {drop_percent:g}%)"
                        ),
                    )
                    await set_price_alert_state(DB_DSN, user_id, pos["id"], True, now_utc.isoformat())
                elif (not is_below) and prev_below:
                    await set_price_alert_state(DB_DSN, user_id, pos["id"], False, None)

    if settings["open_close_enabled"]:
        now_msk = now_utc.astimezone(MSK_TZ)
        if now_msk.weekday() < 5:
            today = now_msk.date().isoformat()
            now_min_of_day = now_msk.hour * 60 + now_msk.minute
            open_min_of_day = MOEX_OPEN_HOUR * 60 + MOEX_OPEN_MINUTE
            close_min_of_day = MOEX_CLOSE_HOUR * 60 + MOEX_CLOSE_MINUTE
            if (
                open_min_of_day <= now_min_of_day < open_min_of_day + MOEX_EVENT_WINDOW_MIN
                and settings.get("open_last_sent_date") != today
            ):
                text, _, _ = await build_portfolio_snapshot(user_id)
                await bot.send_message(user_id, f"Открытие биржи (МСК):\n\n{text}", parse_mode="HTML")
                await update_open_sent_date(DB_DSN, user_id, today)
            if (
                close_min_of_day <= now_min_of_day < close_min_of_day + MOEX_EVENT_WINDOW_MIN
                and settings.get("close_last_sent_date") != today
            ):
                text, _, _ = await build_portfolio_snapshot(user_id)
                await bot.send_message(user_id, f"Закрытие биржи (МСК):\n\n{text}", parse_mode="HTML")
                await update_close_sent_date(DB_DSN, user_id, today)

async def notifications_worker(bot: Bot):
    logger.info("Notifications worker started")
    while True:
        now_utc = datetime.now(timezone.utc)
        try:
            users = await list_users_with_alerts(DB_DSN)
            for uid in users:
                try:
                    await process_user_alerts(bot, uid, now_utc)
                except Exception:
                    logger.exception("Failed processing alerts user=%s", uid)
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
        await state.set_state(AddTradeFlow.waiting_asset_type)
        await call.message.edit_text(
            f"Дата сделки: {d}\n\nЧто добавляем?",
            reply_markup=await make_asset_type_kb(),
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
    await state.set_state(AddTradeFlow.waiting_asset_type)
    await message.answer(f"Дата сделки: {d}\n\nЧто добавляем?", reply_markup=await make_asset_type_kb())

async def on_asset_type_pick(call: CallbackQuery, state: FSMContext):
    asset_type = call.data.split(":", 1)[1]
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
        await call.answer("Неизвестный тип инструмента", show_alert=True)
        return

    await state.update_data(asset_type=asset_type, cands=None, chosen=None, qty=None, price=None)
    await state.set_state(AddTradeFlow.waiting_query)
    if asset_type == ASSET_TYPE_METAL:
        prompt = "Выбрано: Металл\n\nВведи тикер или название металла (например: GLDRUB_TOM):"
    else:
        prompt = "Выбрано: Акции\n\nВведи тикер, ISIN или название компании (например: SBER, RU0009029540, Сбербанк):"

    await call.message.edit_text(prompt, reply_markup=await make_search_back_kb())
    await call.answer()

async def on_back_to_asset_type(call: CallbackQuery, state: FSMContext):
    await state.update_data(cands=None, chosen=None)
    await state.set_state(AddTradeFlow.waiting_asset_type)
    await call.message.edit_text("Что добавляем?", reply_markup=await make_asset_type_kb())
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
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
        await state.set_state(AddTradeFlow.waiting_asset_type)
        await call.message.edit_text("Сначала выбери тип актива:", reply_markup=await make_asset_type_kb())
        await call.answer()
        return
    await state.update_data(cands=None, chosen=None, qty=None, price=None)
    await state.set_state(AddTradeFlow.waiting_query)
    if asset_type == ASSET_TYPE_METAL:
        prompt = "Введи тикер или название металла (например: GLDRUB_TOM):"
    else:
        prompt = "Введи тикер, ISIN или название компании (например: SBER, RU0009029540, Сбербанк):"
    await call.message.edit_text(prompt, reply_markup=await make_search_back_kb())
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
    asset_type = data.get("asset_type") or ASSET_TYPE_STOCK

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
        "Нашёл варианты.\n"
        "Формат кнопки: Тикер - Название (режим торгов).\n"
        "Выбери нужный инструмент:",
        reply_markup=await make_candidates_kb(cands),
    )

async def on_pick(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cands = data.get("cands") or []
    try:
        idx = int(call.data.split(":")[1])
    except Exception:
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
    except Exception:
        await message.answer("Введите число > 0, например 10")
        return
    await state.update_data(qty=qty, price=None)
    await state.set_state(AddTradeFlow.waiting_price)
    await message.answer("Введи стоимость одной единицы:", reply_markup=await make_price_back_kb())

async def on_price(message: Message, state: FSMContext):
    try:
        price = float((message.text or "").replace(",", ".").strip())
        if price <= 0:
            raise ValueError
    except Exception:
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

    async with aiohttp.ClientSession() as session:
        last = await get_last_price_by_asset_type(
            session,
            instr["secid"],
            instr.get("boardid"),
            instr.get("asset_type") or ASSET_TYPE_STOCK,
        )

    if last is None:
        text_price = "Текущую цену не удалось получить (ISS)."
    else:
        current_value = total_qty * last
        pnl = current_value - total_cost
        text_price = (
            f"Текущая цена: {money(last)} RUB\n"
            f"Текущая стоимость позиции: {money(current_value)} RUB\n"
            f"P&L: {money(pnl)} RUB"
        )
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
        await state.update_data(trade_date=None, asset_type=None, cands=None, chosen=None, qty=None, price=None)
        await state.set_state(AddTradeFlow.waiting_date_mode)
        await call.message.edit_text("Выбери дату сделки:", reply_markup=await make_date_mode_kb())
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
            await state.update_data(cands=None, chosen=None, qty=None, price=None)
            await state.set_state(AddTradeFlow.waiting_query)
            if asset_type == ASSET_TYPE_METAL:
                prompt = "Введи тикер или название металла (например: GLDRUB_TOM):"
            else:
                prompt = "Введи тикер, ISIN или название компании (например: SBER, RU0009029540, Сбербанк):"
            await call.message.edit_text(prompt, reply_markup=await make_search_back_kb())
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

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    worker_task = asyncio.create_task(notifications_worker(bot))
    health_runner = await start_health_server()

    dp.message.register(cmd_start, Command("start"), StateFilter("*"))
    dp.message.register(cmd_add_trade, Command("add_trade"), StateFilter("*"))
    dp.message.register(cmd_portfolio, Command("portfolio"), StateFilter("*"))
    dp.message.register(cmd_why_invest, Command("why_invest"), StateFilter("*"))
    dp.message.register(cmd_set_interval, Command("set_interval"), StateFilter("*"))
    dp.message.register(cmd_interval_off, Command("interval_off"), StateFilter("*"))
    dp.message.register(cmd_set_drop_alert, Command("set_drop_alert"), StateFilter("*"))
    dp.message.register(cmd_drop_alert_off, Command("drop_alert_off"), StateFilter("*"))
    dp.message.register(cmd_market_reports_on, Command("market_reports_on"), StateFilter("*"))
    dp.message.register(cmd_market_reports_off, Command("market_reports_off"), StateFilter("*"))
    dp.message.register(cmd_alerts_status, Command("alerts_status"), StateFilter("*"))
    dp.message.register(on_menu_add_trade, StateFilter("*"), F.text == BTN_ADD_TRADE)
    dp.message.register(on_menu_portfolio, StateFilter("*"), F.text == BTN_PORTFOLIO)
    dp.message.register(on_menu_alerts_status, StateFilter("*"), F.text == BTN_ALERTS)
    dp.message.register(cmd_why_invest, StateFilter("*"), F.text == BTN_WHY_INVEST)

    dp.callback_query.register(on_asset_type_pick, AddTradeFlow.waiting_asset_type, F.data.startswith("atype:"))
    dp.callback_query.register(on_date_mode_pick, AddTradeFlow.waiting_date_mode, F.data.startswith("date:"))
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

    dp.callback_query.register(on_new_trade, AddTradeFlow.waiting_more, F.data == "new_trade")
    dp.callback_query.register(on_done, AddTradeFlow.waiting_more, F.data == "done")

    logger.info("Bot started polling")
    try:
        await dp.start_polling(bot)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        if health_runner is not None:
            await health_runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        logger.exception("Bot crashed")
        raise
