import os
import logging
import asyncio
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
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = os.getenv("DB_PATH", "portfolio.sqlite3")

class AddTradeFlow(StatesGroup):
    waiting_asset_type = State()
    waiting_query = State()
    waiting_pick = State()
    waiting_date = State()
    waiting_price = State()
    waiting_qty = State()
    waiting_more = State()

def money(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")

async def make_candidates_kb(cands: list[dict]):
    kb = InlineKeyboardBuilder()
    for i, c in enumerate(cands):
        display_name = c.get("shortname") or c.get("name") or ""
        title = f"{c['secid']} | {c.get('isin') or '-'} | {display_name} | {c.get('boardid') or ''}"
        kb.button(text=title[:64], callback_data=f"pick:{i}")
    kb.adjust(1)
    return kb.as_markup()

async def make_asset_type_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Акции", callback_data=f"atype:{ASSET_TYPE_STOCK}")
    kb.button(text="🥇 Металл", callback_data=f"atype:{ASSET_TYPE_METAL}")
    kb.adjust(1)
    return kb.as_markup()

def make_main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ADD_TRADE), KeyboardButton(text=BTN_PORTFOLIO)],
            [KeyboardButton(text=BTN_ALERTS)],
        ],
        resize_keyboard=True,
    )

def today_ddmmyyyy() -> str:
    return datetime.now(MSK_TZ).strftime("%d.%m.%Y")

async def cmd_start(message: Message):
    logger.info("User %s started bot", message.from_user.id if message.from_user else None)
    await message.answer(
        "Привет! Это MVP портфельного бота.\n\n"
        "Команды:\n"
        "/add_trade — добавить сделку (поиск по тикеру/ISIN/названию компании → цена → количество)\n"
        "/portfolio — показать текущую стоимость портфеля\n"
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

    await set_periodic_alert(DB_PATH, user_id, True, interval)
    await message.answer(f"Готово. Периодические уведомления включены: каждые {interval} мин.")

async def cmd_interval_off(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await set_periodic_alert(DB_PATH, user_id, False, None)
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

    await set_drop_alert(DB_PATH, user_id, True, percent)
    await message.answer(f"Готово. Алерт падения включен: при падении на {percent:g}% и более от вашей средней цены.")

async def cmd_drop_alert_off(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await set_drop_alert(DB_PATH, user_id, False, None)
    await message.answer("Алерт падения выключен.")

async def cmd_market_reports_on(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await set_open_close_alert(DB_PATH, user_id, True)
    await message.answer("Отчеты на открытии и закрытии биржи включены (время МСК).")

async def cmd_market_reports_off(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await set_open_close_alert(DB_PATH, user_id, False)
    await message.answer("Отчеты на открытии и закрытии биржи выключены.")

async def cmd_alerts_status(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return
    await ensure_user_alert_settings(DB_PATH, user_id)
    s = await get_user_alert_settings(DB_PATH, user_id)
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

async def cmd_portfolio(message: Message):
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        await message.answer("Не удалось определить пользователя.")
        return

    positions = await get_user_positions(DB_PATH, user_id)
    if not positions:
        await message.answer("Портфель пуст. Добавьте сделки через /add_trade.")
        return

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
    unknown_prices = 0
    lines = []

    for pos, last in priced:
        qty = pos["total_qty"]
        company = pos.get("shortname") or "Без названия"
        ticker = pos["secid"]
        unit = "гр" if (pos.get("asset_type") == ASSET_TYPE_METAL) else "шт"

        if last is None:
            unknown_prices += 1
            lines.append(
                f"{company} ({ticker}) — {qty:g} {unit} — стоимость актива: нет данных"
            )
            continue

        value = qty * last
        total_value_known += value
        lines.append(
            f"{company} ({ticker}) — {qty:g} {unit} — стоимость актива: {money(value)} RUB"
        )

    header = "Портфель:"
    footer = f"Итоговая стоимость активов по всем тикерам: {money(total_value_known)} RUB"

    if unknown_prices:
        footer += f"\nНет рыночной цены для {unknown_prices} инструментов, они не включены в итог."

    text = header + "\n" + "\n".join(lines) + "\n\n" + footer
    if len(text) <= 3500:
        await message.answer(text)
        return

    await message.answer(header)
    chunk = []
    chunk_len = 0
    for line in lines:
        line_len = len(line) + 1
        if chunk_len + line_len > 3500 and chunk:
            await message.answer("\n".join(chunk))
            chunk = []
            chunk_len = 0
        chunk.append(line)
        chunk_len += line_len
    if chunk:
        await message.answer("\n".join(chunk))
    await message.answer(footer)

async def build_portfolio_snapshot(user_id: int) -> tuple[str, float | None, list[dict]]:
    positions = await get_user_positions(DB_PATH, user_id)
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
    unknown_prices = 0
    lines = []
    for pos, last in priced:
        qty = pos["total_qty"]
        company = pos.get("shortname") or "Без названия"
        ticker = pos["secid"]
        unit = "гр" if (pos.get("asset_type") == ASSET_TYPE_METAL) else "шт"
        if last is None:
            unknown_prices += 1
            lines.append(f"{company} ({ticker}) — {qty:g} {unit} — цена: нет данных")
            continue
        value = qty * last
        total_value_known += value
        lines.append(f"{company} ({ticker}) — {qty:g} {unit} — стоимость: {money(value)} RUB")

    text = "Портфель:\n" + "\n".join(lines) + f"\n\nИтоговая стоимость: {money(total_value_known)} RUB"
    if unknown_prices:
        text += f"\nНет рыночной цены для {unknown_prices} инструментов, они не включены в итог."
    return (text, total_value_known, positions)

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
    settings = await get_user_alert_settings(DB_PATH, user_id)
    positions = await get_user_positions(DB_PATH, user_id)
    if not positions:
        return

    if settings["periodic_enabled"]:
        last = _parse_iso_utc(settings.get("periodic_last_sent_at"))
        due = (last is None) or ((now_utc - last).total_seconds() >= settings["periodic_interval_min"] * 60)
        if due:
            text, _, _ = await build_portfolio_snapshot(user_id)
            await bot.send_message(user_id, f"Периодический отчет:\n\n{text}")
            await update_periodic_last_sent_at(DB_PATH, user_id, now_utc.isoformat())

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
                prev_below = await get_price_alert_state(DB_PATH, user_id, pos["id"])
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
                    await set_price_alert_state(DB_PATH, user_id, pos["id"], True, now_utc.isoformat())
                elif (not is_below) and prev_below:
                    await set_price_alert_state(DB_PATH, user_id, pos["id"], False, None)

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
                await bot.send_message(user_id, f"Открытие биржи (МСК):\n\n{text}")
                await update_open_sent_date(DB_PATH, user_id, today)
            if (
                close_min_of_day <= now_min_of_day < close_min_of_day + MOEX_EVENT_WINDOW_MIN
                and settings.get("close_last_sent_date") != today
            ):
                text, _, _ = await build_portfolio_snapshot(user_id)
                await bot.send_message(user_id, f"Закрытие биржи (МСК):\n\n{text}")
                await update_close_sent_date(DB_PATH, user_id, today)

async def notifications_worker(bot: Bot):
    logger.info("Notifications worker started")
    while True:
        now_utc = datetime.now(timezone.utc)
        try:
            users = await list_users_with_alerts(DB_PATH)
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
    await state.set_state(AddTradeFlow.waiting_asset_type)
    await message.answer("Что добавляем?", reply_markup=await make_asset_type_kb())

async def on_asset_type_pick(call: CallbackQuery, state: FSMContext):
    asset_type = call.data.split(":", 1)[1]
    if asset_type not in {ASSET_TYPE_STOCK, ASSET_TYPE_METAL}:
        await call.answer("Неизвестный тип инструмента", show_alert=True)
        return

    await state.update_data(asset_type=asset_type)
    await state.set_state(AddTradeFlow.waiting_query)
    if asset_type == ASSET_TYPE_METAL:
        prompt = "Введи тикер или название металла (например: GLDRUB_TOM):"
    else:
        prompt = "Введи тикер, ISIN или название компании (например: SBER, RU0009029540, Сбербанк):"

    await call.message.edit_text(prompt)
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
        await message.answer("Ничего не нашёл. Попробуй другой тикер, ISIN или название компании.")
        return

    logger.info("Search returned %s candidates for query=%r user=%s", len(cands), q, message.from_user.id if message.from_user else None)
    await state.update_data(cands=cands)
    await state.set_state(AddTradeFlow.waiting_pick)
    await message.answer("Нашёл варианты. Выбери нужный:", reply_markup=await make_candidates_kb(cands))

async def on_pick(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cands = data["cands"]
    idx = int(call.data.split(":")[1])
    chosen = cands[idx]
    logger.info("User %s picked %s (%s)", call.from_user.id if call.from_user else None, chosen["secid"], chosen.get("boardid"))
    await state.update_data(chosen=chosen)

    await call.message.edit_text(
        f"Ок, выбрано:\n"
        f"SECID: {chosen['secid']}\n"
        f"ISIN: {chosen.get('isin')}\n"
        f"BOARD: {chosen.get('boardid')}\n"
        f"NAME: {chosen.get('shortname')}\n\n"
        f"Теперь введи дату сделки в формате dd.mm.yyyy (например: {today_ddmmyyyy()}):"
    )
    await state.set_state(AddTradeFlow.waiting_date)
    await call.answer()

async def on_date(message: Message, state: FSMContext):
    d = (message.text or "").strip()
    if len(d) != 10 or d[2] != "." or d[5] != ".":
        await message.answer("Формат даты: dd.mm.yyyy. Пример: 08.02.2026")
        return
    dd, mm, yyyy = d[:2], d[3:5], d[6:10]
    if not (dd.isdigit() and mm.isdigit() and yyyy.isdigit()):
        await message.answer("Формат даты: dd.mm.yyyy. Пример: 08.02.2026")
        return
    day, month, year = int(dd), int(mm), int(yyyy)
    if year < 1900 or month < 1 or month > 12 or day < 1 or day > 31:
        await message.answer("Некорректная дата. Используйте формат dd.mm.yyyy.")
        return

    await state.update_data(trade_date=d)
    await state.set_state(AddTradeFlow.waiting_price)
    await message.answer("Введи стоимость:")

async def on_price(message: Message, state: FSMContext):
    try:
        price = float((message.text or "").replace(",", ".").strip())
        if price <= 0:
            raise ValueError
    except Exception:
        await message.answer("Введите число > 0, например 285.4")
        return
    await state.update_data(price=price)
    await state.set_state(AddTradeFlow.waiting_qty)
    data = await state.get_data()
    asset_type = data.get("asset_type") or ASSET_TYPE_STOCK
    if asset_type == ASSET_TYPE_METAL:
        await message.answer("Введи количество граммов металла (например 5.5):")
    else:
        await message.answer("Введи количество акций (например 10):")

async def on_qty(message: Message, state: FSMContext):
    try:
        qty = float((message.text or "").replace(",", ".").strip())
        if qty <= 0:
            raise ValueError
    except Exception:
        await message.answer("Введите число > 0, например 10")
        return
    data = await state.get_data()
    chosen = data["chosen"]
    asset_type = data.get("asset_type") or ASSET_TYPE_STOCK
    trade_date = data["trade_date"]
    price = data["price"]
    commission = 0.0

    instrument_id = await upsert_instrument(
        DB_PATH,
        secid=chosen["secid"],
        isin=chosen.get("isin"),
        boardid=chosen.get("boardid"),
        shortname=chosen.get("shortname"),
        asset_type=asset_type,
    )
    await add_trade(DB_PATH, message.from_user.id, instrument_id, trade_date, qty, price, commission)

    total_qty, total_cost, avg_price = await get_position_agg(DB_PATH, message.from_user.id, instrument_id)
    instr = await get_instrument(DB_PATH, instrument_id)
    logger.info(
        "Trade saved user=%s secid=%s qty=%s price=%s commission=%s",
        message.from_user.id if message.from_user else None,
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

    await message.answer(
        "Сделка сохранена ✅\n\n"
        f"{instr['secid']} ({instr.get('shortname') or ''})\n"
        f"Дата сделки: {trade_date}\n"
        f"Всего в позиции: {total_qty:g} {qty_unit}\n"
        f"Вложено (с комиссиями): {money(total_cost)} RUB\n"
        f"Средняя цена (с комиссиями): {money(avg_price)} RUB\n\n"
        f"{text_price}\n\n"
        "Добавим новую сделку или закончим ввод?",
        reply_markup=kb.as_markup()
    )

async def on_new_trade(call: CallbackQuery, state: FSMContext):
    await state.set_state(AddTradeFlow.waiting_asset_type)
    await call.message.edit_text("Что добавляем?", reply_markup=await make_asset_type_kb())
    await call.answer()

async def on_done(call: CallbackQuery, state: FSMContext):
    logger.info("User %s finished add_trade flow", call.from_user.id if call.from_user else None)
    await state.clear()
    await call.message.edit_text("Готово ✅ Можешь добавить другую бумагу: /add_trade")
    await call.answer()

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не найден BOT_TOKEN в .env")

    await init_db(DB_PATH)

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    worker_task = asyncio.create_task(notifications_worker(bot))
    health_runner = await start_health_server()

    dp.message.register(cmd_start, Command("start"), StateFilter("*"))
    dp.message.register(cmd_add_trade, Command("add_trade"), StateFilter("*"))
    dp.message.register(cmd_portfolio, Command("portfolio"), StateFilter("*"))
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

    dp.callback_query.register(on_asset_type_pick, AddTradeFlow.waiting_asset_type, F.data.startswith("atype:"))
    dp.message.register(on_query, AddTradeFlow.waiting_query)
    dp.callback_query.register(on_pick, AddTradeFlow.waiting_pick, F.data.startswith("pick:"))

    dp.message.register(on_date, AddTradeFlow.waiting_date)
    dp.message.register(on_price, AddTradeFlow.waiting_price)
    dp.message.register(on_qty, AddTradeFlow.waiting_qty)

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
