import os
import logging
import aiohttp
from pathlib import Path
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from db import init_db, upsert_instrument, add_trade, get_position_agg, get_instrument
from moex_iss import search_securities, get_last_price_stock_shares

LOG_PATH = Path(__file__).with_name("bot.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
    force=True,
)
logger = logging.getLogger("bot")
logger.info("Logging to %s", LOG_PATH)
logging.getLogger().info("Root logging initialized; writing to %s", LOG_PATH)
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_DSN = (os.getenv("DATABASE_URL") or os.getenv("DB_PATH") or "").strip()

class AddTradeFlow(StatesGroup):
    waiting_query = State()
    waiting_pick = State()
    waiting_date = State()
    waiting_price = State()
    waiting_qty = State()
    waiting_commission = State()
    waiting_more = State()

def money(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")

async def make_candidates_kb(cands: list[dict]):
    kb = InlineKeyboardBuilder()
    for i, c in enumerate(cands):
        title = f"{c['secid']} | {c.get('isin') or '-'} | {c.get('shortname') or ''} | {c.get('boardid') or ''}"
        kb.button(text=title[:64], callback_data=f"pick:{i}")
    kb.adjust(1)
    return kb.as_markup()

async def cmd_start(message: Message):
    logger.info("START user_id=%s", message.from_user.id)
    await message.answer(
        "Привет! Это MVP портфельного бота.\n\n"
        "Команды:\n"
        "/add_trade — добавить сделку (тикер/ISIN → выбор → дата/цена/кол-во/комиссия)\n"
    )

async def cmd_add_trade(message: Message, state: FSMContext):
    await state.set_state(AddTradeFlow.waiting_query)
    await message.answer("Введи тикер или ISIN (например: SBER или RU0009029540):")

async def on_query(message: Message, state: FSMContext):
    q = (message.text or "").strip()
    if not q:
        await message.answer("Введи тикер или ISIN текстом.")
        return

    async with aiohttp.ClientSession() as session:
        cands = await search_securities(session, q)

    if not cands:
        await message.answer("Ничего не нашёл. Попробуй другой тикер/ISIN.")
        return

    await state.update_data(cands=cands)
    await state.set_state(AddTradeFlow.waiting_pick)
    await message.answer("Нашёл варианты. Выбери нужный:", reply_markup=await make_candidates_kb(cands))

async def on_pick(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cands = data["cands"]
    idx = int(call.data.split(":")[1])
    chosen = cands[idx]
    await state.update_data(chosen=chosen)

    await call.message.edit_text(
        f"Ок, выбрано:\n"
        f"SECID: {chosen['secid']}\n"
        f"ISIN: {chosen.get('isin')}\n"
        f"BOARD: {chosen.get('boardid')}\n"
        f"NAME: {chosen.get('shortname')}\n\n"
        f"Теперь введи дату сделки (YYYY-MM-DD):"
    )
    await state.set_state(AddTradeFlow.waiting_date)
    await call.answer()

async def on_date(message: Message, state: FSMContext):
    d = (message.text or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        await message.answer("Формат даты: YYYY-MM-DD. Пример: 2025-12-20")
        return
    await state.update_data(trade_date=d)
    await state.set_state(AddTradeFlow.waiting_price)
    await message.answer("Введи цену 1 акции (например 285.4):")

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
    await message.answer("Введи количество акций (например 10):")

async def on_qty(message: Message, state: FSMContext):
    try:
        qty = float((message.text or "").replace(",", ".").strip())
        if qty <= 0:
            raise ValueError
    except Exception:
        await message.answer("Введите число > 0, например 10")
        return
    await state.update_data(qty=qty)
    await state.set_state(AddTradeFlow.waiting_commission)
    await message.answer("Введи комиссию в RUB (можно 0):")

async def on_commission(message: Message, state: FSMContext):
    try:
        commission = float((message.text or "").replace(",", ".").strip())
        if commission < 0:
            raise ValueError
    except Exception:
        await message.answer("Введите число >= 0, например 15.5")
        return

    data = await state.get_data()
    chosen = data["chosen"]
    trade_date = data["trade_date"]
    price = data["price"]
    qty = data["qty"]

    instrument_id = await upsert_instrument(
        DB_DSN,
        secid=chosen["secid"],
        isin=chosen.get("isin"),
        boardid=chosen.get("boardid"),
        shortname=chosen.get("shortname"),
    )
    await add_trade(DB_DSN, message.from_user.id, instrument_id, trade_date, qty, price, commission)

    total_qty, total_cost, avg_price = await get_position_agg(DB_DSN, message.from_user.id, instrument_id)
    instr = await get_instrument(DB_DSN, instrument_id)

    async with aiohttp.ClientSession() as session:
        last = await get_last_price_stock_shares(session, instr["secid"], instr.get("boardid"))

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

    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить ещё сделку по этому инструменту", callback_data=f"more:{instrument_id}")
    kb.button(text="✅ Закончить по этому инструменту", callback_data="done")
    kb.adjust(1)

    await state.set_state(AddTradeFlow.waiting_more)

    await message.answer(
        "Сделка сохранена ✅\n\n"
        f"{instr['secid']} ({instr.get('shortname') or ''})\n"
        f"Всего акций: {total_qty:g}\n"
        f"Вложено (с комиссиями): {money(total_cost)} RUB\n"
        f"Средняя цена (с комиссиями): {money(avg_price)} RUB\n\n"
        f"{text_price}\n\n"
        "Добавим ещё сделку или закончим?",
        reply_markup=kb.as_markup()
    )

async def on_more(call: CallbackQuery, state: FSMContext):
    instrument_id = int(call.data.split(":")[1])
    instr = await get_instrument(DB_DSN, instrument_id)

    await state.update_data(
        chosen={"secid": instr["secid"], "isin": instr.get("isin"), "boardid": instr.get("boardid"), "shortname": instr.get("shortname")}
    )
    await state.set_state(AddTradeFlow.waiting_date)
    await call.message.edit_text(f"Ок, добавляем ещё сделку по {instr['secid']}. Введи дату (YYYY-MM-DD):")
    await call.answer()

async def on_done(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("Готово ✅ Можешь добавить другую бумагу: /add_trade")
    await call.answer()

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не найден BOT_TOKEN в .env")
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

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_add_trade, Command("add_trade"))

    dp.message.register(on_query, AddTradeFlow.waiting_query)
    dp.callback_query.register(on_pick, AddTradeFlow.waiting_pick, F.data.startswith("pick:"))

    dp.message.register(on_date, AddTradeFlow.waiting_date)
    dp.message.register(on_price, AddTradeFlow.waiting_price)
    dp.message.register(on_qty, AddTradeFlow.waiting_qty)
    dp.message.register(on_commission, AddTradeFlow.waiting_commission)

    dp.callback_query.register(on_more, AddTradeFlow.waiting_more, F.data.startswith("more:"))
    dp.callback_query.register(on_done, AddTradeFlow.waiting_more, F.data == "done")

    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
