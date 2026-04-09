import os
import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ================== НАСТРОЙКИ ==================
TOKEN = "8115168584:AAFQer8ixzAdmhZN3HB4HoWEjGPYXUbS618"

DATA_DIR = "/data"
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "piggybank.db")

TZ = ZoneInfo("Europe/Moscow")

AUTO_REPORT_DAY = 1
AUTO_REPORT_HOUR = 9
AUTO_REPORT_MINUTE = 0

REPORT_CHAT_ID = None
# ===============================================

print("DB_PATH =", DB_PATH)
print("DATA DIR EXISTS =", os.path.exists(DATA_DIR))
print("DB FILE EXISTS BEFORE START =", os.path.exists(DB_PATH))


@dataclass(frozen=True)
class ParsedAmount:
    cents: int


NUMBER_RE = re.compile(
    r"""^\s*
    (?P<num>[+-]?\d[\d\s\u00A0\u202F]*([.,]\d{1,2})?)
    \s*(?P<cur>₽|р\.?|руб\.?|рублей|рубля|руб)?\s*
    $""",
    re.IGNORECASE | re.VERBOSE,
)


def parse_amount_any(text: str) -> ParsedAmount | None:
    if not text:
        return None

    m = NUMBER_RE.match(text)
    if not m:
        return None

    raw = m.group("num")
    raw = raw.replace(" ", "").replace("\u00A0", "").replace("\u202F", "")
    raw = raw.replace(",", ".")

    try:
        val = float(raw)
    except ValueError:
        return None

    cents = int(round(val * 100))
    if cents == 0:
        return None

    return ParsedAmount(cents=cents)


def format_rub(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    rub = cents // 100
    kop = cents % 100
    rub_str = f"{rub:,}".replace(",", " ")
    return f"{sign}{rub_str},{kop:02d} ₽"


def prev_month(year: int, month: int) -> tuple[int, int]:
    return (year - 1, 12) if month == 1 else (year, month - 1)


def month_bounds_utc(year: int, month: int) -> tuple[str, str, str]:
    start_local = datetime(year, month, 1, 0, 0, 0, tzinfo=TZ)
    if month == 12:
        end_local = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=TZ)
    else:
        end_local = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=TZ)

    start_utc = start_local.astimezone(timezone.utc).isoformat()
    end_utc = end_local.astimezone(timezone.utc).isoformat()
    return start_utc, end_utc, start_local.strftime("%Y-%m")


# ================== БАЗА ДАННЫХ ==================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS tx (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                created_at_utc TEXT NOT NULL
            )
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_tx_chat_time ON tx(chat_id, created_at_utc)"
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                last_seen_utc TEXT NOT NULL
            )
            """
        )
        await db.commit()

    print("INIT_DB_DONE")
    print("DB FILE EXISTS AFTER INIT =", os.path.exists(DB_PATH))
    if os.path.exists(DB_PATH):
        print("DB FILE SIZE AFTER INIT =", os.path.getsize(DB_PATH))


async def touch_chat(chat_id: int):
    now_utc = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO chats(chat_id, last_seen_utc)
            VALUES(?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET last_seen_utc=excluded.last_seen_utc
            """,
            (chat_id, now_utc),
        )
        await db.commit()


async def add_tx(chat_id: int, user_id: int, user_name: str, amount_cents: int):
    now_utc = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO tx(chat_id, user_id, user_name, amount_cents, created_at_utc)
            VALUES(?,?,?,?,?)
            """,
            (chat_id, user_id, user_name, amount_cents, now_utc),
        )
        await db.commit()

    await touch_chat(chat_id)

    print(f"TX_SAVED chat_id={chat_id} user_id={user_id} amount={amount_cents}")
    print("DB FILE EXISTS AFTER TX =", os.path.exists(DB_PATH))
    if os.path.exists(DB_PATH):
        print("DB FILE SIZE AFTER TX =", os.path.getsize(DB_PATH))


async def get_total(chat_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COALESCE(SUM(amount_cents), 0) FROM tx WHERE chat_id=?",
            (chat_id,),
        ) as cur:
            row = await cur.fetchone()
            total = int(row[0] or 0)
            print(f"TOTAL_FOR_CHAT {chat_id} = {total}")
            return total


async def get_month_report(chat_id: int, year: int, month: int):
    start_utc, end_utc, title = month_bounds_utc(year, month)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT user_id, user_name, COALESCE(SUM(amount_cents),0) AS s
            FROM tx
            WHERE chat_id=?
              AND created_at_utc >= ?
              AND created_at_utc < ?
            GROUP BY user_id, user_name
            ORDER BY s DESC
            """,
            (chat_id, start_utc, end_utc),
        ) as cur:
            by_users = await cur.fetchall()

        async with db.execute(
            """
            SELECT COALESCE(SUM(amount_cents),0)
            FROM tx
            WHERE chat_id=?
              AND created_at_utc >= ?
              AND created_at_utc < ?
            """,
            (chat_id, start_utc, end_utc),
        ) as cur2:
            total_month = int((await cur2.fetchone())[0] or 0)

    return title, by_users, total_month


async def get_history(chat_id: int, limit: int = 20):
    limit = max(1, min(limit, 100))
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT user_name, amount_cents, created_at_utc
            FROM tx
            WHERE chat_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (chat_id, limit),
        ) as cur:
            rows = await cur.fetchall()

    out = []
    for user_name, amount_cents, created_at_utc in rows:
        dt_utc = datetime.fromisoformat(created_at_utc)
        dt_local = dt_utc.astimezone(TZ)
        out.append((user_name, int(amount_cents), dt_local))
    return out


async def get_report_chat_ids():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT chat_id FROM chats") as cur:
            rows = await cur.fetchall()
    return [int(r[0]) for r in rows]


async def get_all_time_report(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT user_id, user_name, COALESCE(SUM(amount_cents),0) AS s
            FROM tx
            WHERE chat_id=?
            GROUP BY user_id, user_name
            ORDER BY s DESC
            """,
            (chat_id,),
        ) as cur:
            rows = await cur.fetchall()

        async with db.execute(
            """
            SELECT COALESCE(SUM(amount_cents),0)
            FROM tx
            WHERE chat_id=?
            """,
            (chat_id,),
        ) as cur2:
            total_all = int((await cur2.fetchone())[0] or 0)

    return rows, total_all


# ================== ОТЧЁТЫ ==================
async def send_monthly_report(bot: Bot, chat_id: int, year: int, month: int):
    title, rows, total_month = await get_month_report(chat_id, year, month)
    total_now = await get_total(chat_id)

    if (not rows) and total_month == 0:
        text = (
            f"📅 Отчёт за *{title}*\n"
            f"Операций не было.\n\n"
            f"💰 Баланс копилки сейчас: *{format_rub(total_now)}*"
        )
        await bot.send_message(chat_id, text, parse_mode="Markdown")
        return

    lines = [f"📅 Отчёт за *{title}*"]
    for _, name, s in rows:
        lines.append(f"• {name}: *{format_rub(int(s))}*")
    lines.append(f"\nИтого за месяц: *{format_rub(total_month)}*")
    lines.append(f"💰 Баланс копилки сейчас: *{format_rub(total_now)}*")

    await bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown")


async def scheduled_monthly_job(bot: Bot):
    now_local = datetime.now(TZ)
    y, m = prev_month(now_local.year, now_local.month)
    targets = [REPORT_CHAT_ID] if REPORT_CHAT_ID is not None else await get_report_chat_ids()

    for cid in targets:
        if cid is None:
            continue
        try:
            await send_monthly_report(bot, cid, y, m)
        except Exception as e:
            print("MONTHLY_REPORT_ERROR:", repr(e))


# ================== БОТ ==================
async def main():
    await init_db()

    bot = Bot(TOKEN)
    dp = Dispatcher()

    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(
        scheduled_monthly_job,
        trigger=CronTrigger(
            day=AUTO_REPORT_DAY,
            hour=AUTO_REPORT_HOUR,
            minute=AUTO_REPORT_MINUTE,
        ),
        args=[bot],
        id="monthly_report",
        replace_existing=True,
    )
    scheduler.start()

    @dp.message(Command("total"))
    async def cmd_total(message: Message):
        total = await get_total(message.chat.id)
        await message.answer(
            f"💰 В копилке сейчас: *{format_rub(total)}*",
            parse_mode="Markdown",
        )

    @dp.message(Command("take"))
    async def cmd_take(message: Message):
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await message.reply(
                "Использование: `/take 500` или `/take 500,50`",
                parse_mode="Markdown",
            )
            return

        parsed = parse_amount_any(parts[1])
        if not parsed:
            await message.reply(
                "Не понял сумму. Пример: `/take 200` или `/take 1200,50`",
                parse_mode="Markdown",
            )
            return

        if parsed.cents <= 0:
            await message.reply(
                "Сумма для снятия должна быть положительной.",
                parse_mode="Markdown",
            )
            return

        u = message.from_user
        await add_tx(
            message.chat.id,
            u.id if u else 0,
            u.full_name if u else "Unknown",
            -parsed.cents,
        )

        total = await get_total(message.chat.id)
        await message.answer(
            f"➖ Снятие: *{format_rub(parsed.cents)}*\n"
            f"💰 Остаток в копилке: *{format_rub(total)}*",
            parse_mode="Markdown",
        )

    @dp.message(Command("month"))
    async def cmd_month(message: Message):
        now_local = datetime.now(TZ)
        year, month = now_local.year, now_local.month

        parts = (message.text or "").split(maxsplit=1)
        if len(parts) == 2:
            mm = re.match(r"^\s*(\d{4})-(\d{2})\s*$", parts[1])
            if not mm:
                await message.reply(
                    "Формат: `/month` или `/month 2026-03`",
                    parse_mode="Markdown",
                )
                return
            year = int(mm.group(1))
            month = int(mm.group(2))
            if not (1 <= month <= 12):
                await message.reply("Месяц должен быть 01..12")
                return

        await send_monthly_report(bot, message.chat.id, year, month)

    @dp.message(Command("history"))
    async def cmd_history(message: Message):
        parts = (message.text or "").split(maxsplit=1)
        limit = 20
        if len(parts) == 2:
            try:
                limit = int(parts[1].strip())
            except ValueError:
                limit = 20

        rows = await get_history(message.chat.id, limit)
        if not rows:
            await message.answer("История пуста.")
            return

        lines = [f"🧾 Последние {min(limit, 100)} операций:"]
        for user_name, amount_cents, dt_local in rows:
            sign = "➕" if amount_cents > 0 else "➖"
            lines.append(
                f"{dt_local:%d.%m %H:%M} — {sign} {format_rub(amount_cents)} — {user_name}"
            )
        lines.append(f"\n💰 Баланс: *{format_rub(await get_total(message.chat.id))}*")
        await message.answer("\n".join(lines), parse_mode="Markdown")

    @dp.message(Command("all"))
    async def cmd_all(message: Message):
        rows, total_all = await get_all_time_report(message.chat.id)
        if not rows:
            await message.answer("Пока нет операций за всё время.")
            return

        lines = ["📊 За всё время:"]
        for _, name, s in rows:
            lines.append(f"• {name}: *{format_rub(int(s))}*")
        lines.append(f"\nИтого: *{format_rub(total_all)}*")
        await message.answer("\n".join(lines), parse_mode="Markdown")

    @dp.message(Command("debugdb"))
    async def cmd_debugdb(message: Message):
        exists = os.path.exists(DB_PATH)
        size = os.path.getsize(DB_PATH) if exists else 0
        total = await get_total(message.chat.id)
        await message.answer(
            f"DB_PATH: `{DB_PATH}`\n"
            f"Exists: `{exists}`\n"
            f"Size: `{size}` bytes\n"
            f"Current total: `{total}`",
            parse_mode="Markdown",
        )

    @dp.message(Command("help"))
    async def cmd_help(message: Message):
        await message.answer(
            "Команды:\n"
            "• Написать сумму: `500`, `1500р`, `1 500,50`\n"
            "• /take 200 — снять\n"
            "• /total — баланс\n"
            "• /month или /month 2026-03 — отчёт за месяц\n"
            "• /history 20 — история\n"
            "• /all — вклад всех участников за всё время\n"
            "• /debugdb — диагностика базы\n\n"
            f"Автоотчёт: {AUTO_REPORT_DAY}-го числа в {AUTO_REPORT_HOUR:02d}:{AUTO_REPORT_MINUTE:02d} (МСК) за прошлый месяц.",
            parse_mode="Markdown",
        )

    @dp.message(F.text)
    async def on_text(message: Message):
        parsed = parse_amount_any(message.text)
        if not parsed:
            return

        if parsed.cents <= 0:
            return

        u = message.from_user
        await add_tx(
            message.chat.id,
            u.id if u else 0,
            u.full_name if u else "Unknown",
            parsed.cents,
        )
        total = await get_total(message.chat.id)

        await message.reply(
            f"➕ Добавлено: *{format_rub(parsed.cents)}*\n"
            f"💰 В копилке теперь: *{format_rub(total)}*",
            parse_mode="Markdown",
        )

    print("BOT_STARTING")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
