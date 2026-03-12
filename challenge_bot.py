"""
Челлендж боты — Telegram тобы үшін
Деректер сақтау: PostgreSQL (Railway)
"""

import logging
import os
import random
import psycopg2
import psycopg2.extras
from datetime import datetime, date, time as dtime, timedelta
from zoneinfo import ZoneInfo
from contextlib import contextmanager
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ─── Настройки ────────────────────────────────────────────────────────────────

BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
CHALLENGE_EMOJI = "✅"
FINE_AMOUNT = 1000
ASTANA_TZ = ZoneInfo("Asia/Almaty")  # UTC+5
UTC = ZoneInfo("UTC")

# ─── Логирование ──────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Цитаты ───────────────────────────────────────────────────────────────────

def load_quotes() -> list:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "quotes.txt")
    try:
        with open(path, encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        logger.info(f"Дәйексөздер жүктелді: {len(lines)} жол")
        return lines
    except FileNotFoundError:
        logger.warning("quotes.txt табылмады!")
        return []

QUOTES = load_quotes()

# ─── Приветствие ──────────────────────────────────────────────────────────────

def load_welcome() -> str:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "welcome.txt")
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        logger.warning("welcome.txt табылмады!")
        return "Ботқа қош келдіңіз! Мені топқа қосып, /start жіберіңіз."

WELCOME_TEXT = load_welcome()

# ─── База данных ──────────────────────────────────────────────────────────────

@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                chat_id     TEXT PRIMARY KEY,
                active      BOOLEAN NOT NULL DEFAULT TRUE,
                created_at  DATE NOT NULL DEFAULT CURRENT_DATE
            );
            CREATE TABLE IF NOT EXISTS members (
                chat_id     TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                name        TEXT NOT NULL,
                username    TEXT,
                joined_at   DATE NOT NULL DEFAULT CURRENT_DATE,
                PRIMARY KEY (chat_id, user_id),
                FOREIGN KEY (chat_id) REFERENCES groups(chat_id)
            );
            CREATE TABLE IF NOT EXISTS completions (
                chat_id     TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                day         DATE NOT NULL,
                done_at     TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY (chat_id, user_id, day),
                FOREIGN KEY (chat_id) REFERENCES groups(chat_id)
            );
            CREATE INDEX IF NOT EXISTS idx_completions_day
                ON completions(chat_id, day);
        """)
    logger.info("Дерекқор инициализацияланды.")


# ─── Вспомогательные функции ──────────────────────────────────────────────────

def now_astana():
    return datetime.now(ASTANA_TZ)

def today_str() -> str:
    return now_astana().date().isoformat()

def yesterday_str() -> str:
    return (now_astana().date() - timedelta(days=1)).isoformat()

def month_str(d: str = None) -> str:
    return (d or today_str())[:7]

def prev_month_str() -> str:
    first_of_this = now_astana().date().replace(day=1)
    last_of_prev = first_of_this - timedelta(days=1)
    return last_of_prev.strftime("%Y-%m")

def month_date_range(month: str):
    y, m = int(month[:4]), int(month[5:7])
    start = date(y, m, 1)
    end = date(y + 1, 1, 1) - timedelta(days=1) if m == 12 else date(y, m + 1, 1) - timedelta(days=1)
    return start, end

def ensure_group(cur, chat_id: str):
    cur.execute(
        "INSERT INTO groups (chat_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (chat_id,)
    )

def ensure_member(cur, chat_id: str, user_id: str, name: str, username: str):
    cur.execute(
        """INSERT INTO members (chat_id, user_id, name, username)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (chat_id, user_id) DO UPDATE SET name=EXCLUDED.name, username=EXCLUDED.username""",
        (chat_id, user_id, name, username)
    )

def get_members(cur, chat_id: str) -> list:
    cur.execute("SELECT user_id, name, username FROM members WHERE chat_id = %s", (chat_id,))
    return cur.fetchall()

def get_completions_for_day(cur, chat_id: str, day: str) -> set:
    cur.execute(
        "SELECT user_id FROM completions WHERE chat_id = %s AND day = %s",
        (chat_id, day)
    )
    return {r["user_id"] for r in cur.fetchall()}

def count_fines_for_month(cur, chat_id: str, user_id: str, month: str, up_to: date = None) -> int:
    cur.execute(
        "SELECT joined_at FROM members WHERE chat_id = %s AND user_id = %s",
        (chat_id, user_id)
    )
    row = cur.fetchone()
    if not row:
        return 0

    joined = row["joined_at"]
    if isinstance(joined, str):
        joined = date.fromisoformat(joined)

    month_start, month_end = month_date_range(month)

    if up_to is None:
        up_to = min(month_end, date.today() - timedelta(days=1))

    start = max(joined, month_start)
    end = min(month_end, up_to)

    if start > end:
        return 0

    cur.execute(
        """SELECT COUNT(*) as cnt FROM completions
           WHERE chat_id = %s AND user_id = %s AND day >= %s AND day <= %s""",
        (chat_id, user_id, start.isoformat(), end.isoformat())
    )
    done_count = cur.fetchone()["cnt"]
    return (end - start).days + 1 - done_count

def get_active_months(cur, chat_id: str) -> list:
    cur.execute(
        "SELECT DISTINCT TO_CHAR(day, 'YYYY-MM') as month FROM completions WHERE chat_id = %s ORDER BY month DESC",
        (chat_id,)
    )
    months = [r["month"] for r in cur.fetchall()]
    current = month_str()
    if current not in months:
        months.insert(0, current)
    return months

def get_streak(cur, chat_id: str, user_id: str) -> int:
    today = now_astana().date()
    streak = 0
    check_day = today
    while True:
        cur.execute(
            "SELECT 1 FROM completions WHERE chat_id = %s AND user_id = %s AND day = %s",
            (chat_id, user_id, check_day.isoformat())
        )
        if cur.fetchone():
            streak += 1
            check_day -= timedelta(days=1)
        else:
            break
    return streak


# ─── Отчёты ───────────────────────────────────────────────────────────────────

def build_daily_report(cur, chat_id: str, day: str) -> str:
    members = get_members(cur, chat_id)
    if not members:
        return None

    completed = get_completions_for_day(cur, chat_id, day)
    done_list, fined_list = [], []

    for m in members:
        if m["user_id"] in completed:
            streak = get_streak(cur, chat_id, m["user_id"])
            streak_text = f" ({streak}🔥)" if streak > 1 else ""
            done_list.append(f"✅ {m['name']}{streak_text}")
        else:
            fined_list.append(f"❌ {m['name']} (-{FINE_AMOUNT:,} тг)")

    text = f"🌙 *Күнделікті есеп — {day}*\n\n"
    if done_list:
        text += "🟢 *Орындады — Барак Аллах!*\n" + "\n".join(done_list) + "\n\n"
    if fined_list:
        text += "🔴 *Орындамады — штраф!*\n" + "\n".join(fined_list) + "\n\n"
    if not fined_list:
        text += "🌟 *Бәрі орындады! Машалла!*\n\n"

    current_month = month_str(day)
    today = now_astana().date()
    month_total = sum(
        count_fines_for_month(cur, chat_id, m["user_id"], current_month, up_to=today) * FINE_AMOUNT
        for m in members
    )
    text += f"💰 *{current_month} айының жиналған штрафы: {month_total:,} тг*"
    return text


def build_monthly_report(cur, chat_id: str, month: str) -> str:
    members = get_members(cur, chat_id)
    if not members:
        return None

    month_start, month_end = month_date_range(month)
    total_days = (month_end - month_start).days + 1
    rows = []
    total_fines = 0

    for m in members:
        fined_days = count_fines_for_month(cur, chat_id, m["user_id"], month, up_to=month_end)
        cur.execute(
            """SELECT COUNT(*) as cnt FROM completions
               WHERE chat_id = %s AND user_id = %s AND day >= %s AND day <= %s""",
            (chat_id, m["user_id"], month_start.isoformat(), month_end.isoformat())
        )
        done_count = cur.fetchone()["cnt"]
        amount = fined_days * FINE_AMOUNT
        total_fines += amount
        rows.append({"name": m["name"], "done": done_count, "fined_days": fined_days, "amount": amount})

    rows.sort(key=lambda r: r["fined_days"])

    text = f"📅 *{month} айының қорытынды есебі*\n"
    text += f"_(айдағы күндер саны: {total_days})_\n\n"

    for r in rows:
        medal = "🥇" if r["fined_days"] == 0 else "👤"
        text += (
            f"{medal} *{r['name']}*\n"
            f"   ✅ Орындалды: {r['done']} күн\n"
            f"   ❌ Өткізіп алды: {r['fined_days']} күн — {r['amount']:,} тг\n\n"
        )

    text += f"💰 *{month} айының жиналған штрафы: {total_fines:,} тг*"
    if total_fines == 0:
        text += "\n\n🌟 *Бәрі бүкіл айды орындады! Машалла!*"
    return text


def escape_md(text: str) -> str:
    """Экранирует спецсимволы Markdown v1."""
    for ch in ['_', '*', '[', '`']:
        text = text.replace(ch, f'\\{ch}')
    return text

def build_reminder_text(cur, chat_id: str) -> str | None:
    today = today_str()
    members = get_members(cur, chat_id)
    if not members:
        return None
    completed = get_completions_for_day(cur, chat_id, today)
    not_done = [m for m in members if m["user_id"] not in completed]
    if not not_done:
        return None
    # Используем username для тега, иначе просто имя — без Markdown чтобы не было ошибок парсинга
    mentions = " ".join(
        m["username"] if m["username"] and m["username"].startswith("@")
        else m["name"]
        for m in not_done
    )
    return (
        f"⏰ Еске салу!\n\n"
        f"{mentions}\n\n"
        f"Бүгінгі нормативті әлі орындамадыңдар!\n"
        f"📖 1 бет Құран\n"
        f"📚 1 бет рухани кітап\n"
        f"📿 1 бет Жаухарат\n"
        f"🤲 1 тасбихат\n"
        f"💚 100 салауат\n\n"
        f"Орындасаң — {CHALLENGE_EMOJI} жібер!"
    )


# ─── Команды ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text(WELCOME_TEXT, parse_mode="Markdown")
        return
    chat_id = str(update.effective_chat.id)
    with get_db() as conn:
        cur = conn.cursor()
        ensure_group(cur, chat_id)
    await update.message.reply_text(
        f"🌙 *Ассаламу алейкум!*\n\n"
        f"Челлендж боты дайын!\n\n"
        f"*Күнделікті норматив:*\n"
        f"📖 1 бет Құран\n"
        f"📚 1 бет рухани кітап\n"
        f"📿 1 бет Жаухарат\n"
        f"🤲 1 тасбихат\n"
        f"💚 100 салауат\n\n"
        f"*Орындасаң — жібер:* {CHALLENGE_EMOJI}\n"
        f"*Өткізіп алғаны үшін штраф:* {FINE_AMOUNT:,} тг\n\n"
        f"👥 Топтағы барлық жазған адам автоматты тіркеледі\n"
        f"👥 Барлығын тіркеу үшін: /addall",
        parse_mode="Markdown"
    )


async def cmd_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Команда тек топта жұмыс істейді!")
        return
    chat_id = str(update.effective_chat.id)
    user = update.effective_user
    user_id = str(user.id)
    name = user.full_name
    username = f"@{user.username}" if user.username else name
    with get_db() as conn:
        cur = conn.cursor()
        ensure_group(cur, chat_id)
        ensure_member(cur, chat_id, user_id, name, username)
    await update.message.reply_text(
        f"✅ *{name}* челленджге тіркелді!\n"
        f"Штраф алмас үшін күн сайын {CHALLENGE_EMOJI} жібер.",
        parse_mode="Markdown"
    )


async def cmd_addall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Команда тек топта жұмыс істейді!")
        return

    chat_id = str(update.effective_chat.id)
    registered = []

    try:
        admins = await context.bot.get_chat_administrators(int(chat_id))
        with get_db() as conn:
            cur = conn.cursor()
            ensure_group(cur, chat_id)
            for admin in admins:
                user = admin.user
                if user.is_bot:
                    continue
                user_id = str(user.id)
                name = user.full_name
                username = f"@{user.username}" if user.username else name
                ensure_member(cur, chat_id, user_id, name, username)
                registered.append(name)
    except Exception as e:
        logger.error(f"cmd_addall қатесі: {e}")
        await update.message.reply_text(f"Қате орын алды: {e}")
        return

    text = "👥 *Тіркелді:*\n"
    for name in registered:
        text += f"✅ {name}\n"
    text += (
        f"\n📢 Тізімде жоқтар — өздері `/register` деп жазсын!\n"
        f"_(Бот тек әкімшілерді автоматты таниды)_"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    today = today_str()
    with get_db() as conn:
        cur = conn.cursor()
        members = get_members(cur, chat_id)
        if not members:
            await update.message.reply_text("Тіркелген қатысушылар жоқ. /register деп жазыңыз")
            return
        completed = get_completions_for_day(cur, chat_id, today)
        done_list, not_done_list = [], []
        for m in members:
            if m["user_id"] in completed:
                streak = get_streak(cur, chat_id, m["user_id"])
                streak_text = f" ({streak}🔥)" if streak > 1 else ""
                done_list.append(f"✅ {m['name']}{streak_text}")
            else:
                not_done_list.append(f"❌ {m['name']}")

    text = f"📊 *Бүгінгі күн — {today}*\n\n"
    if done_list:
        text += "🟢 *Орындады:*\n" + "\n".join(done_list) + "\n\n"
    if not_done_list:
        text += "🔴 *Әлі белгілемеді:*\n" + "\n".join(not_done_list)
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_fines(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    current_month = month_str()
    with get_db() as conn:
        cur = conn.cursor()
        members = get_members(cur, chat_id)
        if not members:
            await update.message.reply_text("Тіркелген қатысушылар жоқ.")
            return
        rows = []
        total = 0
        for m in members:
            days = count_fines_for_month(cur, chat_id, m["user_id"], current_month)
            amount = days * FINE_AMOUNT
            total += amount
            rows.append((amount, f"👤 {m['name']}: *{days} күн* — {amount:,} тг"))

    rows.sort(reverse=True)
    text = f"💸 *{current_month} айының штрафтары*\n\n"
    text += "\n".join(r[1] for r in rows)
    text += f"\n\n💰 *Жиналған штраф: {total:,} тг*"
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    with get_db() as conn:
        cur = conn.cursor()
        members = get_members(cur, chat_id)
        if not members:
            await update.message.reply_text("Тіркелген қатысушылар жоқ.")
            return
        months = get_active_months(cur, chat_id)
        text = "📅 *Айлық штраф тарихы*\n\n"
        for month in months:
            _, month_end = month_date_range(month)
            effective_end = min(month_end, date.today() - timedelta(days=1))
            month_total = 0
            lines = []
            for m in members:
                days = count_fines_for_month(cur, chat_id, m["user_id"], month, up_to=effective_end)
                if days > 0:
                    amount = days * FINE_AMOUNT
                    month_total += amount
                    lines.append(f"  • {m['name']}: {days} күн → {amount:,} тг")
            text += f"*{month}:*\n"
            if lines:
                text += "\n".join(lines) + f"\n  *Жиыны: {month_total:,} тг*\n\n"
            else:
                text += "  Бәрі орындады! 🌟\n\n"

    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not QUOTES:
        await update.message.reply_text("quotes.txt бос немесе табылмады.")
        return
    quote = random.choice(QUOTES)
    text = f"🌅 *Таңғы дәйексөз*\n\n✨ {quote}\n\nБүгінгі нормативіңді орындауды ұмытпа! 💚"
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Команда тек топта жұмыс істейді!")
        return
    chat_id = str(update.effective_chat.id)
    user_id = update.effective_user.id
    member = await context.bot.get_chat_member(int(chat_id), user_id)
    if member.status not in ("administrator", "creator"):
        await update.message.reply_text("⛔ Бұл команда тек әкімшілерге қол жетімді!")
        return
    today = today_str()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE members SET joined_at = %s WHERE chat_id = %s",
            (today, chat_id)
        )
        cur.execute("SELECT COUNT(*) as cnt FROM members WHERE chat_id = %s", (chat_id,))
        count = cur.fetchone()["cnt"]
    await update.message.reply_text(
        f"🔄 *Штрафтар тазаланды!*\n\n"
        f"Топтағы {count} қатысушының тіркелу күні *{today}* деп жаңартылды.\n"
        f"Бұрынғы штрафтар есепке алынбайды.",
        parse_mode="Markdown"
    )


async def cmd_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовая команда — сразу отправляет напоминание."""
    if update.effective_chat.type == "private":
        await update.message.reply_text("Команда тек топта жұмыс істейді!")
        return
    chat_id = str(update.effective_chat.id)
    with get_db() as conn:
        cur = conn.cursor()
        text = build_reminder_text(cur, chat_id)
    if text:
        await update.message.reply_text(text)
    else:
        await update.message.reply_text("🌟 Бәрі орындады, еске салу жоқ!")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🤖 *Бот командалары:*\n\n"
        "`/start` — Ботты топта іске қосу\n"
        "`/register` — Челленджге тіркелу\n"
        "`/addall` — Барлық әкімшілерді тіркеу\n"
        f"`{CHALLENGE_EMOJI}` — Нормативті белгілеу\n"
        "`/status` — Бүгін кім орындады\n"
        "`/fines` — Ағымдағы ай штрафтары\n"
        "`/history` — Айлық штраф тарихы\n"
        "`/daily` — Таңғы дәйексөз\n"
        "`/notify` — Еске салуды қазір жіберу (тест)\n"
        "`/reset` — Барлықтың штрафтарын тазалау (айды қайта бастау)\n"
        "`/help` — Бұл анықтама\n"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


# ─── Обработчики сообщений ────────────────────────────────────────────────────

async def handle_sticker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or update.effective_chat.type == "private":
        return

    chat_id = str(update.effective_chat.id)
    user = update.effective_user
    if not user or user.is_bot:
        return

    user_id = str(user.id)
    name = user.full_name
    username = f"@{user.username}" if user.username else name

    sticker = msg.sticker
    logger.info(f"Стикер: {name}, emoji={sticker.emoji}, set={sticker.set_name}")

    with get_db() as conn:
        cur = conn.cursor()
        ensure_group(cur, chat_id)
        ensure_member(cur, chat_id, user_id, name, username)

    if sticker.emoji == "✅":
        await _mark_completion(update, chat_id, user_id, name)


async def handle_any_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text:
        return

    chat_id = str(update.effective_chat.id)
    user = update.effective_user
    if not user or user.is_bot:
        return

    user_id = str(user.id)
    name = user.full_name
    username = f"@{user.username}" if user.username else name

    logger.info(f"Хабар {name} ({update.effective_chat.type}): '{msg.text[:50]}'")

    if update.effective_chat.type == "private":
        return

    with get_db() as conn:
        cur = conn.cursor()
        ensure_group(cur, chat_id)
        cur.execute(
            "SELECT 1 FROM members WHERE chat_id = %s AND user_id = %s",
            (chat_id, user_id)
        )
        existing = cur.fetchone()
        ensure_member(cur, chat_id, user_id, name, username)

    if not existing:
        logger.info(f"Авто-тіркелді: {name} ({user_id}) in {chat_id}")

    if CHALLENGE_EMOJI in msg.text:
        await _mark_completion(update, chat_id, user_id, name)


async def _mark_completion(update: Update, chat_id: str, user_id: str, name: str):
    today = today_str()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM completions WHERE chat_id = %s AND user_id = %s AND day = %s",
            (chat_id, user_id, today)
        )
        if cur.fetchone():
            return
        cur.execute(
            "INSERT INTO completions (chat_id, user_id, day) VALUES (%s, %s, %s)",
            (chat_id, user_id, today)
        )
    logger.info(f"Белгіленді: {name} ({user_id}) — {today}")


# ─── Автоматические задания ───────────────────────────────────────────────────

async def job_daily_report(context: ContextTypes.DEFAULT_TYPE):
    yesterday = yesterday_str()
    logger.info(f"Күнделікті есеп: {yesterday}")
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM groups WHERE active = TRUE")
        groups = cur.fetchall()
        for group in groups:
            chat_id = group["chat_id"]
            text = build_daily_report(cur, chat_id, yesterday)
            if not text:
                continue
            try:
                await context.bot.send_message(chat_id=int(chat_id), text=text, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Күнделікті есеп қатесі {chat_id}: {e}")


async def job_monthly_report(context: ContextTypes.DEFAULT_TYPE):
    prev_month = prev_month_str()
    logger.info(f"Ай есебі: {prev_month}")
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM groups WHERE active = TRUE")
        groups = cur.fetchall()
        for group in groups:
            chat_id = group["chat_id"]
            text = build_monthly_report(cur, chat_id, prev_month)
            if not text:
                continue
            try:
                await context.bot.send_message(chat_id=int(chat_id), text=text, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Ай есебі қатесі {chat_id}: {e}")


async def job_reminder(context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Еске салу: {today_str()}")
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM groups WHERE active = TRUE")
        groups = cur.fetchall()
        for group in groups:
            chat_id = group["chat_id"]
            text = build_reminder_text(cur, chat_id)
            if not text:
                continue
            try:
                await context.bot.send_message(chat_id=int(chat_id), text=text)
            except Exception as e:
                logger.error(f"Еске салу қатесі {chat_id}: {e}")


async def job_morning_quote(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Таңғы дәйексөз жіберілуде")
    if not QUOTES:
        logger.warning("QUOTES бос — хабар жіберілмеді")
        return
    quote = random.choice(QUOTES)
    text = f"🌅 *Ассаламу алейкум!*\n\n✨ {quote}\n\nБүгінгі нормативіңді орындауды ұмытпа! 💚"
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM groups WHERE active = TRUE")
        groups = cur.fetchall()
        for group in groups:
            chat_id = group["chat_id"]
            try:
                await context.bot.send_message(chat_id=int(chat_id), text=text, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Таңғы хабар қатесі {chat_id}: {e}")


# ─── Запуск ───────────────────────────────────────────────────────────────────

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("register", cmd_register))
    app.add_handler(CommandHandler("addall", cmd_addall))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("fines", cmd_fines))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("daily", cmd_daily))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("notify", cmd_notify))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_any_message))
    app.add_handler(MessageHandler(filters.Sticker.ALL, handle_sticker))

    # 19:00 UTC = 00:00 Астана
    app.job_queue.run_daily(
        job_daily_report,
        time=dtime(19, 0, tzinfo=UTC),
    )
    # 1-го числа каждого месяца в 00:05 Астаны = 19:05 UTC
    app.job_queue.run_monthly(
        job_monthly_report,
        when=dtime(19, 5, tzinfo=UTC),
        day=1,
    )
    # 16:00 UTC = 21:00 Астана — еске салу
    app.job_queue.run_daily(
        job_reminder,
        time=dtime(16, 0, tzinfo=UTC),
    )
    # 03:00 UTC = 08:00 Астана — таңғы дәйексөз
    app.job_queue.run_daily(
        job_morning_quote,
        time=dtime(3, 0, tzinfo=UTC),
    )

    logger.info("Бот іске қосылды!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()