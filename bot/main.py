import os
import asyncio
import random
import sqlite3
import json
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands, tasks
from discord.ext.commands import CommandNotFound
from dotenv import load_dotenv

# -----------------------
# CONFIG
# -----------------------
TZ = ZoneInfo("Europe/Moscow")  # всё считаем по МСК

# сколько ждём !да/!нет после выпадения 1
ANARCHY_TIMEOUT_SEC = 3600

# сколько даём времени на переброс при ничьей (сек)
TIEBREAK_TIMEOUT_SEC = 12 * 3600  # 43200

# режим теста: сбрасывать "день" раз в N минут (например 1)
# В проде оставь None (тогда будет строго 00:00 МСК)
TEST_RESET_EVERY_MINUTES = None  # например 1 для теста, или None для прода

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR.parent / ".env"
DB_PATH = BASE_DIR / "bot.db"

load_dotenv(dotenv_path=ENV_PATH)

TOKEN = os.getenv("TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
ROLE_ID = int(os.getenv("ROLE_ID", "0"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))

if not TOKEN:
    raise RuntimeError(f"TOKEN is missing. Check {ENV_PATH}")

def now_msk() -> datetime.datetime:
    return datetime.datetime.now(tz=TZ)

def today_str(dt: datetime.datetime | None = None) -> str:
    dt = dt or now_msk()
    return dt.date().isoformat()  # YYYY-MM-DD

# -----------------------
# DISCORD
# -----------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

midnight_task = None

def get_channel() -> discord.TextChannel | None:
    ch = bot.get_channel(CHANNEL_ID)
    if isinstance(ch, discord.TextChannel):
        return ch
    return None

# -----------------------
# DB
# -----------------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rolls (
                date TEXT NOT NULL,          -- YYYY-MM-DD (по МСК)
                user_id INTEGER NOT NULL,
                value INTEGER NOT NULL,       -- финал: 1..20 или 1..100 (анархия)
                mode TEXT NOT NULL,           -- normal | anarchy
                finalized INTEGER NOT NULL,   -- 0/1 (если выпало 1 и ждём !да/!нет -> 0)
                created_at TEXT NOT NULL,
                PRIMARY KEY(date, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tiebreaks (
                date TEXT NOT NULL,           -- за какой день тай-брейк (этот же день)
                round INTEGER NOT NULL,        -- номер раунда
                user_id INTEGER NOT NULL,
                value INTEGER NOT NULL,        -- 1..20
                created_at TEXT NOT NULL,
                PRIMARY KEY(date, round, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()

def meta_get(key: str) -> str | None:
    with db() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

def meta_set(key: str, value: str | None):
    with db() as conn:
        if value is None:
            conn.execute("DELETE FROM meta WHERE key=?", (key,))
        else:
            conn.execute("REPLACE INTO meta(key,value) VALUES(?,?)", (key, value))
        conn.commit()

def user_has_roll(date: str, user_id: int) -> bool:
    with db() as conn:
        row = conn.execute("SELECT 1 FROM rolls WHERE date=? AND user_id=?", (date, user_id)).fetchone()
        return row is not None

def get_user_roll(date: str, user_id: int):
    with db() as conn:
        row = conn.execute("SELECT * FROM rolls WHERE date=? AND user_id=?", (date, user_id)).fetchone()
        return row

def insert_roll(date: str, user_id: int, value: int, mode: str, finalized: int):
    with db() as conn:
        conn.execute("""
            INSERT INTO rolls(date, user_id, value, mode, finalized, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
        """, (date, user_id, value, mode, finalized, now_msk().isoformat()))
        conn.commit()

def update_roll(date: str, user_id: int, value: int, mode: str, finalized: int):
    with db() as conn:
        conn.execute("""
            UPDATE rolls
            SET value=?, mode=?, finalized=?
            WHERE date=? AND user_id=?
        """, (value, mode, finalized, date, user_id))
        conn.commit()

async def finalize_pending_as_ones(date: str):
    # все кто кинул 1 и не ответил !да/!нет -> остаётся 1
    with db() as conn:
        conn.execute("""
            UPDATE rolls
            SET finalized=1, mode='normal', value=1
            WHERE date=? AND finalized=0
        """, (date,))
        conn.commit()

def get_day_rolls(date: str):
    with db() as conn:
        rows = conn.execute("""
            SELECT user_id, value, mode, finalized
            FROM rolls
            WHERE date=?
        """, (date,)).fetchall()
        return [dict(r) for r in rows]

def has_any_100(date: str) -> bool:
    rows = get_day_rolls(date)
    return any(r["value"] == 100 for r in rows)

def top_candidates(date: str):
    rows = get_day_rolls(date)
    if not rows:
        return []

    # финализируем для подсчета только finalized=1
    rows = [r for r in rows if int(r["finalized"]) == 1]
    if not rows:
        return []

    # если есть 100 — победителя нет (правило 6), но всё равно можно показать топ
    # в этом случае всё равно вернём кандидатов, но роль не выдадим
    max_val = max(r["value"] for r in rows)
    winners = [r for r in rows if r["value"] == max_val]
    return winners

def mention(uid: int) -> str:
    return f"<@{uid}>"

def display_name(guild: discord.Guild | None, uid: int) -> str:
    """
    Возвращает ник/username без пинга.
    Если пользователь в гильдии найден — берём display_name (серверный ник),
    иначе просто 'user:<id>' как фоллбек.
    """
    if guild:
        m = guild.get_member(uid)
        if m:
            return m.display_name  # ник на сервере
    return f"user:{uid}"

# -----------------------
# TIEBREAK
# -----------------------
def tiebreak_state(date: str):
    """
    meta keys:
    tiebreak_active_date
    tiebreak_round
    tiebreak_users (json list[int])
    tiebreak_deadline (iso)
    """
    active_date = meta_get("tiebreak_active_date")
    if active_date != date:
        return None

    try:
        round_num = int(meta_get("tiebreak_round") or "0")
        users = json.loads(meta_get("tiebreak_users") or "[]")
        deadline = meta_get("tiebreak_deadline")
        return {
            "date": active_date,
            "round": round_num,
            "users": users,
            "deadline": deadline
        }
    except Exception:
        return None

def tiebreak_start(date: str, users: list[int]):
    meta_set("tiebreak_active_date", date)
    meta_set("tiebreak_round", "1")
    meta_set("tiebreak_users", json.dumps(users))
    meta_set("tiebreak_deadline", (now_msk() + datetime.timedelta(seconds=TIEBREAK_TIMEOUT_SEC)).isoformat())

def tiebreak_next_round(date: str):
    st = tiebreak_state(date)
    if not st:
        return
    new_round = st["round"] + 1
    meta_set("tiebreak_round", str(new_round))
    meta_set("tiebreak_deadline", (now_msk() + datetime.timedelta(seconds=TIEBREAK_TIMEOUT_SEC)).isoformat())

def tiebreak_clear():
    meta_set("tiebreak_active_date", None)
    meta_set("tiebreak_round", None)
    meta_set("tiebreak_users", None)
    meta_set("tiebreak_deadline", None)

def tiebreak_user_already_rolled(date: str, round_num: int, user_id: int) -> bool:
    with db() as conn:
        row = conn.execute("""
            SELECT 1 FROM tiebreaks WHERE date=? AND round=? AND user_id=?
        """, (date, round_num, user_id)).fetchone()
        return row is not None

def tiebreak_record(date: str, round_num: int, user_id: int, value: int):
    with db() as conn:
        conn.execute("""
            INSERT INTO tiebreaks(date, round, user_id, value, created_at)
            VALUES(?,?,?,?,?)
        """, (date, round_num, user_id, value, now_msk().isoformat()))
        conn.commit()

def tiebreak_round_results(date: str, round_num: int):
    with db() as conn:
        rows = conn.execute("""
            SELECT user_id, value FROM tiebreaks WHERE date=? AND round=?
        """, (date, round_num)).fetchall()
        return [dict(r) for r in rows]

async def maybe_finish_tiebreak(date: str):
    st = tiebreak_state(date)
    if not st:
        return

    round_num = st["round"]
    users = st["users"]

    res = tiebreak_round_results(date, round_num)
    if len(res) < len(users):
        return  # ещё не все кинули

    # все кинули
    max_val = max(r["value"] for r in res)
    winners = [r for r in res if r["value"] == max_val]

    ch = get_channel()

    if len(winners) == 1:
        winner_id = winners[0]["user_id"]
        if ch:
            await ch.send(f"✅ Тай-брейк раунд {round_num} завершён. Победитель: {mention(winner_id)} (**{max_val}**)")

        # выдаём роль победителю (если сегодня нет 100)
        if has_any_100(date):
            if ch:
                who100 = [mention(r["user_id"]) for r in get_day_rolls(date) if r["value"] == 100]
                await ch.send(
                    f"💯 За {date} кто-то выбил **100** ({', '.join(who100)}).\n"
                    f"🚫 По правилам роль **АРБУЗ** сегодня не выдаётся никому (до следующего сброса)."
                )
        else:
            ok = await set_winner_role(winner_id)
            if ch:
                await ch.send("🍉 Роль **АРБУЗ** выдана победителю." if ok else "⚠️ Не смог выдать роль (проверь права/позицию роли).")

        tiebreak_clear()
    else:
        # снова ничья -> следующий раунд
        tied_users = [r["user_id"] for r in winners]
        meta_set("tiebreak_users", json.dumps(tied_users))
        tiebreak_next_round(date)
        if ch:
            await ch.send(
                "🤝 Снова ничья! Переброс между: " + " ".join(mention(u) for u in tied_users) +
                f"\nКидайте снова `!arbuz` (раунд {round_num+1})."
            )

# -----------------------
# ROLE MANAGEMENT
# -----------------------
async def remove_role_from_all(guild: discord.Guild, role: discord.Role):
    # снимаем роль со всех у кого она есть
    for member in role.members:
        try:
            await member.remove_roles(role, reason="Arbuz daily reset")
        except Exception:
            pass

async def set_winner_role(winner_id: int) -> bool:
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return False
    role = guild.get_role(ROLE_ID)
    if not role:
        return False

    # снимаем со всех, потом выдаём победителю
    await remove_role_from_all(guild, role)

    member = guild.get_member(winner_id)
    if not member:
        try:
            member = await guild.fetch_member(winner_id)
        except Exception:
            return False
    try:
        await member.add_roles(role, reason="Arbuz daily winner")
        return True
    except Exception:
        return False

# -----------------------
# COMMANDS
# -----------------------
@bot.command(name="arbuz")
async def arbuz_cmd(ctx: commands.Context):
    if ctx.guild is None or ctx.guild.id != GUILD_ID:
        return

    date = today_str()

    # Тай-брейк?
    st = tiebreak_state(date)
    if st:
        # в тайбрейке могут кидать только участники тайбрейка
        if ctx.author.id not in st["users"]:
            await ctx.send(" Сейчас идёт тай-брейк. Кидать могут только участники ничьей.")
            return

        round_num = st["round"]
        if tiebreak_user_already_rolled(date, round_num, ctx.author.id):
            await ctx.send(" Ты уже кинул в этом раунде тай-брейка.")
            return

        value = random.randint(1, 20)
        tiebreak_record(date, round_num, ctx.author.id, value)
        await ctx.send(f"{ctx.author.mention} (тай-брейк раунд {round_num}) выбросил **{value}** 🎲")

        await maybe_finish_tiebreak(date)
        return

    # обычный день
    existing = get_user_roll(date, ctx.author.id)
    if existing:
        if int(existing["finalized"]) == 0 and int(existing["value"]) == 1:
            await ctx.send(
                f"{ctx.author.mention}, ты уже выбил **1** и ждёшь решение: `!да` / `!нет`."
            )
        else:
            await ctx.send(f"{ctx.author.mention}, ты уже кидал сегодня. Твой результат: **{existing['value']}**.")
        return

    # первая попытка
    value = random.randint(1, 20)

    if value == 1:
        # записываем как pending (finalized=0)
        insert_roll(date, ctx.author.id, 1, "normal", 0)
        await ctx.send(
            f"{ctx.author.mention} выбросил **1** 🎲\n"
            f"Хотите арбузную анархию? `!да` / `!нет`\n"
            f"⏳ Ответь в течение {ANARCHY_TIMEOUT_SEC} секунд."
        )
        return

    insert_roll(date, ctx.author.id, value, "normal", 1)
    await ctx.send(f"{ctx.author.mention} выбросил **{value}** 🎲")

@bot.command(name="да")
async def yes_cmd(ctx: commands.Context):
    if ctx.guild is None or ctx.guild.id != GUILD_ID:
        return

    date = today_str()
    row = get_user_roll(date, ctx.author.id)
    if not row:
        await ctx.send("Сначала кинь `!arbuz` ")
        return
    if int(row["finalized"]) == 1:
        await ctx.send("Твой бросок уже финализирован. Сегодня второй попытки нет ")
        return
    if int(row["value"]) != 1:
        await ctx.send("`!да` работает только если ты выбросил 1.")
        return

    # проверим таймаут
    created_at = datetime.datetime.fromisoformat(row["created_at"])
    if (now_msk() - created_at).total_seconds() > ANARCHY_TIMEOUT_SEC:
        # время вышло — оставляем 1
        update_roll(date, ctx.author.id, 1, "normal", 1)
        await ctx.send(f"{ctx.author.mention} время вышло. Остаётся **1**.")
        return

    value = random.randint(1, 100)
    update_roll(date, ctx.author.id, value, "anarchy", 1)
    await ctx.send(f"{ctx.author.mention} включил арбузную анархию и выбросил **{value}** 🍉")

@bot.command(name="нет")
async def no_cmd(ctx: commands.Context):
    if ctx.guild is None or ctx.guild.id != GUILD_ID:
        return

    date = today_str()
    row = get_user_roll(date, ctx.author.id)
    if not row:
        await ctx.send("Сначала кинь `!arbuz` ")
        return
    if int(row["finalized"]) == 1:
        await ctx.send("Твой бросок уже финализирован. Сегодня второй попытки нет ")
        return
    if int(row["value"]) != 1:
        await ctx.send("`!нет` работает только если ты выбросил 1.")
        return

    update_roll(date, ctx.author.id, 1, "normal", 1)
    await ctx.send(f"{ctx.author.mention} отказался от анархии. Остаётся **1** 🍉")

@bot.command(name="top")
async def top_cmd(ctx: commands.Context):
    if ctx.guild is None or ctx.guild.id != GUILD_ID:
        return

    date = today_str()
    rows = get_day_rolls(date)
    if not rows:
        await ctx.send("Сегодня ещё никто не кидал `!arbuz`.")
        return

    guild = bot.get_guild(GUILD_ID)

    finalized = [r for r in rows if int(r["finalized"]) == 1]
    pending   = [r for r in rows if int(r["finalized"]) == 0]

    normal  = [r for r in finalized if r["mode"] != "anarchy"]   # 1..20
    anarchy = [r for r in finalized if r["mode"] == "anarchy"]   # 1..100

    text = [f"🏆 **Лидерборд за {date} (МСК)**"]

    if pending:
        pend_users = ", ".join(display_name(guild, r["user_id"]) for r in pending)
        text.append(f"⏳ Ждут решения `!да/!нет`: {pend_users}")

    # --- Нормальный топ 1..20 ---
    text.append("")
    text.append("🎲 **Обычный режим (1–20):**")
    if normal:
        normal_sorted = sorted(normal, key=lambda r: r["value"], reverse=True)
        for i, r in enumerate(normal_sorted, 1):
            text.append(f"{i}. {display_name(guild, r['user_id'])} — **{r['value']}**")
    else:
        text.append("— сегодня никто не кидал в обычном режиме")

    # --- Анархия 1..100 ---
    text.append("")
    text.append("🍉 **Арбузная анархия (1–100):**")
    if anarchy:
        anarchy_sorted = sorted(anarchy, key=lambda r: r["value"], reverse=True)
        for i, r in enumerate(anarchy_sorted, 1):
            text.append(f"{i}. {display_name(guild, r['user_id'])} — **{r['value']}**")
    else:
        text.append("— сегодня никто не включал анархию")

    # --- Распределение 1..20 (только normal) ---
    dist = {i: 0 for i in range(1, 21)}
    for r in normal:
        v = r["value"]
        if 1 <= v <= 20:
            dist[v] += 1

    text.append("")
    text.append("🔢 **Распределение (1–20):**")
    if any(dist.values()):
        text.append(" ".join([f"{k}:{dist[k]}" for k in range(1, 21)]))
    else:
        text.append("— никто не выбил число в диапазоне 1–20")

    # --- Кто выбил 100 (только anarchy, без пинга) ---
    who100_ids = [r["user_id"] for r in anarchy if r["value"] == 100]
    text.append("")
    if who100_ids:
        who100_names = ", ".join(display_name(guild, uid) for uid in who100_ids)
        text.append(f"💯 **Кто-то выбил 100**: {who100_names}")
    else:
        text.append("💯 **Никто не выбил 100.**")

    await ctx.send("\n".join(text))

# -----------------------
# ERROR HANDLER (не спамим логами)
# -----------------------
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, CommandNotFound):
        return
    raise error

# -----------------------
# DAILY RESET / TEST RESET
# -----------------------
async def process_day_end(date: str):
    ch = get_channel()

    # 0) финализируем все pending единицы как 1
    await finalize_pending_as_ones(date)

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        if ch:
            await ch.send("⚠️ Не нашёл guild. Проверь GUILD_ID.")
        return

    role = guild.get_role(ROLE_ID)
    if not role:
        if ch:
            await ch.send("⚠️ Не нашёл роль. Проверь ROLE_ID.")
        return

    # 1) снимаем роль со всех
    await remove_role_from_all(guild, role)

    # 2) если есть 100 — роль никому
    if has_any_100(date):
        if ch:
            who100 = [mention(r["user_id"]) for r in get_day_rolls(date) if r["value"] == 100 and int(r["finalized"]) == 1]
            await ch.send(
                f"💯 Итоги за {date}: кто-то выбил **100** ({', '.join(who100)}).\n"
                f"🚫 По правилам роль **АРБУЗ** сегодня не выдаётся никому."
            )
        return

    # 3) определяем победителя (по максимуму)
    winners = top_candidates(date)
    if not winners:
        if ch:
            await ch.send(f"📭 Итоги за {date}: никто не кинул `!arbuz`.")
        return

    max_val = winners[0]["value"]

    if len(winners) == 1:
        winner_id = winners[0]["user_id"]
        ok = await set_winner_role(winner_id)
        if ch:
            await ch.send(
                f"🍉 Итоги за {date}: победитель {mention(winner_id)} со значением **{max_val}**.\n"
                + ("✅ Роль выдана." if ok else "⚠️ Не смог выдать роль (проверь права/позицию роли).")
            )
        return

    # 4) ничья -> тай-брейк
    tied_users = [w["user_id"] for w in winners]
    tiebreak_start(date, tied_users)
    if ch:
        await ch.send(
            f" Итоги за {date}: ничья на **{max_val}** между: " + " ".join(mention(u) for u in tied_users) +
            "\nКидайте `!arbuz` ещё раз для тай-брейка!"
        )

def seconds_until_next_midnight_msk() -> int:
    now = now_msk()
    tomorrow = (now + datetime.timedelta(days=1)).date()
    next_midnight = datetime.datetime.combine(tomorrow, datetime.time(0, 0, 0), tzinfo=TZ)
    return max(1, int((next_midnight - now).total_seconds()))

@tasks.loop(seconds=30)
async def heartbeat():
    # просто чтобы видеть что бот жив, можно убрать
    pass

@tasks.loop(minutes=1)
async def test_reset_loop():
    # для теста: "день" закрываем каждые N минут
    if TEST_RESET_EVERY_MINUTES is None:
        return
    # закрываем "период" по текущей дате, но чтобы не повторять бесконечно — храним last_test_reset_minute
    key = "last_test_reset_minute"
    last = meta_get(key)

    now = now_msk()
    marker = now.strftime("%Y-%m-%d %H:%M")  # минута
    if last == marker:
        return

    # каждые N минут
    if now.minute % int(TEST_RESET_EVERY_MINUTES) != 0:
        return

    meta_set(key, marker)
    date = today_str(now)
    ch = get_channel()
    if ch:
        await ch.send(f"🧪 TEST RESET: закрываю период за {date} (минутный режим)")
    await process_day_end(date)

async def midnight_scheduler():
    # продовый планировщик: ждём до 00:00 МСК, запускаем, повторяем
    await bot.wait_until_ready()
    while not bot.is_closed():
        if TEST_RESET_EVERY_MINUTES is not None:
            await asyncio.sleep(5)
            continue
        secs = seconds_until_next_midnight_msk()
        await asyncio.sleep(secs)
        date = today_str(now_msk() - datetime.timedelta(seconds=1))  # закрываем "вчерашний" день
        await process_day_end(date)

@bot.event
async def on_ready():
    global midnight_task

    print(f"Logged in as {bot.user} (id: {bot.user.id})")

    if not heartbeat.is_running():
        heartbeat.start()

    if not test_reset_loop.is_running():
        test_reset_loop.start()

    # запускаем ночной планировщик (если не тестовый режим)
    if TEST_RESET_EVERY_MINUTES is None:
        if midnight_task is None or midnight_task.done():
            midnight_task = bot.loop.create_task(midnight_scheduler())

# -----------------------
# STARTUP
# -----------------------
init_db()

print("Starting bot...")
bot.run(TOKEN)
