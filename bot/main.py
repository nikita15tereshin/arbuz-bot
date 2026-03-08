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

# режим теста: сбрасывать "день" раз в N минут
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


def tiebreak_deadline_for_date(date_str: str) -> datetime.datetime:
    """
    Тай-брейк за день YYYY-MM-DD длится до 12:00 МСК следующего дня.
    """
    day = datetime.date.fromisoformat(date_str)
    next_day = day + datetime.timedelta(days=1)
    return datetime.datetime.combine(next_day, datetime.time(12, 0, 0), tzinfo=TZ)


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
                value INTEGER NOT NULL,      -- финал: 1..20
                mode TEXT NOT NULL,          -- normal | anarchy
                finalized INTEGER NOT NULL,  -- 0/1 (если выпало 1 и ждём !да/!нет -> 0)
                created_at TEXT NOT NULL,
                PRIMARY KEY(date, user_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS tiebreaks (
                date TEXT NOT NULL,          -- за какой день тай-брейк
                round INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                value INTEGER NOT NULL,      -- 1..20
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
            conn.execute("REPLACE INTO meta(key, value) VALUES(?, ?)", (key, value))
        conn.commit()


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
    # все кто кинул 1 и не ответил !да/!нет -> остаётся обычная 1
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


def anarchy_twenty_blocks_role(date: str) -> bool:
    """
    Если кто-то в анархии выбил 20, роль за этот день никому не выдаётся.
    """
    rows = get_day_rolls(date)
    return any(
        int(r["finalized"]) == 1 and r["mode"] == "anarchy" and int(r["value"]) == 20
        for r in rows
    )


def top_candidates(date: str):
    rows = get_day_rolls(date)
    if not rows:
        return []

    # на роль претендуют только обычные финализированные броски
    rows = [
        r for r in rows
        if int(r["finalized"]) == 1 and r["mode"] == "normal"
    ]
    if not rows:
        return []

    max_val = max(r["value"] for r in rows)
    winners = [r for r in rows if r["value"] == max_val]
    return winners


def mention(uid: int) -> str:
    return f"<@{uid}>"


def display_name(guild: discord.Guild | None, uid: int) -> str:
    """
    Возвращает ник/username без пинга.
    """
    if guild:
        m = guild.get_member(uid)
        if m:
            return m.display_name
    return f"user:{uid}"


# -----------------------
# TIEBREAK
# -----------------------
def tiebreak_state(date: str | None = None):
    """
    Если date=None — возвращаем активный тай-брейк (какой бы он ни был).
    Если date задан — возвращаем только если active_date == date.
    Если дедлайн истёк — автоматически очищаем тай-брейк и возвращаем None.
    """
    active_date = meta_get("tiebreak_active_date")
    if not active_date:
        return None

    if date is not None and active_date != date:
        return None

    try:
        round_num = int(meta_get("tiebreak_round") or "0")
        users = json.loads(meta_get("tiebreak_users") or "[]")
        deadline_iso = meta_get("tiebreak_deadline")

        if deadline_iso:
            deadline_dt = datetime.datetime.fromisoformat(deadline_iso)
            if now_msk() > deadline_dt:
                tiebreak_clear()
                return None

        return {
            "date": active_date,
            "round": round_num,
            "users": users,
            "deadline": deadline_iso,
        }
    except Exception:
        return None


def tiebreak_start(date: str, users: list[int]):
    meta_set("tiebreak_active_date", date)
    meta_set("tiebreak_round", "1")
    meta_set("tiebreak_users", json.dumps(users))
    meta_set("tiebreak_deadline", tiebreak_deadline_for_date(date).isoformat())


def tiebreak_next_round(date: str):
    st = tiebreak_state(date)
    if not st:
        return
    new_round = st["round"] + 1
    meta_set("tiebreak_round", str(new_round))
    meta_set("tiebreak_deadline", tiebreak_deadline_for_date(date).isoformat())


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
            SELECT user_id, value
            FROM tiebreaks
            WHERE date=? AND round=?
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
        return

    max_val = max(r["value"] for r in res)
    winners = [r for r in res if r["value"] == max_val]

    ch = get_channel()

    if len(winners) == 1:
        winner_id = winners[0]["user_id"]

        if ch:
            await ch.send(
                f"✅ Тай-брейк за **{date}**, раунд {round_num}, завершён.\n"
                f"Победитель: {mention(winner_id)} (**{max_val}**)"
            )

        # даже в тай-брейке учитываем правило: если в анархии кто-то выбил 20, роль никому
        if anarchy_twenty_blocks_role(date):
            if ch:
                await ch.send(
                    f"🚫 За **{date}** в арбузной анархии кто-то выбил **20**.\n"
                    f"По правилам роль **АРБУЗ** за этот день не получает никто."
                )
        else:
            ok = await set_winner_role(winner_id)
            if ch:
                await ch.send(
                    "🍉 Роль **АРБУЗ** выдана победителю тай-брейка."
                    if ok else
                    "⚠️ Не смог выдать роль (проверь права/позицию роли)."
                )

        tiebreak_clear()
    else:
        tied_users = [r["user_id"] for r in winners]
        meta_set("tiebreak_users", json.dumps(tied_users))
        tiebreak_next_round(date)

        if ch:
            deadline = tiebreak_deadline_for_date(date).strftime("%d.%m %H:%M МСК")
            await ch.send(
                "🤝 Снова ничья в тай-брейке между: "
                + " ".join(mention(u) for u in tied_users)
                + f"\nКидайте снова `!arbuz` (раунд {round_num + 1}). Дедлайн: **{deadline}**."
            )


# -----------------------
# ROLE MANAGEMENT
# -----------------------
async def remove_role_from_all(guild: discord.Guild, role: discord.Role):
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

    today = today_str()

    # Если пользователь участвует в активном тай-брейке,
    # то этот !arbuz идёт в тай-брейк и НЕ считается обычным броском дня
    st = tiebreak_state(None)
    if st and ctx.author.id in st["users"]:
        tb_date = st["date"]
        round_num = st["round"]

        if tiebreak_user_already_rolled(tb_date, round_num, ctx.author.id):
            await ctx.send("⛔ Ты уже кинул в этом раунде тай-брейка.")
            return

        value = random.randint(1, 20)
        tiebreak_record(tb_date, round_num, ctx.author.id, value)
        await ctx.send(
            f"{ctx.author.mention} (тай-брейк за {tb_date}, раунд {round_num}) выбросил **{value}** 🎲"
        )

        await maybe_finish_tiebreak(tb_date)
        return

    # Обычный дневной бросок
    existing = get_user_roll(today, ctx.author.id)
    if existing:
        if int(existing["finalized"]) == 0 and int(existing["value"]) == 1:
            await ctx.send(
                f"{ctx.author.mention}, ты уже выбил **1** и ждёшь решение: `!да` / `!нет`."
            )
        else:
            await ctx.send(
                f"{ctx.author.mention}, ты уже кидал сегодня. Твой результат: **{existing['value']}**."
            )
        return

    value = random.randint(1, 20)

    if value == 1:
        insert_roll(today, ctx.author.id, 1, "normal", 0)
        await ctx.send(
            f"{ctx.author.mention} выбросил **1** 🎲\n"
            f"Хотите арбузную анархию? `!да` / `!нет`\n"
            f"⏳ Ответь в течение {ANARCHY_TIMEOUT_SEC} секунд."
        )
        return

    insert_roll(today, ctx.author.id, value, "normal", 1)
    await ctx.send(f"{ctx.author.mention} выбросил **{value}** 🎲")


@bot.command(name="да")
async def yes_cmd(ctx: commands.Context):
    if ctx.guild is None or ctx.guild.id != GUILD_ID:
        return

    date = today_str()
    row = get_user_roll(date, ctx.author.id)
    if not row:
        await ctx.send("Сначала кинь `!arbuz`.")
        return
    if int(row["finalized"]) == 1:
        await ctx.send("Твой бросок уже финализирован. Сегодня второй попытки нет.")
        return
    if int(row["value"]) != 1:
        await ctx.send("`!да` работает только если ты выбросил 1.")
        return

    created_at = datetime.datetime.fromisoformat(row["created_at"])
    if (now_msk() - created_at).total_seconds() > ANARCHY_TIMEOUT_SEC:
        update_roll(date, ctx.author.id, 1, "normal", 1)
        await ctx.send(f"{ctx.author.mention} время вышло. Остаётся **1**.")
        return

    value = random.randint(1, 20)
    update_roll(date, ctx.author.id, value, "anarchy", 1)

    if value == 20:
        await ctx.send(
            f"{ctx.author.mention} включил арбузную анархию и выбросил **20** 🍉\n"
            f"🚫 По правилам роль **АРБУЗ** за сегодня не получает никто."
        )
    else:
        await ctx.send(
            f"{ctx.author.mention} включил арбузную анархию и выбросил **{value}** 🍉"
        )


@bot.command(name="нет")
async def no_cmd(ctx: commands.Context):
    if ctx.guild is None or ctx.guild.id != GUILD_ID:
        return

    date = today_str()
    row = get_user_roll(date, ctx.author.id)
    if not row:
        await ctx.send("Сначала кинь `!arbuz`.")
        return
    if int(row["finalized"]) == 1:
        await ctx.send("Твой бросок уже финализирован. Сегодня второй попытки нет.")
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
    pending = [r for r in rows if int(r["finalized"]) == 0]

    normal = [r for r in finalized if r["mode"] == "normal"]
    anarchy = [r for r in finalized if r["mode"] == "anarchy"]

    text = [f"🏆 **Лидерборд за {date} (МСК)**"]

    if pending:
        pend_users = ", ".join(display_name(guild, r["user_id"]) for r in pending)
        text.append(f"⏳ Ждут решения `!да/!нет`: {pend_users}")

    text.append("")
    text.append("🍉 **Топ на роль АРБУЗ (только обычные броски 1–20):**")
    if normal:
        normal_sorted = sorted(normal, key=lambda r: r["value"], reverse=True)
        for i, r in enumerate(normal_sorted, 1):
            text.append(f"{i}. {display_name(guild, r['user_id'])} — **{r['value']}**")
    else:
        text.append("— сегодня никто не сделал обычный финальный бросок")

    text.append("")
    text.append("🎲 **Арбузная анархия (1–20, отдельно от розыгрыша роли):**")
    if anarchy:
        anarchy_sorted = sorted(anarchy, key=lambda r: r["value"], reverse=True)
        for i, r in enumerate(anarchy_sorted, 1):
            text.append(f"{i}. {display_name(guild, r['user_id'])} — **{r['value']}**")
    else:
        text.append("— сегодня никто не включал анархию")

    dist = {i: 0 for i in range(1, 21)}
    for r in normal:
        v = r["value"]
        if 1 <= v <= 20:
            dist[v] += 1

    text.append("")
    text.append("🔢 **Распределение обычных бросков (1–20):**")
    if any(dist.values()):
        text.append(" ".join([f"{k}:{dist[k]}" for k in range(1, 21)]))
    else:
        text.append("— никто не выбил число в диапазоне 1–20")

    if anarchy_twenty_blocks_role(date):
        text.append("")
        text.append("🚫 **В анархии выпало 20 — роль АРБУЗ сегодня не получает никто.**")

    st = tiebreak_state(None)
    if st:
        users_str = ", ".join(display_name(guild, uid) for uid in st["users"])
        deadline_str = "—"
        if st["deadline"]:
            try:
                deadline_str = datetime.datetime.fromisoformat(st["deadline"]).strftime("%d.%m %H:%M МСК")
            except Exception:
                pass

        text.append("")
        text.append(
            f"⚔️ **Активный тай-брейк** за {st['date']} "
            f"(раунд {st['round']}): {users_str}. Дедлайн: **{deadline_str}**"
        )

    await ctx.send("\n".join(text))


# -----------------------
# ERROR HANDLER
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

    # 0) финализируем все pending единицы как обычные 1
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

    # Если в анархии выпало 20 — роль никому
    if anarchy_twenty_blocks_role(date):
        await remove_role_from_all(guild, role)
        if ch:
            await ch.send(
                f"🚫 Итоги за {date}: в арбузной анархии кто-то выбил **20**.\n"
                f"По правилам роль **АРБУЗ** за этот день не получает никто."
            )
        return

    # 1) определяем кандидатов только по обычным броскам
    winners = top_candidates(date)

    # если обычных бросков не было — снимаем роль
    if not winners:
        await remove_role_from_all(guild, role)
        if ch:
            await ch.send(f"📭 Итоги за {date}: никто не сделал обычный бросок `!arbuz`. Роль снята.")
        return

    max_val = winners[0]["value"]

    # 2) если победитель один — обновляем роль сразу
    if len(winners) == 1:
        winner_id = winners[0]["user_id"]
        ok = await set_winner_role(winner_id)
        if ch:
            await ch.send(
                f"🍉 Итоги за {date}: победитель {mention(winner_id)} со значением **{max_val}**.\n"
                + ("✅ Роль выдана." if ok else "⚠️ Не смог выдать роль (проверь права/позицию роли).")
            )
        return

    # 3) если ничья — НЕ снимаем текущую роль, а запускаем тай-брейк
    tied_users = [w["user_id"] for w in winners]
    tiebreak_start(date, tied_users)

    if ch:
        deadline = tiebreak_deadline_for_date(date).strftime("%d.%m %H:%M МСК")
        await ch.send(
            f"⚔️ Итоги за {date}: ничья на **{max_val}** между "
            + " ".join(mention(u) for u in tied_users)
            + f"\nЗапускается тай-брейк. Дедлайн: **{deadline}**."
            + "\nТекущая роль **АРБУЗ** пока остаётся у прошлого владельца до завершения тай-брейка."
            + "\nУчастники тай-брейка кидают `!arbuz` отдельно."
            + "\nТай-брейк не идёт в общий топ и не считается за обычный бросок нового дня."
        )


def seconds_until_next_midnight_msk() -> int:
    now = now_msk()
    tomorrow = (now + datetime.timedelta(days=1)).date()
    next_midnight = datetime.datetime.combine(tomorrow, datetime.time(0, 0, 0), tzinfo=TZ)
    return max(1, int((next_midnight - now).total_seconds()))


@tasks.loop(seconds=30)
async def heartbeat():
    pass


@tasks.loop(minutes=1)
async def test_reset_loop():
    if TEST_RESET_EVERY_MINUTES is None:
        return

    key = "last_test_reset_minute"
    last = meta_get(key)

    now = now_msk()
    marker = now.strftime("%Y-%m-%d %H:%M")
    if last == marker:
        return

    if now.minute % int(TEST_RESET_EVERY_MINUTES) != 0:
        return

    meta_set(key, marker)
    date = today_str(now)
    ch = get_channel()
    if ch:
        await ch.send(f"🧪 TEST RESET: закрываю период за {date} (минутный режим)")
    await process_day_end(date)


async def midnight_scheduler():
    await bot.wait_until_ready()
    while not bot.is_closed():
        if TEST_RESET_EVERY_MINUTES is not None:
            await asyncio.sleep(5)
            continue

        secs = seconds_until_next_midnight_msk()
        await asyncio.sleep(secs)

        date = today_str(now_msk() - datetime.timedelta(seconds=1))  # закрываем вчерашний день
        await process_day_end(date)


@bot.event
async def on_ready():
    global midnight_task

    print(f"Logged in as {bot.user} (id: {bot.user.id})")

    if not heartbeat.is_running():
        heartbeat.start()

    if not test_reset_loop.is_running():
        test_reset_loop.start()

    if TEST_RESET_EVERY_MINUTES is None:
        if midnight_task is None or midnight_task.done():
            midnight_task = bot.loop.create_task(midnight_scheduler())


# -----------------------
# STARTUP
# -----------------------
init_db()

print("Starting bot...")
bot.run(TOKEN)