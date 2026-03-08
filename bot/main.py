import os
import asyncio
import random
import sqlite3
import json
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands
from discord.ext.commands import CommandNotFound
from dotenv import load_dotenv

TZ = ZoneInfo("Europe/Moscow")

ANARCHY_TIMEOUT_SEC = 3600

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR.parent / ".env"
DB_PATH = BASE_DIR / "bot.db"

load_dotenv(dotenv_path=ENV_PATH)

TOKEN = os.getenv("TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
ROLE_ID = int(os.getenv("ROLE_ID", "0"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))

if not TOKEN:
    raise RuntimeError("TOKEN missing")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# -----------------------
# TIME
# -----------------------

def now():
    return datetime.datetime.now(tz=TZ)

def today():
    return now().date().isoformat()

# -----------------------
# DB
# -----------------------

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS rolls(
            date TEXT,
            user_id INTEGER,
            value INTEGER,
            mode TEXT,
            finalized INTEGER,
            created_at TEXT,
            PRIMARY KEY(date,user_id)
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS tiebreaks(
            date TEXT,
            round INTEGER,
            user_id INTEGER,
            value INTEGER,
            created_at TEXT,
            PRIMARY KEY(date,round,user_id)
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS meta(
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

def meta_get(key):
    with db() as conn:
        r = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r["value"] if r else None

def meta_set(key,val):
    with db() as conn:
        if val is None:
            conn.execute("DELETE FROM meta WHERE key=?", (key,))
        else:
            conn.execute("REPLACE INTO meta VALUES(?,?)",(key,val))

# -----------------------
# HELPERS
# -----------------------

def display_name(guild, uid):
    m = guild.get_member(uid)
    if m:
        return m.display_name
    return f"user:{uid}"

def mention(uid):
    return f"<@{uid}>"

# -----------------------
# ROLLS
# -----------------------

def get_roll(date,user):
    with db() as conn:
        return conn.execute(
            "SELECT * FROM rolls WHERE date=? AND user_id=?",
            (date,user)
        ).fetchone()

def insert_roll(date,user,value,mode,finalized):
    with db() as conn:
        conn.execute("""
        INSERT INTO rolls VALUES(?,?,?,?,?,?)
        """,(date,user,value,mode,finalized,now().isoformat()))

def update_roll(date,user,value,mode,finalized):
    with db() as conn:
        conn.execute("""
        UPDATE rolls SET value=?,mode=?,finalized=?
        WHERE date=? AND user_id=?
        """,(value,mode,finalized,date,user))

def day_rolls(date):
    with db() as conn:
        rows=conn.execute(
        "SELECT * FROM rolls WHERE date=?",(date,)
        ).fetchall()
        return [dict(r) for r in rows]

# -----------------------
# ROLE
# -----------------------

async def set_role(user_id):

    guild=bot.get_guild(GUILD_ID)
    role=guild.get_role(ROLE_ID)

    for m in role.members:
        await m.remove_roles(role)

    m=guild.get_member(user_id)
    if m:
        await m.add_roles(role)

# -----------------------
# TIEBREAK
# -----------------------

def tiebreak_state():

    d=meta_get("tb_date")
    if not d:
        return None

    return {
        "date":d,
        "round":int(meta_get("tb_round")),
        "users":json.loads(meta_get("tb_users"))
    }

def tiebreak_start(date,users):

    meta_set("tb_date",date)
    meta_set("tb_round","1")
    meta_set("tb_users",json.dumps(users))

def tiebreak_clear():

    meta_set("tb_date",None)
    meta_set("tb_round",None)
    meta_set("tb_users",None)

# -----------------------
# COMMANDS
# -----------------------

@bot.command()
async def arbuz(ctx):

    if ctx.guild.id!=GUILD_ID:
        return

    date=today()

    tb=tiebreak_state()

    if tb and ctx.author.id in tb["users"]:

        r=random.randint(1,20)

        with db() as conn:
            conn.execute("""
            INSERT INTO tiebreaks VALUES(?,?,?,?,?)
            """,(tb["date"],tb["round"],ctx.author.id,r,now().isoformat()))

        await ctx.send(f"{ctx.author.mention} выбросил **{r}** (тайбрейк)")

        rows=[]
        with db() as conn:
            rows=conn.execute(
            "SELECT user_id,value FROM tiebreaks WHERE date=? AND round=?",
            (tb["date"],tb["round"])
            ).fetchall()

        if len(rows)==len(tb["users"]):

            mx=max(r["value"] for r in rows)
            winners=[r["user_id"] for r in rows if r["value"]==mx]

            if len(winners)==1:

                await ctx.send(f"Победитель тайбрейка {mention(winners[0])}")
                await set_role(winners[0])
                tiebreak_clear()

            else:

                meta_set("tb_users",json.dumps(winners))
                meta_set("tb_round",str(tb["round"]+1))

                await ctx.send("Новая ничья. Ещё раз !arbuz")

        return

    row=get_roll(date,ctx.author.id)

    if row:
        await ctx.send("Ты уже кидал сегодня")
        return

    v=random.randint(1,20)

    if v==1:

        insert_roll(date,ctx.author.id,1,"normal",0)

        await ctx.send(
        f"{ctx.author.mention} выбросил **1**\n!да / !нет"
        )
        return

    insert_roll(date,ctx.author.id,v,"normal",1)

    await ctx.send(f"{ctx.author.mention} выбросил **{v}**")

@bot.command()
async def да(ctx):

    date=today()
    row=get_roll(date,ctx.author.id)

    if not row or row["finalized"]:
        return

    v=random.randint(1,20)

    update_roll(date,ctx.author.id,v,"anarchy",1)

    if v==20:
        meta_set("no_role_today","1")

    await ctx.send(f"{ctx.author.mention} анархия **{v}**")

@bot.command()
async def нет(ctx):

    date=today()
    row=get_roll(date,ctx.author.id)

    if not row:
        return

    update_roll(date,ctx.author.id,1,"normal",1)

    await ctx.send("Остаётся **1**")

@bot.command()
async def top(ctx):

    date=today()
    rows=day_rolls(date)

    guild=bot.get_guild(GUILD_ID)

    normal=[r for r in rows if r["mode"]=="normal" and r["finalized"]]

    text=[f"🏆 Топ за {date}"]

    normal=sorted(normal,key=lambda r:r["value"],reverse=True)

    for i,r in enumerate(normal,1):
        text.append(
        f"{i}. {display_name(guild,r['user_id'])} — {r['value']}"
        )

    await ctx.send("\n".join(text))

# -----------------------

@bot.event
async def on_command_error(ctx,error):
    if isinstance(error,CommandNotFound):
        return
    raise error

init_db()
bot.run(TOKEN)