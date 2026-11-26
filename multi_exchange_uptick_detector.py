"""
Главный файл запуска скринера:
- База данных (PostgreSQL / SQLite)
- Telegram Bot + Mini App + Tiger.Trade
- Discord Webhooks (отдельный модуль: discord_notifier.py)
- FastAPI сервер (включая /ping для UptimeRobot)

Логика Аптиков вынесена в отдельный модуль: upticks.py
Логика Покупаш / Продаваш вынесена в отдельный модуль: robots.py
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import aiosqlite
import uvicorn
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from discord_notifier import (
    get_env_webhooks,
    is_valid_discord_webhook,
    normalize_discord_config,
    resolve_webhook_url,
    send_robot_discord,
    send_startup_discord,
    send_test_discord,
    send_uptick_discord,
)
from robots import (
    DEFAULT_MIN_ROBOT_USD,
    RobotEngine,
    RobotEvent,
    RobotSide,
    create_robot_tasks,
    is_robot_blacklisted_symbol,
    normalize_robot_config,
    refresh_tradfi_metadata,
    robot_matches_user_config,
)
from upticks import (
    DEFAULT_MIN_VOLUME,
    UptickEngine,
    UptickEvent,
    UptickSide,
    create_uptick_tasks,
    extract_base_asset,
    is_blacklisted_symbol,
)

try:
    import asyncpg  # type: ignore
except ImportError:
    asyncpg = None

# ==================== ЛОГИРОВАНИЕ И КОНФИГ ====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("uptick_bot")

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
WEBAPP_URL = (os.getenv("WEBAPP_URL") or "").strip()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
SQLITE_PATH = os.getenv("SQLITE_PATH", "uptick_bot.db")


# ==================== ПОЛЬЗОВАТЕЛЬСКИЙ ЧЕРНЫЙ СПИСОК ====================

def parse_banned_coins(raw: Any) -> Dict[str, str]:
    """Парсит личный черный список пользователя: {'COIN': 'ALL'|'UPTICK'|'ROBOT'}."""
    if not raw:
        return {}
    if isinstance(raw, dict):
        data = raw
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            data = parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    else:
        return {}

    out: Dict[str, str] = {}
    for k, v in data.items():
        base = extract_base_asset(str(k))
        scope = str(v or "ALL").strip().upper()
        if scope not in ("ALL", "UPTICK", "ROBOT"):
            scope = "ALL"
        if base:
            out[base] = scope
    return out


def is_coin_banned(base: str, banned_map: Dict[str, str], kind: str) -> bool:
    """Проверяет, скрыта ли монета пользователем для данного типа сигнала (UPTICK или ROBOT)."""
    if not base or not banned_map:
        return False
    scope = banned_map.get(base)
    if not scope:
        return False
    if scope == "ALL":
        return True
    return scope == kind


# ==================== БАЗА ДАННЫХ (PostgreSQL / SQLite) ====================

_pg_pool: Any = None
_use_pg: bool = False
_http_session: Optional[aiohttp.ClientSession] = None


def _to_pg_query(query: str) -> str:
    """Преобразует ?-плейсхолдеры в $1, $2... для asyncpg."""
    idx = 0
    out: List[str] = []
    for ch in query:
        if ch == "?":
            idx += 1
            out.append(f"${idx}")
        else:
            out.append(ch)
    return "".join(out)


async def init_db() -> None:
    """Инициализирует PostgreSQL (если указан DATABASE_URL) или локальную SQLite."""
    global _pg_pool, _use_pg
    if DATABASE_URL and asyncpg is not None:
        try:
            _pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
            async with _pg_pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        min_volume DOUBLE PRECISION DEFAULT 500000,
                        banned_coins TEXT DEFAULT '{}',
                        enable_upticks INTEGER DEFAULT 1,
                        enable_robots INTEGER DEFAULT 1,
                        min_robot_usd DOUBLE PRECISION DEFAULT 25000,
                        robot_config TEXT DEFAULT '{}',
                        discord_config TEXT DEFAULT '{}'
                    )
                    """
                )
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS upticks (
                        id SERIAL PRIMARY KEY,
                        utc_date TEXT NOT NULL,
                        utc_time TEXT NOT NULL,
                        exchange TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        price DOUBLE PRECISION,
                        limit_price DOUBLE PRECISION,
                        mark_price DOUBLE PRECISION,
                        volume_24h_usd DOUBLE PRECISION,
                        wall_usd DOUBLE PRECISION DEFAULT 0
                    )
                    """
                )
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS robots (
                        id SERIAL PRIMARY KEY,
                        utc_date TEXT NOT NULL,
                        utc_time TEXT NOT NULL,
                        exchange TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        price DOUBLE PRECISION,
                        total_usd DOUBLE PRECISION,
                        prints_count INTEGER,
                        avg_print_usd DOUBLE PRECISION,
                        avg_interval_sec DOUBLE PRECISION,
                        dominance_pct DOUBLE PRECISION,
                        volume_24h_usd DOUBLE PRECISION
                    )
                    """
                )
                for col_def in (
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS banned_coins TEXT DEFAULT '{}'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS enable_upticks INTEGER DEFAULT 1",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS enable_robots INTEGER DEFAULT 1",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS min_robot_usd DOUBLE PRECISION DEFAULT 25000",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS robot_config TEXT DEFAULT '{}'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS discord_config TEXT DEFAULT '{}'",
                    "ALTER TABLE upticks ADD COLUMN IF NOT EXISTS wall_usd DOUBLE PRECISION DEFAULT 0",
                ):
                    await conn.execute(col_def)
            _use_pg = True
            logger.info("База данных: PostgreSQL подключена.")
            return
        except Exception as exc:
            logger.warning("Ошибка PostgreSQL (%s) — переключаюсь на SQLite.", exc)
            _pg_pool = None
            _use_pg = False

    async with aiosqlite.connect(SQLITE_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                min_volume REAL DEFAULT 500000,
                banned_coins TEXT DEFAULT '{}',
                enable_upticks INTEGER DEFAULT 1,
                enable_robots INTEGER DEFAULT 1,
                min_robot_usd REAL DEFAULT 25000,
                robot_config TEXT DEFAULT '{}',
                discord_config TEXT DEFAULT '{}'
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS upticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utc_date TEXT NOT NULL,
                utc_time TEXT NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL,
                limit_price REAL,
                mark_price REAL,
                volume_24h_usd REAL,
                wall_usd REAL DEFAULT 0
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS robots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utc_date TEXT NOT NULL,
                utc_time TEXT NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL,
                total_usd REAL,
                prints_count INTEGER,
                avg_print_usd REAL,
                avg_interval_sec REAL,
                dominance_pct REAL,
                volume_24h_usd REAL
            )
            """
        )
        cur = await db.execute("PRAGMA table_info(users)")
        cols = {row[1] for row in await cur.fetchall()}
        if "banned_coins" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN banned_coins TEXT DEFAULT '{}'")
        if "enable_upticks" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN enable_upticks INTEGER DEFAULT 1")
        if "enable_robots" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN enable_robots INTEGER DEFAULT 1")
        if "min_robot_usd" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN min_robot_usd REAL DEFAULT 25000")
        if "robot_config" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN robot_config TEXT DEFAULT '{}'")
        if "discord_config" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN discord_config TEXT DEFAULT '{}'")

        cur_up = await db.execute("PRAGMA table_info(upticks)")
        up_cols = {row[1] for row in await cur_up.fetchall()}
        if "wall_usd" not in up_cols:
            await db.execute("ALTER TABLE upticks ADD COLUMN wall_usd REAL DEFAULT 0")
        await db.commit()
    logger.info("База данных: SQLite (%s) готова.", SQLITE_PATH)


async def db_execute(query: str, params: Tuple[Any, ...] = ()) -> None:
    """Выполняет INSERT/UPDATE/DDL запрос в активную БД."""
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            await conn.execute(_to_pg_query(query), *params)
        return
    async with aiosqlite.connect(SQLITE_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def db_fetchone(query: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    """Возвращает одну строку запроса в виде словаря."""
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            row = await conn.fetchrow(_to_pg_query(query), *params)
            return dict(row) if row is not None else None
    async with aiosqlite.connect(SQLITE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        row = await cur.fetchone()
        return dict(row) if row is not None else None


async def db_fetchall(query: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    """Возвращает все строки запроса в виде списка словарей."""
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            rows = await conn.fetch(_to_pg_query(query), *params)
            return [dict(r) for r in rows]
    async with aiosqlite.connect(SQLITE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


# ==================== TELEGRAM BOT ====================

bot: Optional[Bot] = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()


def _build_main_keyboard() -> Optional[InlineKeyboardMarkup]:
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Открыть Скринер (Mini App)", web_app=WebAppInfo(url=WEBAPP_URL))]
        ]
    )


async def _broadcast_messages(
    user_ids: List[int], text: str, reply_markup: Optional[InlineKeyboardMarkup] = None
) -> None:
    if bot is None or not user_ids:
        return

    async def _send_one(uid: int) -> None:
        try:
            await bot.send_message(uid, text, parse_mode="HTML", reply_markup=reply_markup)
        except Exception as exc:
            logger.debug("Ошибка отправки пользователю %s: %s", uid, exc)

    # Отправляем пачками по 20 пользователей параллельно (убирает последовательную задержку)
    for i in range(0, len(user_ids), 20):
        batch = user_ids[i : i + 20]
        await asyncio.gather(*(_send_one(uid) for uid in batch))
        if i + 20 < len(user_ids):
            await asyncio.sleep(0.05)


# Кэш пользователей в памяти (чтобы не тратить 50–250мс на чтение БД перед каждым алертом)
_users_cache: List[Dict[str, Any]] = []
_users_cache_ts: float = 0.0


def invalidate_users_cache() -> None:
    global _users_cache_ts
    _users_cache_ts = 0.0


async def get_cached_users(force: bool = False) -> List[Dict[str, Any]]:
    global _users_cache, _users_cache_ts
    now = time.time()
    if not force and _users_cache_ts > 0.0 and (now - _users_cache_ts) < 60.0:
        return _users_cache
    rows = await db_fetchall(
        "SELECT user_id, min_volume, min_robot_usd, robot_config, banned_coins, "
        "enable_upticks, enable_robots, discord_config FROM users"
    )
    _users_cache = rows
    _users_cache_ts = now
    return rows


# Кэш для быстрого ответа /api/data
_api_cache: Dict[str, Any] = {"utc_date": "", "history": [], "robots": [], "ts": 0.0}


async def refresh_api_cache(force: bool = False) -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = time.time()
    if not force and _api_cache["utc_date"] == today_utc and (now - _api_cache["ts"]) < 0.8:
        return
    hist = await db_fetchall(
        "SELECT id, utc_time, exchange, symbol, side, price, limit_price, mark_price, volume_24h_usd, wall_usd "
        "FROM upticks WHERE utc_date = ? ORDER BY id DESC LIMIT 150",
        (today_utc,),
    )
    robs = await db_fetchall(
        "SELECT id, utc_time, exchange, symbol, side, price, total_usd, prints_count, "
        "avg_print_usd, avg_interval_sec, dominance_pct, volume_24h_usd "
        "FROM robots WHERE utc_date = ? ORDER BY id DESC LIMIT 150",
        (today_utc,),
    )
    _api_cache["utc_date"] = today_utc
    _api_cache["history"] = hist
    _api_cache["robots"] = robs
    _api_cache["ts"] = now


async def handle_uptick_event(event: UptickEvent) -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    emoji = "🟢" if event.side == UptickSide.LONG else "🔴"
    text = (
        f"⚡ <b>АПТИК | {event.exchange}</b>\n"
        f"Монета: <code>{event.symbol}</code> — {emoji} <b>{event.side.value}</b>\n"
        f"Цена: <code>{event.price:g}</code> | Планка: <code>{event.limit_price:g}</code>\n"
        f"🧱 Плотность: <b>${event.wall_usd:,.0f}</b> | 24ч: <b>${event.volume_24h_usd:,.0f}</b>\n"
        f"Время (UTC): {event.timestamp_utc}"
    )

    # 1) Сразу берём пользователей из RAM-кэша (0мс) и МГНОВЕННО шлём в Discord и Telegram до записи в БД
    users = await get_cached_users()
    base = extract_base_asset(event.symbol)
    target_ids: List[int] = []
    discord_urls: List[str] = []
    for u in users:
        uid = int(u["user_id"])
        if uid <= 0:
            continue
        if int(u.get("enable_upticks") if u.get("enable_upticks") is not None else 1) == 0:
            continue
        banned_map = parse_banned_coins(u.get("banned_coins"))
        if is_coin_banned(base, banned_map, "UPTICK"):
            continue
        if event.volume_24h_usd >= float(u.get("min_volume") or DEFAULT_MIN_VOLUME):
            target_ids.append(uid)
            dcfg = normalize_discord_config(u.get("discord_config"))
            wurl = resolve_webhook_url(dcfg, "UPTICK")
            if wurl and wurl not in discord_urls:
                discord_urls.append(wurl)

    # Если в БД ещё нет пользователей, но задан Discord Webhook в Render Environment
    if not users and event.volume_24h_usd >= DEFAULT_MIN_VOLUME:
        env_up, _ = get_env_webhooks()
        if env_up:
            discord_urls.append(env_up)

    for wurl in discord_urls:
        asyncio.create_task(send_uptick_discord(wurl, event, session=_http_session))
    if target_ids:
        asyncio.create_task(_broadcast_messages(target_ids, text, _build_main_keyboard()))

    # 2) Запись в БД уже после старта отправки алертов (не тормозит уведомления)
    await db_execute(
        "INSERT INTO upticks (utc_date, utc_time, exchange, symbol, side, price, limit_price, "
        "mark_price, volume_24h_usd, wall_usd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            today_utc,
            event.timestamp_utc,
            event.exchange,
            event.symbol,
            event.side.value,
            event.price,
            event.limit_price,
            event.mark_price,
            event.volume_24h_usd,
            event.wall_usd,
        ),
    )
    _api_cache["ts"] = 0.0

    emoji = "🟢" if event.side == UptickSide.LONG else "🔴"
    text = (
        f"⚡ <b>АПТИК | {event.exchange}</b>\n"
        f"Монета: <code>{event.symbol}</code> — {emoji} <b>{event.side.value}</b>\n"
        f"Цена: <code>{event.price:g}</code> | Планка: <code>{event.limit_price:g}</code>\n"
        f"🧱 Плотность: <b>${event.wall_usd:,.0f}</b> | 24ч: <b>${event.volume_24h_usd:,.0f}</b>\n"
        f"Время (UTC): {event.timestamp_utc}"
    )

    users = await db_fetchall("SELECT user_id, min_volume, banned_coins, enable_upticks, discord_config FROM users")
    base = extract_base_asset(event.symbol)
    target_ids: List[int] = []
    discord_urls: List[str] = []
    for u in users:
        uid = int(u["user_id"])
        if uid <= 0:
            continue
        if int(u.get("enable_upticks") if u.get("enable_upticks") is not None else 1) == 0:
            continue
        banned_map = parse_banned_coins(u.get("banned_coins"))
        if is_coin_banned(base, banned_map, "UPTICK"):
            continue
        if event.volume_24h_usd >= float(u.get("min_volume") or DEFAULT_MIN_VOLUME):
            target_ids.append(uid)
            dcfg = normalize_discord_config(u.get("discord_config"))
            wurl = resolve_webhook_url(dcfg, "UPTICK")
            if wurl and wurl not in discord_urls:
                discord_urls.append(wurl)

    # Если в БД ещё нет пользователей, но задан Discord Webhook в Render Environment
    if not users and event.volume_24h_usd >= DEFAULT_MIN_VOLUME:
        env_up, _ = get_env_webhooks()
        if env_up:
            discord_urls.append(env_up)

    for wurl in discord_urls:
        asyncio.create_task(send_uptick_discord(wurl, event, session=_http_session))

    await _broadcast_messages(target_ids, text, _build_main_keyboard())


async def handle_robot_event(event: RobotEvent) -> None:
    if is_robot_blacklisted_symbol(event.symbol, event.exchange):
        return
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await db_execute(
        "INSERT INTO robots (utc_date, utc_time, exchange, symbol, side, price, total_usd, "
        "prints_count, avg_print_usd, avg_interval_sec, dominance_pct, volume_24h_usd) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            today_utc,
            event.timestamp_utc,
            event.exchange,
            event.symbol,
            event.side.value,
            event.price,
            event.total_usd,
            event.prints_count,
            event.avg_print_usd,
            event.avg_interval_sec,
            event.dominance_pct,
            event.volume_24h_usd,
        ),
    )
    _api_cache["ts"] = 0.0

    is_buyer = event.side == RobotSide.BUY
    label = "ПОКУПАШ" if is_buyer else "ПРОДАВАШ"
    emoji = "🟢" if is_buyer else "🔴"
    text = (
        f"🤖 <b>{label} | {event.exchange}</b>\n"
        f"Монета: <code>{event.symbol}</code>\n"
        f"{emoji} Объём ({event.window_sec}с): <b>${event.total_usd:,.0f}</b> ({event.dominance_pct:.0f}% перекос)\n"
        f"Принтов: <b>{event.prints_count}</b> · ~<b>${event.avg_print_usd:,.0f}</b> / раз в <b>{event.avg_interval_sec:g}с</b>\n"
        f"Цена: <code>{event.price:g}</code> | 24ч: <b>${event.volume_24h_usd:,.0f}</b>\n"
        f"Время (UTC): {event.timestamp_utc}"
    )

    users = await db_fetchall(
        "SELECT user_id, min_robot_usd, robot_config, banned_coins, enable_robots, discord_config FROM users"
    )
    base = extract_base_asset(event.symbol)
    target_ids: List[int] = []
    discord_urls: List[str] = []
    for u in users:
        uid = int(u["user_id"])
        if uid <= 0:
            continue
        if int(u.get("enable_robots") if u.get("enable_robots") is not None else 1) == 0:
            continue
        banned_map = parse_banned_coins(u.get("banned_coins"))
        if is_coin_banned(base, banned_map, "ROBOT"):
            continue
        rcfg = normalize_robot_config(u.get("robot_config"), fallback_min_total=u.get("min_robot_usd"))
        if robot_matches_user_config(
            event.exchange,
            event.side.value,
            event.total_usd,
            event.prints_count,
            event.avg_print_usd,
            event.avg_interval_sec,
            event.dominance_pct,
            event.volume_24h_usd,
            rcfg,
        ):
            target_ids.append(uid)
            dcfg = normalize_discord_config(u.get("discord_config"))
            wurl = resolve_webhook_url(dcfg, "ROBOT")
            if wurl and wurl not in discord_urls:
                discord_urls.append(wurl)

    # Если в БД ещё нет пользователей, но задан Discord Webhook в Render Environment
    if not users and robot_matches_user_config(
        event.exchange,
        event.side.value,
        event.total_usd,
        event.prints_count,
        event.avg_print_usd,
        event.avg_interval_sec,
        event.dominance_pct,
        event.volume_24h_usd,
        robot_engine.config,
    ):
        _, env_rob = get_env_webhooks()
        if env_rob:
            discord_urls.append(env_rob)

    for wurl in discord_urls:
        asyncio.create_task(send_robot_discord(wurl, event, session=_http_session))

    await _broadcast_messages(target_ids, text, _build_main_keyboard())


engine = UptickEngine(on_uptick=handle_uptick_event)
robot_engine = RobotEngine(on_robot=handle_robot_event)


async def load_engine_robot_config() -> None:
    """Загружает сохранённую конфигурацию Покупаша/Продаваша из БД при старте."""
    try:
        rows = await db_fetchall(
            "SELECT user_id, min_robot_usd, robot_config FROM users "
            "WHERE robot_config IS NOT NULL AND robot_config != '{}'"
        )
        if rows:
            row = rows[-1]
            rcfg = normalize_robot_config(row.get("robot_config"), fallback_min_total=row.get("min_robot_usd"))
            robot_engine.update_config(rcfg)
    except Exception as exc:
        logger.debug("Не удалось загрузить robot_config из БД: %s", exc)


async def notify_bot_updated() -> None:
    """Отправляет уведомление в Telegram и Discord после успешного деплоя / запуска бота."""
    await asyncio.sleep(4)
    try:
        users = await db_fetchall("SELECT user_id, discord_config FROM users")
        target_ids = [int(u["user_id"]) for u in users if int(u.get("user_id") or 0) > 0]
        commit_sha = (os.getenv("RENDER_GIT_COMMIT") or "").strip()[:7]
        commit_line = f"\nВерсия: <code>{commit_sha}</code>" if commit_sha else ""
        now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        rcfg = robot_engine.config

        discord_urls: List[str] = []
        for u in users:
            dcfg = normalize_discord_config(u.get("discord_config"))
            for kind in ("UPTICK", "ROBOT"):
                wurl = resolve_webhook_url(dcfg, kind)
                if wurl and wurl not in discord_urls:
                    discord_urls.append(wurl)
        if not users:
            env_up, env_rob = get_env_webhooks()
            for u_env in (env_up, env_rob):
                if u_env and u_env not in discord_urls:
                    discord_urls.append(u_env)

        if discord_urls:
            asyncio.create_task(
                send_startup_discord(discord_urls, commit_sha, now_utc, rcfg, session=_http_session)
            )

        if not target_ids:
            return
        text = (
            "🚀 <b>Бот обновлён и запущен!</b>"
            f"{commit_line}\n"
            f"Время запуска: <code>{now_utc}</code>\n"
            "• ⚡ Аптики (WebSocket · 4 биржи): активны\n"
            f"• 🤖 Покупаш/Продаваш (WebSocket · {len(rcfg['exchanges'])} бирж): "
            f"от <b>${rcfg['min_total_usd']:,.0f}</b> / {rcfg['window_sec']:g}с\n\n"
            "Скринер работает 24/7."
        )
        await _broadcast_messages(target_ids, text, _build_main_keyboard())
        logger.info("Уведомление об обновлении отправлено (%s польз.)", len(target_ids))
    except Exception as exc:
        logger.warning("Не удалось отправить уведомление об обновлении: %s", exc)


@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    if not message.from_user:
        return
    await db_execute(
        "INSERT INTO users (user_id) VALUES (?) ON CONFLICT(user_id) DO NOTHING",
        (message.from_user.id,),
    )
    invalidate_users_cache()
    text = (
        "👋 <b>Мультибиржевой скринер Аптиков и Роботов (WebSocket)</b>\n\n"
        "• ⚡ <b>Аптики</b> (Binance, Bybit, OKX, Aster) — проверка реальной плотности у планки (включая акции и металлы)\n"
        "• 🤖 <b>Покупаш / Продаваш</b> (Binance, Bybit, OKX, Bitget, Gate, Aster) — полная настройка под себя\n\n"
        "<b>Команды:</b>\n"
        "/status — активные сигналы сейчас\n"
        "/robotcfg — текущие настройки Покупаша / Продаваша\n"
        "/upticks on|off — вкл/выкл Аптики\n"
        "/robots on|off — вкл/выкл Покупаш / Продаваш\n"
        "/minvol 500000 — мин. оборот 24ч для Аптиков ($)\n"
        "/robotmin 25000 — мин. объём Покупаша / Продаваша ($)\n"
        "/ban COIN [uptick|robot|all] — скрыть монету\n"
        "/unban COIN — вернуть монету"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=_build_main_keyboard())


@dp.message(Command("status"))
async def cmd_status(message: types.Message) -> None:
    upticks = engine.get_active_upticks()
    robots = robot_engine.get_active_robots()
    lines: List[str] = []
    if upticks:
        lines.append("⚡ <b>Активные аптики сейчас:</b>")
        for ex, sym, side, _, wall in upticks[:10]:
            em = "🟢" if side == UptickSide.LONG else "🔴"
            lines.append(f"• {ex} | <code>{sym}</code> — {em} <b>{side.value}</b> (🧱 ${wall:,.0f})")
    else:
        lines.append("⚡ Активных аптиков прямо сейчас нет.")
    lines.append("")
    if robots:
        lines.append("🤖 <b>Покупаш / Продаваш сейчас:</b>")
        for ex, sym, side, usd, cnt, avg_int, avg_pr, _, _ in robots[:10]:
            lbl = "🟢 ПОКУПАШ" if side == RobotSide.BUY else "🔴 ПРОДАВАШ"
            lines.append(f"• {ex} | <code>{sym}</code> — <b>{lbl}</b> (${usd:,.0f}, {cnt}× ~${avg_pr:,.0f} / {avg_int:.1f}с)")
    else:
        lines.append("🤖 Активных Покупашей / Продавашей сейчас нет.")
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=_build_main_keyboard())


@dp.message(Command("minvol"))
async def cmd_minvol(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        parts = (message.text or "").split()
        val = max(0.0, float(parts[1]))
        await db_execute(
            "INSERT INTO users (user_id, min_volume) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET min_volume = excluded.min_volume",
            (message.from_user.id, val),
        )
        invalidate_users_cache()
        await message.answer(f"✅ Мин. оборот 24ч для Аптиков: <b>${val:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/minvol 500000</code>", parse_mode="HTML")


@dp.message(Command("upticks"))
async def cmd_upticks_toggle(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    arg = parts[1].strip().lower() if len(parts) > 1 else ""
    if arg not in ("on", "off", "1", "0"):
        await message.answer("Использование: <code>/upticks on</code> или <code>/upticks off</code>", parse_mode="HTML")
        return
    en = 1 if arg in ("on", "1") else 0
    await db_execute(
        "INSERT INTO users (user_id, enable_upticks) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET enable_upticks = excluded.enable_upticks",
        (message.from_user.id, en),
    )
    invalidate_users_cache()
    await message.answer(f"⚡ Аптики: <b>{'ВКЛЮЧЕНЫ' if en else 'ВЫКЛЮЧЕНЫ'}</b>", parse_mode="HTML")


@dp.message(Command("robots"))
async def cmd_robots_toggle(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    arg = parts[1].strip().lower() if len(parts) > 1 else ""
    if arg not in ("on", "off", "1", "0"):
        await message.answer("Использование: <code>/robots on</code> или <code>/robots off</code>", parse_mode="HTML")
        return
    en = 1 if arg in ("on", "1") else 0
    await db_execute(
        "INSERT INTO users (user_id, enable_robots) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET enable_robots = excluded.enable_robots",
        (message.from_user.id, en),
    )
    invalidate_users_cache()
    await message.answer(f"🤖 Покупаш / Продаваш: <b>{'ВКЛЮЧЕНЫ' if en else 'ВЫКЛЮЧЕНЫ'}</b>", parse_mode="HTML")


@dp.message(Command("ban"))
async def cmd_ban(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: <code>/ban WIF</code> или <code>/ban WIF robot</code>", parse_mode="HTML")
        return
    base = extract_base_asset(parts[1])
    if not base:
        await message.answer("Некорректный тикер монеты.")
        return
    scope_raw = parts[2].strip().upper() if len(parts) > 2 else "ALL"
    scope = scope_raw if scope_raw in ("ALL", "UPTICK", "ROBOT") else "ALL"
    row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (message.from_user.id,))
    bmap = parse_banned_coins(row.get("banned_coins") if row else None)
    bmap[base] = scope
    await db_execute(
        "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
        (message.from_user.id, json.dumps(bmap)),
    )
    invalidate_users_cache()
    await message.answer(f"🚫 Монета <b>{base}</b> скрыта ({scope}).", parse_mode="HTML")


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: <code>/unban WIF</code>", parse_mode="HTML")
        return
    base = extract_base_asset(parts[1])
    row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (message.from_user.id,))
    bmap = parse_banned_coins(row.get("banned_coins") if row else None)
    bmap.pop(base, None)
    await db_execute(
        "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
        (message.from_user.id, json.dumps(bmap)),
    )
    invalidate_users_cache()
    await message.answer(f"✅ Монета <b>{base}</b> снова активна.", parse_mode="HTML")


@dp.message(Command("robotmin"))
async def cmd_robotmin(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        parts = (message.text or "").split()
        val = max(2000.0, float(parts[1]))
        row = await db_fetchone("SELECT robot_config FROM users WHERE user_id = ?", (message.from_user.id,))
        rcfg = normalize_robot_config(row.get("robot_config") if row else None, fallback_min_total=val)
        rcfg["min_total_usd"] = round(val, 2)
        await db_execute(
            "INSERT INTO users (user_id, min_robot_usd, robot_config) VALUES (?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET min_robot_usd = excluded.min_robot_usd, robot_config = excluded.robot_config",
            (message.from_user.id, val, json.dumps(rcfg)),
        )
        invalidate_users_cache()
        robot_engine.update_config(rcfg)
        await message.answer(f"✅ Мин. объём Покупаша / Продаваша: <b>${val:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/robotmin 25000</code>", parse_mode="HTML")


@dp.message(Command("robotcfg"))
async def cmd_robotcfg(message: types.Message) -> None:
    if not message.from_user:
        return
    row = await db_fetchone("SELECT min_robot_usd, robot_config FROM users WHERE user_id = ?", (message.from_user.id,))
    rcfg = normalize_robot_config(
        row.get("robot_config") if row else None,
        fallback_min_total=row.get("min_robot_usd") if row else None,
    )
    max_p = f"${rcfg['max_print_usd']:,.0f}" if rcfg["max_print_usd"] > 0 else "без лимита"
    side_txt = {"BOTH": "Покупаш + Продаваш", "BUY": "Только Покупаш", "SELL": "Только Продаваш"}[rcfg["side_mode"]]
    text = (
        "⚙️ <b>Настройки Покупаш / Продаваш:</b>\n\n"
        f"• Направление: <b>{side_txt}</b>\n"
        f"• Окно анализа: <b>{rcfg['window_sec']:g} сек</b>\n"
        f"• Размер 1 удара: <b>${rcfg['min_print_usd']:,.0f} — {max_p}</b>\n"
        f"• Общий объём за окно: <b>от ${rcfg['min_total_usd']:,.0f}</b>\n"
        f"• Кол-во ударов: <b>от {rcfg['min_prints']} шт</b>\n"
        f"• Интервал между ударами: <b>{rcfg['min_interval_sec']:g} — {rcfg['max_interval_sec']:g} сек</b>\n"
        f"• Перекос дельты: <b>от {rcfg['min_dominance']:g}%</b>\n"
        f"• Мин. оборот 24ч: <b>${rcfg['min_vol24h']:,.0f}</b>\n"
        f"• Пауза повтора: <b>{rcfg['cooldown_sec']:g} сек</b>\n"
        f"• Строгий ритм/лоты: <b>{'Да' if rcfg['strict_pattern'] else 'Нет'}</b>\n"
        f"• Биржи: <b>{', '.join(rcfg['exchanges'])}</b>\n\n"
        "Изменить все параметры удобно в Mini App → вкладка <b>⚙️ Настройки</b>."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=_build_main_keyboard())


# ==================== MINI APP HTML ====================

WEBAPP_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Uptick & Robot Screener</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        :root {
            --bg: #0c0f16;
            --card: #151a24;
            --card-2: #1b2230;
            --text: #eef2f8;
            --hint: #8b95a7;
            --border: rgba(255,255,255,0.08);
            --long: #16c784;
            --short: #ea3943;
            --gold: #f5b942;
            --accent: #3b82f6;
        }
        * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
        body {
            margin: 0; padding: 12px 12px 84px;
            background: var(--bg); color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            font-size: 14px;
        }
        .topbar {
            display: flex; align-items: center; justify-content: space-between;
            gap: 8px; margin-bottom: 10px;
        }
        .brand { font-size: 16px; font-weight: 800; letter-spacing: 0.2px; display: flex; align-items: center; gap: 6px; }
        .top-actions { display: flex; gap: 6px; }
        .chip-btn {
            border: 1px solid var(--border); background: var(--card); color: var(--text);
            padding: 6px 10px; border-radius: 10px; font-size: 12px; font-weight: 700;
            cursor: pointer; display: inline-flex; align-items: center; gap: 4px;
        }
        .chip-btn.active { border-color: rgba(22,199,132,0.55); background: rgba(22,199,132,0.14); color: var(--long); }
        .chip-btn.muted { border-color: rgba(234,57,67,0.55); background: rgba(234,57,67,0.14); color: var(--short); }

        .mode-bar { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 10px; }
        .mode-pill {
            display: flex; align-items: center; justify-content: space-between;
            padding: 8px 10px; border-radius: 11px; border: 1px solid var(--border);
            background: var(--card); color: var(--hint); font-size: 12px; font-weight: 700;
            cursor: pointer;
        }
        .mode-pill.on { border-color: rgba(22,199,132,0.45); color: var(--text); }
        .mode-pill .st { font-size: 10px; padding: 2px 6px; border-radius: 6px; background: rgba(255,255,255,0.06); }
        .mode-pill.on .st { background: rgba(22,199,132,0.2); color: var(--long); }

        .tabs {
            display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px;
            background: var(--card); padding: 4px; border-radius: 12px;
            border: 1px solid var(--border); margin-bottom: 12px;
        }
        .tab-btn {
            border: none; background: transparent; color: var(--hint);
            padding: 8px 6px; border-radius: 9px; font-size: 12px; font-weight: 700; cursor: pointer;
        }
        .tab-btn.active { background: var(--accent); color: #fff; }
        .panel { display: none; }
        .panel.active { display: block; }

        .section-title {
            display: flex; justify-content: space-between; align-items: center;
            font-size: 12px; font-weight: 700; color: var(--hint);
            text-transform: uppercase; letter-spacing: 0.4px; margin: 12px 2px 8px;
        }
        .live-box {
            display: flex; flex-direction: column; gap: 6px;
            background: var(--card); border: 1px solid var(--border);
            border-radius: 12px; padding: 10px; min-height: 44px;
        }
        .empty { color: var(--hint); font-size: 12px; text-align: center; padding: 10px 4px; }

        .filter-row { display: flex; gap: 6px; overflow-x: auto; padding-bottom: 6px; margin-bottom: 6px; }
        .f-pill {
            border: 1px solid var(--border); background: var(--card); color: var(--hint);
            padding: 5px 10px; border-radius: 999px; font-size: 11px; font-weight: 700;
            cursor: pointer; white-space: nowrap;
        }
        .f-pill.active { background: var(--text); color: var(--bg); border-color: var(--text); }

        .card {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 12px; padding: 10px 12px; margin-bottom: 8px;
            border-left: 3px solid var(--accent); cursor: pointer;
        }
        .card.long { border-left-color: var(--long); }
        .card.short { border-left-color: var(--short); }
        .card-top { display: flex; justify-content: space-between; align-items: center; gap: 6px; margin-bottom: 5px; }
        .sym-wrap { display: flex; align-items: center; gap: 6px; font-weight: 800; font-size: 14px; }
        .ex-tag {
            font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 6px;
            background: rgba(255,255,255,0.08); color: var(--text);
        }
        .side-badge {
            font-size: 11px; font-weight: 800; padding: 2px 7px; border-radius: 6px;
        }
        .side-badge.long { background: rgba(22,199,132,0.18); color: var(--long); }
        .side-badge.short { background: rgba(234,57,67,0.18); color: var(--short); }
        .wall-badge {
            font-size: 11px; font-weight: 800; padding: 2px 7px; border-radius: 6px;
            background: rgba(245,185,66,0.18); color: var(--gold);
        }
        .ban-x {
            border: none; background: rgba(234,57,67,0.16); color: var(--short);
            width: 22px; height: 22px; border-radius: 6px; font-weight: 800;
            font-size: 12px; cursor: pointer; display: inline-flex; align-items: center; justify-content: center;
        }
        .card-meta { display: flex; justify-content: space-between; flex-wrap: wrap; gap: 6px; font-size: 12px; color: var(--hint); }
        .card-meta b { color: var(--text); }

        .setting-box {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 12px; padding: 12px; margin-bottom: 10px;
        }
        .setting-label { display: block; font-size: 13px; font-weight: 800; margin-bottom: 8px; }
        .meta { font-size: 12px; color: var(--hint); }
        .toggle-row {
            display: flex; align-items: center; justify-content: space-between;
            padding: 6px 0; font-size: 13px;
        }
        .toggle-row input[type="checkbox"] { width: 18px; height: 18px; accent-color: var(--long); }
        input[type="number"], input[type="text"], select {
            background: var(--card-2); color: var(--text); border: 1px solid var(--border);
            border-radius: 9px; padding: 8px 10px; font-size: 13px; outline: none;
        }
        .input-row { display: flex; gap: 8px; }
        .input-row input { flex: 1; }
        .btn {
            border: none; background: var(--accent); color: #fff;
            padding: 9px 14px; border-radius: 9px; font-size: 13px; font-weight: 700; cursor: pointer;
        }
        .btn.sec { background: var(--card-2); color: var(--text); border: 1px solid var(--border); }
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 8px; }
        .field-group { display: flex; flex-direction: column; gap: 4px; }
        .field-group input, .field-group select { width: 100%; }
        .field-lbl { font-size: 11px; color: var(--hint); font-weight: 600; }
        .preset-row { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; margin-bottom: 10px; }
        .preset-btn {
            padding: 8px 6px; border-radius: 9px; border: 1px solid var(--border);
            background: rgba(255,255,255,0.04); color: var(--text); font-size: 11px;
            font-weight: 700; cursor: pointer;
        }
        .preset-btn:active { background: var(--accent); border-color: var(--accent); color: #fff; }
        .cfg-summary {
            display: flex; align-items: center; justify-content: space-between; gap: 8px;
            padding: 8px 10px; border-radius: 10px; background: var(--card);
            border: 1px solid var(--border); font-size: 11px; color: var(--hint); margin-bottom: 10px;
        }
        .cfg-summary b { color: var(--text); }
        .cfg-edit-btn {
            border: 1px solid var(--border); background: rgba(255,255,255,0.06); color: var(--text);
            border-radius: 8px; padding: 4px 8px; font-size: 11px; font-weight: 700; cursor: pointer; white-space: nowrap;
        }
        .ex-checks { display: flex; flex-wrap: wrap; gap: 6px; margin: 6px 0 10px; }
        .ex-chk {
            display: inline-flex; align-items: center; gap: 5px; padding: 6px 9px;
            border-radius: 9px; border: 1px solid var(--border); background: rgba(255,255,255,0.03);
            color: var(--hint); font-size: 12px; font-weight: 700; cursor: pointer;
        }
        .ex-chk.on { border-color: rgba(22,199,132,0.55); background: rgba(22,199,132,0.14); color: var(--text); }
        .ban-tags { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
        .ban-tag {
            display: inline-flex; align-items: center; gap: 6px; padding: 5px 9px;
            border-radius: 8px; background: rgba(234,57,67,0.14); border: 1px solid rgba(234,57,67,0.35);
            font-size: 12px; font-weight: 700;
        }
        .ban-tag button { border: none; background: transparent; color: var(--short); font-weight: 800; cursor: pointer; }

        .modal-bg {
            position: fixed; inset: 0; background: rgba(0,0,0,0.68);
            display: none; align-items: center; justify-content: center; padding: 16px; z-index: 100;
        }
        .modal-bg.open { display: flex; }
        .modal {
            width: 100%; max-width: 340px; background: var(--card); border: 1px solid var(--border);
            border-radius: 14px; padding: 16px; display: flex; flex-direction: column; gap: 8px;
        }
        .toast {
            position: fixed; bottom: 18px; left: 50%; transform: translateX(-50%);
            background: #1f2937; color: #fff; padding: 9px 14px; border-radius: 10px;
            font-size: 12px; font-weight: 700; opacity: 0; pointer-events: none;
            transition: opacity 0.2s; z-index: 200; border: 1px solid var(--border);
        }
        .toast.show { opacity: 1; }
    </style>
</head>
<body>
    <div class="topbar">
        <div class="brand">⚡ Screener <span style="font-size:11px;color:var(--long)">WS</span></div>
        <div class="top-actions">
            <button id="mute-btn" class="chip-btn active" onclick="toggleMute()">🔊 Звук</button>
            <button id="tiger-btn" class="chip-btn" onclick="connectTiger(true)">🐯 Tiger</button>
        </div>
    </div>

    <div class="mode-bar">
        <button id="pill-upticks" class="mode-pill on" onclick="quickToggleFeature('upticks')">
            <span>⚡ Аптики</span><span class="st" id="pill-up-st">ON</span>
        </button>
        <button id="pill-robots" class="mode-pill on" onclick="quickToggleFeature('robots')">
            <span>🤖 Покупаш/Прод.</span><span class="st" id="pill-rob-st">ON</span>
        </button>
    </div>

    <div class="tabs">
        <button class="tab-btn active" id="btn-tab-upticks" onclick="switchTab('upticks')">⚡ Аптики</button>
        <button class="tab-btn" id="btn-tab-robots" onclick="switchTab('robots')">🤖 Покупаш/Прод.</button>
        <button class="tab-btn" id="btn-tab-settings" onclick="switchTab('settings')">⚙️ Настройки</button>
    </div>

    <!-- ВКЛАДКА АПТИКИ -->
    <div id="tab-upticks" class="panel active">
        <div class="section-title">
            <span>🔴 Активные аптики сейчас (WS · 4 биржи)</span>
            <span id="live-up-cnt">0</span>
        </div>
        <div class="live-box" id="live-active-upticks"></div>

        <div class="section-title"><span>📜 История аптиков за сегодня (UTC)</span></div>
        <div class="filter-row" id="uptick-ex-filters"></div>
        <div id="upticks-list"></div>
    </div>

    <!-- ВКЛАДКА ПОКУПАШ / ПРОДАВАШ -->
    <div id="tab-robots" class="panel">
        <div class="cfg-summary">
            <span id="robot-cfg-summary">Окно 25с · Удар $1K+ · 0.35–6с · ≥6 шт · ≥$25K</span>
            <button class="cfg-edit-btn" onclick="switchTab('settings')">⚙️ Настроить</button>
        </div>
        <div class="section-title">
            <span>🤖 Активные роботы сейчас (WS · 6 бирж)</span>
            <span id="live-rob-cnt">0</span>
        </div>
        <div class="live-box" id="live-active-robots"></div>

        <div class="section-title"><span>📜 История Покупаш / Продаваш (UTC)</span></div>
        <div class="filter-row" id="robot-ex-filters"></div>
        <div id="robots-list"></div>
    </div>

    <!-- ВКЛАДКА НАСТРОЙКИ -->
    <div id="tab-settings" class="panel">
        <div class="setting-box">
            <label class="setting-label">🎛 Включение функций (Telegram + Mini App)</label>
            <div class="toggle-row">
                <span>⚡ Аптики (Binance, Bybit, OKX, Aster)</span>
                <input type="checkbox" id="set-en-upticks" checked onchange="saveGlobalSettings()">
            </div>
            <div class="toggle-row">
                <span>🤖 Покупаш / Продаваш (6 бирж)</span>
                <input type="checkbox" id="set-en-robots" checked onchange="saveGlobalSettings()">
            </div>
        </div>

        <div class="setting-box">
            <label class="setting-label">🤖 Полная настройка Покупаша / Продаваша</label>
            <div class="meta" style="margin-bottom:8px;">Быстрые пресеты (или задайте свои тайминги и суммы ниже):</div>
            <div class="preset-row">
                <button class="preset-btn" onclick="applyRobotPreset('scalp')">⚡ Скальп</button>
                <button class="preset-btn" onclick="applyRobotPreset('standard')">⚖️ Стандарт</button>
                <button class="preset-btn" onclick="applyRobotPreset('whale')">🐋 Крупный</button>
            </div>

            <div class="field-group" style="margin-bottom:8px;">
                <span class="field-lbl">Кого отслеживать</span>
                <select id="set-rob-side">
                    <option value="BOTH">🟢🔴 Оба (Покупаш + Продаваш)</option>
                    <option value="BUY">🟢 Только Покупаш</option>
                    <option value="SELL">🔴 Только Продаваш</option>
                </select>
            </div>

            <div class="meta" style="margin:6px 0 4px; font-weight:700; color:var(--text);">💵 По сколько покупает / продаёт</div>
            <div class="grid-2">
                <div class="field-group">
                    <span class="field-lbl">Мин. 1 удар ($)</span>
                    <input type="number" id="set-rob-min-print" value="1000" min="100" step="100">
                </div>
                <div class="field-group">
                    <span class="field-lbl">Макс. 1 удар ($ · 0 = ∞)</span>
                    <input type="number" id="set-rob-max-print" value="0" min="0" step="500">
                </div>
            </div>
            <div class="grid-2">
                <div class="field-group">
                    <span class="field-lbl">Мин. объём за окно ($)</span>
                    <input type="number" id="set-robot-usd" value="25000" min="2000" step="1000">
                </div>
                <div class="field-group">
                    <span class="field-lbl">Мин. кол-во ударов</span>
                    <input type="number" id="set-rob-min-prints" value="6" min="3" max="100" step="1">
                </div>
            </div>

            <div class="meta" style="margin:8px 0 4px; font-weight:700; color:var(--text);">⏱ Тайминги работы робота</div>
            <div class="grid-2">
                <div class="field-group">
                    <span class="field-lbl">Окно анализа (сек)</span>
                    <input type="number" id="set-rob-window" value="25" min="5" max="120" step="5">
                </div>
                <div class="field-group">
                    <span class="field-lbl">Пауза повтора (сек)</span>
                    <input type="number" id="set-rob-cooldown" value="180" min="15" max="3600" step="15">
                </div>
            </div>
            <div class="grid-2">
                <div class="field-group">
                    <span class="field-lbl">Мин. интервал ударов (сек)</span>
                    <input type="number" id="set-rob-min-int" value="0.35" min="0.1" max="60" step="0.1">
                </div>
                <div class="field-group">
                    <span class="field-lbl">Макс. интервал ударов (сек)</span>
                    <input type="number" id="set-rob-max-int" value="6" min="0.3" max="120" step="0.5">
                </div>
            </div>

            <div class="meta" style="margin:8px 0 4px; font-weight:700; color:var(--text);">🎯 Точность и фильтр рынка</div>
            <div class="grid-2">
                <div class="field-group">
                    <span class="field-lbl">Мин. перекос дельты (%)</span>
                    <input type="number" id="set-rob-dom" value="78" min="55" max="99" step="1">
                </div>
                <div class="field-group">
                    <span class="field-lbl">Мин. оборот 24ч ($)</span>
                    <input type="number" id="set-rob-vol24" value="300000" min="0" step="100000">
                </div>
            </div>

            <div class="toggle-row" style="margin-bottom:6px;">
                <span>Строгий режим (ровный лот / ритм TWAP)</span>
                <input type="checkbox" id="set-rob-strict" checked>
            </div>

            <div class="field-lbl">Биржи для Покупаша / Продаваша:</div>
            <div class="ex-checks" id="rob-ex-checks"></div>

            <button class="btn" style="width:100%" onclick="saveGlobalSettings()">💾 Сохранить настройки роботов</button>
        </div>

        <div class="setting-box">
            <label class="setting-label">⚡ Фильтр для Аптиков</label>
            <div class="meta" style="margin-bottom:6px;">Мин. суточный оборот 24ч ($) для Аптиков</div>
            <div class="input-row">
                <input type="number" id="set-vol" value="500000">
                <button class="btn" onclick="saveGlobalSettings()">Сохранить</button>
            </div>
        </div>

        <div class="setting-box">
            <label class="setting-label">💬 Discord-канал (Webhooks)</label>
            <div class="meta" style="margin-bottom:8px;">
                В Discord: Настройки канала → Интеграции → Вебхуки → Скопировать URL. Можно указать один общий канал или два разных.
            </div>
            <div class="toggle-row">
                <span>Включить отправку в Discord</span>
                <input type="checkbox" id="set-dc-enabled" checked>
            </div>
            <div class="toggle-row">
                <span>⚡ Отправлять Аптики в Discord</span>
                <input type="checkbox" id="set-dc-upticks" checked>
            </div>
            <div class="toggle-row" style="margin-bottom:8px;">
                <span>🤖 Отправлять Покупаш / Продаваш в Discord</span>
                <input type="checkbox" id="set-dc-robots" checked>
            </div>
            <div class="field-group" style="margin-bottom:8px;">
                <span class="field-lbl">Webhook URL для Аптиков (или общий канал)</span>
                <input type="text" id="set-dc-up-url" placeholder="https://discord.com/api/webhooks/...">
            </div>
            <div class="field-group" style="margin-bottom:10px;">
                <span class="field-lbl">Webhook URL для Роботов (опционально — если пусто, шлёт в верхний)</span>
                <input type="text" id="set-dc-rob-url" placeholder="https://discord.com/api/webhooks/...">
            </div>
            <div class="grid-2">
                <button class="btn" onclick="saveGlobalSettings()">💾 Сохранить Discord</button>
                <button class="btn sec" onclick="testDiscord()">🧪 Тест Discord</button>
            </div>
        </div>

        <div class="setting-box">
            <label class="setting-label">🔊 Звук и голосовые алерты</label>
            <div class="toggle-row">
                <span>Убрать весь звук (Mute)</span>
                <input type="checkbox" id="set-mute" onchange="toggleMute(this.checked)">
            </div>
            <div class="toggle-row">
                <span>Озвучка Аптиков (LONG / SHORT + плотность)</span>
                <input type="checkbox" id="snd-uptick" checked onchange="saveLocalPrefs()">
            </div>
            <div class="toggle-row">
                <span>Озвучка Покупаш / Продаваш</span>
                <input type="checkbox" id="snd-robot" checked onchange="saveLocalPrefs()">
            </div>
            <div class="grid-2" style="margin-top:6px;">
                <button class="btn sec" onclick="speakText('Аптик на бирже Бинанс. Виф. Лонг. Плотность 140 тысяч')">▶ Тест Аптик</button>
                <button class="btn sec" onclick="speakText('Покупаш на бирже Байбит. Суи. 45 тысяч долларов')">▶ Тест Покупаш</button>
            </div>
        </div>

        <div class="setting-box">
            <label class="setting-label">🐯 Tiger.Trade (авто-открытие)</label>
            <div class="toggle-row">
                <span>Авто-открывать Аптики</span>
                <input type="checkbox" id="tiger-auto-up" onchange="saveLocalPrefs()">
            </div>
            <div class="toggle-row">
                <span>Авто-открывать Покупаш / Продаваш</span>
                <input type="checkbox" id="tiger-auto-rob" onchange="saveLocalPrefs()">
            </div>
            <div class="toggle-row">
                <span>Режим Tiger Broker</span>
                <input type="checkbox" id="tiger-broker" onchange="saveLocalPrefs()">
            </div>
            <div class="grid-2" style="margin-top:6px;">
                <div class="field-group">
                    <span class="field-lbl">Группа линковки (1–8)</span>
                    <input type="number" id="tiger-group" value="1" min="1" max="8" onchange="saveLocalPrefs()">
                </div>
                <div class="field-group" style="justify-content:flex-end;">
                    <button class="btn sec" onclick="connectTiger(true)">Переподключить</button>
                </div>
            </div>
        </div>

        <div class="setting-box">
            <label class="setting-label">🚫 Убрать монеты (личный чёрный список)</label>
            <div class="grid-2">
                <input type="text" id="ban-input" placeholder="Тикер, напр. WIF">
                <select id="ban-scope">
                    <option value="ALL">Из обоих</option>
                    <option value="UPTICK">Только Аптики</option>
                    <option value="ROBOT">Только Роботы</option>
                </select>
            </div>
            <button class="btn" style="width:100%" onclick="addBannedFromInput()">Скрыть монету</button>
            <div class="ban-tags" id="banned-list"></div>
        </div>
    </div>

    <!-- МОДАЛКА СКРЫТИЯ МОНЕТЫ -->
    <div id="ban-modal-bg" class="modal-bg" onclick="closeBanModal()">
        <div class="modal" onclick="event.stopPropagation()">
            <div style="font-weight:800;font-size:15px;">🚫 Скрыть <span id="ban-modal-coin"></span></div>
            <div class="meta">Откуда убрать сигналы по этой монете?</div>
            <button class="btn sec" onclick="confirmBanModal('UPTICK')">⚡ Только из Аптиков</button>
            <button class="btn sec" onclick="confirmBanModal('ROBOT')">🤖 Только из Покупаш / Продаваш</button>
            <button class="btn" style="background:var(--short)" onclick="confirmBanModal('ALL')">🚫 Из обоих</button>
            <button class="btn sec" onclick="closeBanModal()">Отмена</button>
        </div>
    </div>

    <div id="toast" class="toast"></div>

    <script>
        const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
        if (tg) { tg.ready(); tg.expand(); }
        const userId = (tg && tg.initDataUnsafe && tg.initDataUnsafe.user && tg.initDataUnsafe.user.id)
            ? Number(tg.initDataUnsafe.user.id)
            : Number(new URLSearchParams(location.search).get('user_id') || 0);

        const UPTICK_EXCHANGES = ['Binance', 'Bybit', 'OKX', 'Aster'];
        const ROBOT_EXCHANGES = ['Binance', 'Bybit', 'OKX', 'Bitget', 'Gate', 'Aster'];
        const EX_VOICE = {
            Binance: 'Бинанс', Bybit: 'Байбит', OKX: 'Окэкс',
            Bitget: 'Битгет', Gate: 'Гейт', Aster: 'Астер'
        };

        let activeTab = 'upticks';
        let upExFilter = 'ALL';
        let robExFilter = 'ALL';
        let isMuted = false;
        let enableUpticks = true;
        let enableRobots = true;
        let cachedUpticks = [];
        let cachedActiveUp = [];
        let cachedRobots = [];
        let cachedActiveRob = [];
        let userBannedCoins = {};
        let selectedRobotExchanges = [...ROBOT_EXCHANGES];
        let settingsInitialized = false;
        let lastUptickId = 0;
        let lastRobotId = 0;
        let firstLoadDone = false;
        let tigerWs = null;
        let pendingBanCoin = '';

        function showToast(msg) {
            const t = document.getElementById('toast');
            t.textContent = msg;
            t.classList.add('show');
            clearTimeout(window._toastTimer);
            window._toastTimer = setTimeout(() => t.classList.remove('show'), 2400);
        }

        function fmtUsd(v) {
            const n = Number(v || 0);
            if (n >= 1e6) return '$' + (n / 1e6).toFixed(2) + 'M';
            if (n >= 1e3) return '$' + Math.round(n / 1e3) + 'K';
            return '$' + Math.round(n);
        }

        function fmtUsdVoice(v) {
            const n = Number(v || 0);
            if (n >= 1e6) return (n / 1e6).toFixed(1).replace('.', ' и ') + ' миллиона';
            if (n >= 1e3) return Math.round(n / 1e3) + ' тысяч';
            return Math.round(n) + ' долларов';
        }

        function baseCoin(sym) {
            return String(sym || '').toUpperCase().replace(/(-USDT-SWAP|_USDT|USDT|USDC)$/i, '').replace(/^(1000000|10000|1000)/, '');
        }

        function speakText(text) {
            if (isMuted) return;
            try {
                if ('speechSynthesis' in window) {
                    window.speechSynthesis.cancel();
                    const u = new SpeechSynthesisUtterance(text);
                    u.lang = 'ru-RU';
                    u.rate = 1.08;
                    window.speechSynthesis.speak(u);
                }
            } catch {}
        }

        function updateMuteUI() {
            const btn = document.getElementById('mute-btn');
            const chk = document.getElementById('set-mute');
            if (chk) chk.checked = isMuted;
            if (btn) {
                btn.className = 'chip-btn ' + (isMuted ? 'muted' : 'active');
                btn.textContent = isMuted ? '🔇 Выкл' : '🔊 Звук';
            }
        }

        function toggleMute(forceVal) {
            isMuted = typeof forceVal === 'boolean' ? forceVal : !isMuted;
            if (isMuted && 'speechSynthesis' in window) {
                try { window.speechSynthesis.cancel(); } catch {}
            }
            updateMuteUI();
            saveLocalPrefs();
            showToast(isMuted ? '🔇 Звук выключен' : '🔊 Звук включён');
        }

        function updateFeatureUI() {
            const upP = document.getElementById('pill-upticks');
            const robP = document.getElementById('pill-robots');
            upP.classList.toggle('on', enableUpticks);
            robP.classList.toggle('on', enableRobots);
            document.getElementById('pill-up-st').textContent = enableUpticks ? 'ON' : 'OFF';
            document.getElementById('pill-rob-st').textContent = enableRobots ? 'ON' : 'OFF';
            document.getElementById('set-en-upticks').checked = enableUpticks;
            document.getElementById('set-en-robots').checked = enableRobots;
        }

        function quickToggleFeature(which) {
            if (which === 'upticks') {
                enableUpticks = !enableUpticks;
                document.getElementById('set-en-upticks').checked = enableUpticks;
            } else {
                enableRobots = !enableRobots;
                document.getElementById('set-en-robots').checked = enableRobots;
            }
            updateFeatureUI();
            saveGlobalSettings(true);
        }

        function switchTab(name) {
            activeTab = name;
            ['upticks', 'robots', 'settings'].forEach(t => {
                document.getElementById('tab-' + t).classList.toggle('active', t === name);
                document.getElementById('btn-tab-' + t).classList.toggle('active', t === name);
            });
        }

        function renderExFilters() {
            document.getElementById('uptick-ex-filters').innerHTML = ['ALL', ...UPTICK_EXCHANGES].map(ex =>
                `<button class="f-pill ${upExFilter === ex ? 'active' : ''}" onclick="setUpFilter('${ex}')">${ex === 'ALL' ? 'Все' : ex}</button>`
            ).join('');
            document.getElementById('robot-ex-filters').innerHTML = ['ALL', ...ROBOT_EXCHANGES].map(ex =>
                `<button class="f-pill ${robExFilter === ex ? 'active' : ''}" onclick="setRobFilter('${ex}')">${ex === 'ALL' ? 'Все' : ex}</button>`
            ).join('');
        }

        function setUpFilter(ex) { upExFilter = ex; renderExFilters(); renderUpticks(); }
        function setRobFilter(ex) { robExFilter = ex; renderExFilters(); renderRobots(); }

        function renderRobotExChecks() {
            const box = document.getElementById('rob-ex-checks');
            if (!box) return;
            box.innerHTML = ROBOT_EXCHANGES.map(ex => {
                const on = selectedRobotExchanges.includes(ex);
                return `<button type="button" class="ex-chk ${on ? 'on' : ''}" onclick="toggleRobotEx('${ex}')">${ex}</button>`;
            }).join('');
        }

        function toggleRobotEx(ex) {
            if (selectedRobotExchanges.includes(ex)) {
                if (selectedRobotExchanges.length <= 1) {
                    showToast('Оставьте хотя бы 1 биржу');
                    return;
                }
                selectedRobotExchanges = selectedRobotExchanges.filter(x => x !== ex);
            } else {
                selectedRobotExchanges.push(ex);
            }
            renderRobotExChecks();
        }

        function readRobotConfigFromUI() {
            return {
                window_sec: Number(document.getElementById('set-rob-window').value || 25),
                min_print_usd: Number(document.getElementById('set-rob-min-print').value || 1000),
                max_print_usd: Number(document.getElementById('set-rob-max-print').value || 0),
                min_total_usd: Number(document.getElementById('set-robot-usd').value || 25000),
                min_prints: Number(document.getElementById('set-rob-min-prints').value || 6),
                min_interval_sec: Number(document.getElementById('set-rob-min-int').value || 0.35),
                max_interval_sec: Number(document.getElementById('set-rob-max-int').value || 6),
                min_dominance: Number(document.getElementById('set-rob-dom').value || 78),
                min_vol24h: Number(document.getElementById('set-rob-vol24').value || 300000),
                cooldown_sec: Number(document.getElementById('set-rob-cooldown').value || 180),
                side_mode: document.getElementById('set-rob-side').value || 'BOTH',
                strict_pattern: document.getElementById('set-rob-strict').checked,
                exchanges: [...selectedRobotExchanges]
            };
        }

        function readDiscordConfigFromUI() {
            return {
                enabled: document.getElementById('set-dc-enabled').checked,
                send_upticks: document.getElementById('set-dc-upticks').checked,
                send_robots: document.getElementById('set-dc-robots').checked,
                uptick_webhook: (document.getElementById('set-dc-up-url').value || '').trim(),
                robot_webhook: (document.getElementById('set-dc-rob-url').value || '').trim()
            };
        }

        function applyDiscordConfigToUI(cfg) {
            if (!cfg) return;
            document.getElementById('set-dc-enabled').checked = cfg.enabled !== false;
            document.getElementById('set-dc-upticks').checked = cfg.send_upticks !== false;
            document.getElementById('set-dc-robots').checked = cfg.send_robots !== false;
            document.getElementById('set-dc-up-url').value = cfg.uptick_webhook || '';
            document.getElementById('set-dc-rob-url').value = cfg.robot_webhook || '';
        }

        async function testDiscord() {
            const dcfg = readDiscordConfigFromUI();
            try {
                const res = await fetch('/api/discord/test', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, discord_config: dcfg })
                });
                const out = await res.json();
                showToast(out.ok ? '✅ Тест отправлен в Discord!' : ('❌ ' + (out.detail || 'Ошибка Discord')));
            } catch {
                showToast('❌ Ошибка проверки Discord');
            }
        }

        function applyRobotConfigToUI(cfg) {
            if (!cfg) return;
            document.getElementById('set-rob-window').value = cfg.window_sec ?? 25;
            document.getElementById('set-rob-min-print').value = cfg.min_print_usd ?? 1000;
            document.getElementById('set-rob-max-print').value = cfg.max_print_usd ?? 0;
            document.getElementById('set-robot-usd').value = cfg.min_total_usd ?? 25000;
            document.getElementById('set-rob-min-prints').value = cfg.min_prints ?? 6;
            document.getElementById('set-rob-min-int').value = cfg.min_interval_sec ?? 0.35;
            document.getElementById('set-rob-max-int').value = cfg.max_interval_sec ?? 6;
            document.getElementById('set-rob-dom').value = cfg.min_dominance ?? 78;
            document.getElementById('set-rob-vol24').value = cfg.min_vol24h ?? 300000;
            document.getElementById('set-rob-cooldown').value = cfg.cooldown_sec ?? 180;
            document.getElementById('set-rob-side').value = cfg.side_mode || 'BOTH';
            document.getElementById('set-rob-strict').checked = cfg.strict_pattern !== false;
            if (Array.isArray(cfg.exchanges) && cfg.exchanges.length) {
                selectedRobotExchanges = cfg.exchanges.filter(e => ROBOT_EXCHANGES.includes(e));
                if (!selectedRobotExchanges.length) selectedRobotExchanges = [...ROBOT_EXCHANGES];
            }
            renderRobotExChecks();
            updateRobotSummary();
        }

        function updateRobotSummary() {
            const c = readRobotConfigFromUI();
            const lotTxt = c.max_print_usd > 0
                ? `${fmtUsd(c.min_print_usd)}–${fmtUsd(c.max_print_usd)}`
                : `${fmtUsd(c.min_print_usd)}+`;
            const sideBadge = c.side_mode === 'BUY' ? '🟢 ' : (c.side_mode === 'SELL' ? '🔴 ' : '');
            const el = document.getElementById('robot-cfg-summary');
            if (el) {
                el.innerHTML = `${sideBadge}Окно <b>${c.window_sec}с</b> · Удар <b>${lotTxt}</b> · <b>${c.min_interval_sec}–${c.max_interval_sec}с</b> · ≥<b>${c.min_prints}</b> шт · ≥<b>${fmtUsd(c.min_total_usd)}</b>`;
            }
        }

        function applyRobotPreset(name) {
            const presets = {
                scalp: {
                    window_sec: 15, min_print_usd: 500, max_print_usd: 15000, min_total_usd: 10000,
                    min_prints: 5, min_interval_sec: 0.25, max_interval_sec: 3.0, min_dominance: 75,
                    min_vol24h: 200000, cooldown_sec: 90, side_mode: 'BOTH', strict_pattern: true,
                    exchanges: [...ROBOT_EXCHANGES]
                },
                standard: {
                    window_sec: 25, min_print_usd: 1000, max_print_usd: 0, min_total_usd: 25000,
                    min_prints: 6, min_interval_sec: 0.35, max_interval_sec: 6.0, min_dominance: 78,
                    min_vol24h: 300000, cooldown_sec: 180, side_mode: 'BOTH', strict_pattern: true,
                    exchanges: [...ROBOT_EXCHANGES]
                },
                whale: {
                    window_sec: 60, min_print_usd: 3000, max_print_usd: 0, min_total_usd: 80000,
                    min_prints: 8, min_interval_sec: 1.0, max_interval_sec: 15.0, min_dominance: 80,
                    min_vol24h: 1000000, cooldown_sec: 300, side_mode: 'BOTH', strict_pattern: true,
                    exchanges: [...ROBOT_EXCHANGES]
                }
            };
            if (presets[name]) {
                applyRobotConfigToUI(presets[name]);
                saveGlobalSettings();
            }
        }

        function loadLocalPrefs() {
            try {
                const p = JSON.parse(localStorage.getItem('uptick_prefs_v4') || '{}');
                if (typeof p.muted === 'boolean') isMuted = p.muted;
                if (typeof p.sndUp === 'boolean') document.getElementById('snd-uptick').checked = p.sndUp;
                if (typeof p.sndRob === 'boolean') document.getElementById('snd-robot').checked = p.sndRob;
                if (typeof p.autoUp === 'boolean') document.getElementById('tiger-auto-up').checked = p.autoUp;
                if (typeof p.autoRob === 'boolean') document.getElementById('tiger-auto-rob').checked = p.autoRob;
                if (typeof p.broker === 'boolean') document.getElementById('tiger-broker').checked = p.broker;
                if (typeof p.enUp === 'boolean') enableUpticks = p.enUp;
                if (typeof p.enRob === 'boolean') enableRobots = p.enRob;
                if (p.group) document.getElementById('tiger-group').value = p.group;
                if (p.robotCfg) applyRobotConfigToUI(p.robotCfg);
            } catch {}
            renderRobotExChecks();
            updateRobotSummary();
            updateMuteUI();
            updateFeatureUI();
        }

        function saveLocalPrefs() {
            localStorage.setItem('uptick_prefs_v4', JSON.stringify({
                muted: isMuted,
                sndUp: document.getElementById('snd-uptick').checked,
                sndRob: document.getElementById('snd-robot').checked,
                autoUp: document.getElementById('tiger-auto-up').checked,
                autoRob: document.getElementById('tiger-auto-rob').checked,
                broker: document.getElementById('tiger-broker').checked,
                enUp: enableUpticks,
                enRob: enableRobots,
                group: document.getElementById('tiger-group').value,
                robotCfg: readRobotConfigFromUI()
            }));
        }

        function connectTiger(manual = false) {
            const btn = document.getElementById('tiger-btn');
            try {
                if (tigerWs && tigerWs.readyState === 1) {
                    if (manual) showToast('🐯 Tiger.Trade уже подключён');
                    return;
                }
                tigerWs = new WebSocket('ws://127.0.0.1:21310');
                tigerWs.onopen = () => {
                    btn.className = 'chip-btn active';
                    btn.textContent = '🐯 ON';
                    if (manual) showToast('🐯 Подключено к Tiger.Trade');
                };
                tigerWs.onclose = () => {
                    btn.className = 'chip-btn';
                    btn.textContent = '🐯 Tiger';
                };
                tigerWs.onerror = () => {
                    btn.className = 'chip-btn';
                    btn.textContent = '🐯 Tiger';
                };
            } catch {}
        }

        function openInTiger(exchange, symbol) {
            if (!tigerWs || tigerWs.readyState !== 1) return;
            const grp = Number(document.getElementById('tiger-group').value || 1);
            const broker = document.getElementById('tiger-broker').checked;
            const exMap = {
                Binance: broker ? 'BINANCE_F' : 'BINANCEF',
                Bybit: 'BYBITF',
                OKX: 'OKXF',
                Bitget: 'BITGETF',
                Gate: 'GATEIOF',
                Aster: 'BINANCEF'
            };
            try {
                tigerWs.send(JSON.stringify({
                    action: 'SetTicker',
                    exchange: exMap[exchange] || 'BINANCEF',
                    ticker: symbol,
                    group: grp
                }));
                showToast(`🐯 Открыто: ${symbol} (${exchange})`);
            } catch {}
        }

        function openBanModal(symbol, ev) {
            if (ev) ev.stopPropagation();
            pendingBanCoin = baseCoin(symbol);
            document.getElementById('ban-modal-coin').textContent = pendingBanCoin;
            document.getElementById('ban-modal-bg').classList.add('open');
        }

        function closeBanModal() {
            document.getElementById('ban-modal-bg').classList.remove('open');
        }

        async function confirmBanModal(scope) {
            closeBanModal();
            if (!pendingBanCoin) return;
            await sendBanUpdate(pendingBanCoin, 'ban', scope);
        }

        async function addBannedFromInput() {
            const inp = document.getElementById('ban-input');
            const coin = baseCoin(inp.value);
            const scope = document.getElementById('ban-scope').value;
            if (!coin) return;
            inp.value = '';
            await sendBanUpdate(coin, 'ban', scope);
        }

        async function sendBanUpdate(coin, action, scope = 'ALL') {
            try {
                const res = await fetch('/api/ban', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, coin, action, scope })
                });
                if (res.ok) {
                    const out = await res.json();
                    userBannedCoins = out.banned_coins || {};
                    renderBannedCoins();
                    showToast(action === 'ban' ? `🚫 ${coin} скрыта` : `✅ ${coin} возвращена`);
                    loadData();
                }
            } catch { showToast('Ошибка обновления списка'); }
        }

        function renderBannedCoins() {
            const box = document.getElementById('banned-list');
            const entries = Object.entries(userBannedCoins || {});
            if (!entries.length) {
                box.innerHTML = '<div class="meta">Список пуст</div>';
                return;
            }
            const lbl = { ALL: 'Оба', UPTICK: 'Аптики', ROBOT: 'Роботы' };
            box.innerHTML = entries.map(([c, sc]) =>
                `<span class="ban-tag">${c} <span style="opacity:.7">(${lbl[sc]||sc})</span>` +
                `<button onclick="sendBanUpdate('${c}','unban')">✕</button></span>`
            ).join('');
        }

        function renderUpticks() {
            const liveBox = document.getElementById('live-active-upticks');
            const act = cachedActiveUp.filter(x => upExFilter === 'ALL' || x.exchange === upExFilter);
            document.getElementById('live-up-cnt').textContent = act.length;
            liveBox.innerHTML = act.length ? act.map(a => {
                const isL = a.side === 'LONG';
                return `<div class="card ${isL?'long':'short'}" style="margin:0" onclick="openInTiger('${a.exchange}','${a.symbol}')">
                    <div class="card-top">
                        <div class="sym-wrap"><span>${a.symbol}</span><span class="ex-tag">${a.exchange}</span></div>
                        <div style="display:flex;gap:5px;align-items:center">
                            <span class="wall-badge">🧱 ${fmtUsd(a.wall_usd)}</span>
                            <span class="side-badge ${isL?'long':'short'}">${a.side}</span>
                            <button class="ban-x" onclick="openBanModal('${a.symbol}', event)">✕</button>
                        </div>
                    </div>
                </div>`;
            }).join('') : '<div class="empty">Сейчас активных аптиков нет</div>';

            const list = document.getElementById('upticks-list');
            const rows = cachedUpticks.filter(x => upExFilter === 'ALL' || x.exchange === upExFilter);
            list.innerHTML = rows.length ? rows.map(r => {
                const isL = r.side === 'LONG';
                return `<div class="card ${isL?'long':'short'}" onclick="openInTiger('${r.exchange}','${r.symbol}')">
                    <div class="card-top">
                        <div class="sym-wrap"><span>${r.symbol}</span><span class="ex-tag">${r.exchange}</span></div>
                        <div style="display:flex;gap:5px;align-items:center">
                            <span class="wall-badge">🧱 ${fmtUsd(r.wall_usd)}</span>
                            <span class="side-badge ${isL?'long':'short'}">${r.side}</span>
                            <button class="ban-x" onclick="openBanModal('${r.symbol}', event)">✕</button>
                        </div>
                    </div>
                    <div class="card-meta">
                        <span>Цена: <b>${r.price}</b> · Планка: <b>${r.limit_price}</b></span>
                        <span>24ч: <b>${fmtUsd(r.volume_24h_usd)}</b> · ${r.utc_time}</span>
                    </div>
                </div>`;
            }).join('') : '<div class="empty">За сегодня (UTC) аптиков пока не было</div>';
        }

        function renderRobots() {
            const liveBox = document.getElementById('live-active-robots');
            const act = cachedActiveRob.filter(x => robExFilter === 'ALL' || x.exchange === robExFilter);
            document.getElementById('live-rob-cnt').textContent = act.length;
            liveBox.innerHTML = act.length ? act.map(a => {
                const isB = a.side === 'BUY';
                return `<div class="card ${isB?'long':'short'}" style="margin:0" onclick="openInTiger('${a.exchange}','${a.symbol}')">
                    <div class="card-top">
                        <div class="sym-wrap"><span>${a.symbol}</span><span class="ex-tag">${a.exchange}</span></div>
                        <div style="display:flex;gap:5px;align-items:center">
                            <span class="wall-badge">${fmtUsd(a.total_usd)}</span>
                            <span class="side-badge ${isB?'long':'short'}">${isB?'ПОКУПАШ':'ПРОДАВАШ'}</span>
                            <button class="ban-x" onclick="openBanModal('${a.symbol}', event)">✕</button>
                        </div>
                    </div>
                    <div class="card-meta"><span>Ударов: <b>${a.prints_count}</b> · раз в <b>${a.avg_interval_sec}с</b></span></div>
                </div>`;
            }).join('') : '<div class="empty">Сейчас активных роботов нет</div>';

            const list = document.getElementById('robots-list');
            const rows = cachedRobots.filter(x => robExFilter === 'ALL' || x.exchange === robExFilter);
            list.innerHTML = rows.length ? rows.map(r => {
                const isB = r.side === 'BUY';
                return `<div class="card ${isB?'long':'short'}" onclick="openInTiger('${r.exchange}','${r.symbol}')">
                    <div class="card-top">
                        <div class="sym-wrap"><span>${r.symbol}</span><span class="ex-tag">${r.exchange}</span></div>
                        <div style="display:flex;gap:5px;align-items:center">
                            <span class="wall-badge">${fmtUsd(r.total_usd)}</span>
                            <span class="side-badge ${isB?'long':'short'}">${isB?'ПОКУПАШ':'ПРОДАВАШ'}</span>
                            <button class="ban-x" onclick="openBanModal('${r.symbol}', event)">✕</button>
                        </div>
                    </div>
                    <div class="card-meta">
                        <span><b>${r.prints_count}×</b> ~${fmtUsd(r.avg_print_usd)} / <b>${r.avg_interval_sec}с</b> (${r.dominance_pct}%)</span>
                        <span>24ч: <b>${fmtUsd(r.volume_24h_usd)}</b> · ${r.utc_time}</span>
                    </div>
                </div>`;
            }).join('') : '<div class="empty">За сегодня (UTC) сигналов Покупаш/Продаваш пока не было</div>';
        }

        async function saveGlobalSettings(fromQuickToggle = false) {
            enableUpticks = document.getElementById('set-en-upticks').checked;
            enableRobots = document.getElementById('set-en-robots').checked;
            const robotCfg = readRobotConfigFromUI();
            const discordCfg = readDiscordConfigFromUI();
            updateFeatureUI();
            updateRobotSummary();
            saveLocalPrefs();
            try {
                const res = await fetch('/api/settings', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        user_id: userId,
                        min_volume: Number(document.getElementById('set-vol').value || 500000),
                        min_robot_usd: robotCfg.min_total_usd,
                        enable_upticks: enableUpticks,
                        enable_robots: enableRobots,
                        robot_config: robotCfg,
                        discord_config: discordCfg
                    })
                });
                if (res.ok) {
                    const out = await res.json();
                    if (out.robot_config) applyRobotConfigToUI(out.robot_config);
                    if (out.discord_config) applyDiscordConfigToUI(out.discord_config);
                    if (!fromQuickToggle) showToast('✅ Настройки сохранены и применены');
                    loadData();
                }
            } catch { showToast('Ошибка сохранения'); }
        }

        async function loadData() {
            try {
                const res = await fetch('/api/data?user_id=' + encodeURIComponent(userId));
                if (!res.ok) return;
                const data = await res.json();

                if (!settingsInitialized) {
                    document.getElementById('set-vol').value = data.user_min_volume || 500000;
                    if (data.user_robot_config) {
                        applyRobotConfigToUI(data.user_robot_config);
                    } else {
                        document.getElementById('set-robot-usd').value = data.user_min_robot_usd || 25000;
                    }
                    if (data.user_discord_config) {
                        applyDiscordConfigToUI(data.user_discord_config);
                    }
                    if (userId) {
                        enableUpticks = Boolean(data.user_enable_upticks);
                        enableRobots = Boolean(data.user_enable_robots);
                        updateFeatureUI();
                    }
                    userBannedCoins = data.user_banned_coins || {};
                    renderBannedCoins();
                    settingsInitialized = true;
                }

                cachedActiveUp = data.active || [];
                cachedUpticks = data.history || [];
                cachedActiveRob = data.active_robots || [];
                cachedRobots = data.robots_history || [];

                if (cachedUpticks.length) {
                    const topUp = cachedUpticks[0];
                    if (firstLoadDone && topUp.id > lastUptickId && enableUpticks) {
                        if (document.getElementById('snd-uptick').checked) {
                            const sideRu = topUp.side === 'LONG' ? 'Лонг' : 'Шорт';
                            const exRu = EX_VOICE[topUp.exchange] || topUp.exchange;
                            speakText(`Аптик на бирже ${exRu}. ${baseCoin(topUp.symbol)}. ${sideRu}. Плотность ${fmtUsdVoice(topUp.wall_usd)}`);
                        }
                        if (document.getElementById('tiger-auto-up').checked) {
                            openInTiger(topUp.exchange, topUp.symbol);
                        }
                    }
                    lastUptickId = Math.max(lastUptickId, topUp.id);
                }

                if (cachedRobots.length) {
                    const topRob = cachedRobots[0];
                    if (firstLoadDone && topRob.id > lastRobotId && enableRobots) {
                        if (document.getElementById('snd-robot').checked) {
                            const who = topRob.side === 'BUY' ? 'Покупаш' : 'Продаваш';
                            const exRu = EX_VOICE[topRob.exchange] || topRob.exchange;
                            speakText(`${who} на бирже ${exRu}. ${baseCoin(topRob.symbol)}. ${fmtUsdVoice(topRob.total_usd)}`);
                        }
                        if (document.getElementById('tiger-auto-rob').checked) {
                            openInTiger(topRob.exchange, topRob.symbol);
                        }
                    }
                    lastRobotId = Math.max(lastRobotId, topRob.id);
                }

                firstLoadDone = true;
                renderUpticks();
                renderRobots();
            } catch {}
        }

        loadLocalPrefs();
        renderExFilters();
        connectTiger(false);
        loadData();
        setInterval(loadData, 1000);
    </script>
</body>
</html>
"""


# ==================== FASTAPI СЕРВЕР ====================

class SettingsPayload(BaseModel):
    user_id: int
    min_volume: float = DEFAULT_MIN_VOLUME
    min_robot_usd: float = DEFAULT_MIN_ROBOT_USD
    enable_upticks: bool = True
    enable_robots: bool = True
    robot_config: Optional[Dict[str, Any]] = None
    discord_config: Optional[Dict[str, Any]] = None


class DiscordTestPayload(BaseModel):
    user_id: int = 0
    discord_config: Optional[Dict[str, Any]] = None


class BanPayload(BaseModel):
    user_id: int
    coin: str
    action: str = "ban"  # ban | unban
    scope: str = "ALL"   # ALL | UPTICK | ROBOT


async def _run_telegram_polling() -> None:
    if bot is None:
        logger.warning("BOT_TOKEN не задан — Telegram-бот не запущен.")
        return
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot, handle_signals=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Ошибка polling Telegram (%s) — перезапуск через 5с", exc)
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _http_session
    await init_db()
    await load_engine_robot_config()
    await get_cached_users(force=True)

    ws_session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=80, ttl_dns_cache=300),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    _http_session = ws_session
    # Сразу загружаем со всех 6 бирж актуальные списки акций/ETF/металлов/нефти до запуска роботов
    await refresh_tradfi_metadata(ws_session)

    tasks: List[asyncio.Task[Any]] = [
        asyncio.create_task(_run_telegram_polling()),
        asyncio.create_task(notify_bot_updated()),
        *create_uptick_tasks(ws_session, engine),
        *create_robot_tasks(ws_session, robot_engine),
    ]

    logger.info("Запущены модули: upticks.py (4 WS), robots.py (6 WS) и discord_notifier.py.")
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        _http_session = None
        await ws_session.close()
        if bot is not None:
            await bot.session.close()
        if _pg_pool is not None:
            await _pg_pool.close()


app = FastAPI(lifespan=lifespan)


@app.api_route("/", methods=["GET", "HEAD"])
async def root_status() -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "uptick-robot-screener"})


@app.api_route("/ping", methods=["GET", "HEAD"])
async def ping() -> JSONResponse:
    return JSONResponse({"status": "ok", "uptick_exchanges": 4, "robot_exchanges": 6})


@app.get("/webapp", response_class=HTMLResponse)
async def webapp() -> HTMLResponse:
    return HTMLResponse(WEBAPP_HTML)


@app.get("/api/data")
async def api_data(user_id: int = Query(default=0)) -> JSONResponse:
    await refresh_api_cache()
    today_utc = _api_cache["utc_date"]

    user_min_vol = DEFAULT_MIN_VOLUME
    user_min_robot = DEFAULT_MIN_ROBOT_USD
    user_en_upticks = True
    user_en_robots = True
    user_banned_map: Dict[str, str] = {}
    user_robot_cfg: Dict[str, Any] = dict(robot_engine.config)
    user_discord_cfg: Dict[str, Any] = normalize_discord_config(None)

    if user_id:
        row = await db_fetchone(
            "SELECT min_volume, min_robot_usd, robot_config, enable_upticks, enable_robots, banned_coins, discord_config "
            "FROM users WHERE user_id = ?",
            (user_id,),
        )
        if row:
            user_min_vol = float(row.get("min_volume") or DEFAULT_MIN_VOLUME)
            user_min_robot = float(row.get("min_robot_usd") or DEFAULT_MIN_ROBOT_USD)
            user_en_upticks = int(row.get("enable_upticks") if row.get("enable_upticks") is not None else 1) == 1
            user_en_robots = int(row.get("enable_robots") if row.get("enable_robots") is not None else 1) == 1
            user_banned_map = parse_banned_coins(row.get("banned_coins"))
            user_robot_cfg = normalize_robot_config(row.get("robot_config"), fallback_min_total=user_min_robot)
            user_min_robot = float(user_robot_cfg["min_total_usd"])
            user_discord_cfg = normalize_discord_config(row.get("discord_config"))

    history_rows = [
        h
        for h in _api_cache["history"]
        if not is_blacklisted_symbol(str(h.get("symbol") or ""))
        and not is_coin_banned(extract_base_asset(str(h.get("symbol") or "")), user_banned_map, "UPTICK")
        and float(h.get("volume_24h_usd") or 0) >= user_min_vol
    ]

    active_upticks = [
        {"exchange": ex, "symbol": sym, "side": side.value, "wall_usd": wall}
        for ex, sym, side, vol, wall in engine.get_active_upticks()
        if not is_blacklisted_symbol(sym)
        and not is_coin_banned(extract_base_asset(sym), user_banned_map, "UPTICK")
        and vol >= user_min_vol
    ]

    robots_rows = [
        r
        for r in _api_cache["robots"]
        if not is_robot_blacklisted_symbol(str(r.get("symbol") or ""), str(r.get("exchange") or ""))
        and not is_coin_banned(extract_base_asset(str(r.get("symbol") or "")), user_banned_map, "ROBOT")
        and robot_matches_user_config(
            str(r.get("exchange") or ""),
            str(r.get("side") or ""),
            float(r.get("total_usd") or 0),
            int(r.get("prints_count") or 0),
            float(r.get("avg_print_usd") or 0),
            float(r.get("avg_interval_sec") or 0),
            float(r.get("dominance_pct") or 0),
            float(r.get("volume_24h_usd") or 0),
            user_robot_cfg,
        )
    ]

    active_robots = [
        {
            "exchange": ex,
            "symbol": sym,
            "side": side.value,
            "total_usd": usd,
            "prints_count": cnt,
            "avg_interval_sec": round(avg_int, 2),
        }
        for ex, sym, side, usd, cnt, avg_int, avg_pr, dom, vol in robot_engine.get_active_robots()
        if not is_robot_blacklisted_symbol(sym, ex)
        and not is_coin_banned(extract_base_asset(sym), user_banned_map, "ROBOT")
        and robot_matches_user_config(ex, side.value, usd, cnt, avg_pr, avg_int, dom, vol, user_robot_cfg)
    ]

    return JSONResponse(
        {
            "utc_date": today_utc,
            "active": active_upticks,
            "history": history_rows,
            "active_robots": active_robots,
            "robots_history": robots_rows,
            "user_min_volume": user_min_vol,
            "user_min_robot_usd": user_min_robot,
            "user_robot_config": user_robot_cfg,
            "user_discord_config": user_discord_cfg,
            "user_enable_upticks": user_en_upticks,
            "user_enable_robots": user_en_robots,
            "user_banned_coins": user_banned_map,
        }
    )


@app.post("/api/settings")
async def api_settings(payload: SettingsPayload) -> JSONResponse:
    vol = max(0.0, payload.min_volume)
    rcfg_in = dict(payload.robot_config) if isinstance(payload.robot_config, dict) else {}
    if "min_total_usd" not in rcfg_in:
        rcfg_in["min_total_usd"] = payload.min_robot_usd
    rcfg = normalize_robot_config(rcfg_in, fallback_min_total=payload.min_robot_usd)
    rob_usd = float(rcfg["min_total_usd"])
    en_up = 1 if payload.enable_upticks else 0
    en_rob = 1 if payload.enable_robots else 0

    dcfg: Optional[Dict[str, Any]] = None
    if isinstance(payload.discord_config, dict):
        dcfg = normalize_discord_config(payload.discord_config)

    if payload.user_id > 0:
        if dcfg is not None:
            await db_execute(
                "INSERT INTO users (user_id, min_volume, min_robot_usd, robot_config, enable_upticks, enable_robots, discord_config) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET "
                "min_volume = excluded.min_volume, "
                "min_robot_usd = excluded.min_robot_usd, "
                "robot_config = excluded.robot_config, "
                "enable_upticks = excluded.enable_upticks, "
                "enable_robots = excluded.enable_robots, "
                "discord_config = excluded.discord_config",
                (payload.user_id, vol, rob_usd, json.dumps(rcfg), en_up, en_rob, json.dumps(dcfg)),
            )
        else:
            await db_execute(
                "INSERT INTO users (user_id, min_volume, min_robot_usd, robot_config, enable_upticks, enable_robots) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET "
                "min_volume = excluded.min_volume, "
                "min_robot_usd = excluded.min_robot_usd, "
                "robot_config = excluded.robot_config, "
                "enable_upticks = excluded.enable_upticks, "
                "enable_robots = excluded.enable_robots",
                (payload.user_id, vol, rob_usd, json.dumps(rcfg), en_up, en_rob),
            )
    robot_engine.update_config(rcfg)
    invalidate_users_cache()
    return JSONResponse({"status": "ok", "robot_config": rcfg, "discord_config": dcfg})


@app.post("/api/discord/test")
async def api_discord_test(payload: DiscordTestPayload) -> JSONResponse:
    dcfg = normalize_discord_config(payload.discord_config)
    up_url = resolve_webhook_url(dcfg, "UPTICK")
    rob_url = resolve_webhook_url(dcfg, "ROBOT")
    targets: List[Tuple[str, str]] = []
    if up_url:
        targets.append((up_url, "Аптики" if rob_url and rob_url != up_url else "Общий канал"))
    if rob_url and rob_url != up_url:
        targets.append((rob_url, "Покупаш / Продаваш"))
    if not targets:
        return JSONResponse(
            {"ok": False, "detail": "Укажите корректный Discord Webhook URL (или переменную в Render)"},
            status_code=400,
        )

    all_ok = True
    last_err = ""
    for url, lbl in targets:
        ok, err = await send_test_discord(url, label=lbl, session=_http_session)
        if not ok:
            all_ok = False
            last_err = err
    return JSONResponse({"ok": all_ok, "detail": "OK" if all_ok else last_err})


@app.post("/api/ban")
async def api_ban(payload: BanPayload) -> JSONResponse:
    base = extract_base_asset(payload.coin)
    if not base:
        return JSONResponse({"status": "error", "detail": "invalid coin"}, status_code=400)
    bmap: Dict[str, str] = {}
    if payload.user_id > 0:
        row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (payload.user_id,))
        bmap = parse_banned_coins(row.get("banned_coins") if row else None)
        if payload.action == "unban":
            bmap.pop(base, None)
        else:
            scope = payload.scope.upper() if payload.scope.upper() in ("ALL", "UPTICK", "ROBOT") else "ALL"
            bmap[base] = scope
        await db_execute(
            "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
            (payload.user_id, json.dumps(bmap)),
        )
        invalidate_users_cache()
    return JSONResponse({"status": "ok", "banned_coins": bmap})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)