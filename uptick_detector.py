"""
Мультибиржевой детектор «Аптиков» + Сканер «Плотностей» (Спот и Фьючерсы) + Telegram Bot + Mini App + Tiger.Trade API.
Аптики (4 биржи): Binance, Bybit, OKX, Aster.
Плотности (7 бирж): Binance, Bybit, OKX, KuCoin, Aster, Gate, Bitget.

Функции:
1. Детектор аптиков (планок / границ ценового коридора):
   - Binance / Aster: PERCENT_PRICE (markPrice * multiplierUp / multiplierDown).
   - Bybit / OKX: двухэтапная проверка + точные биржевые планки buyLmt / sellLmt.
   - Защита от пустых стаканов и гистерезис.

2. Сканер плотностей (крупных лимитных заявок в стакане в пределах 5% от цены):
   - Классические фильтры по умолчанию: Спот от $250,000 | Фьючерсы от $1,000,000 (настраивается).
   - Полное исключение тяжелых монет (BTC, ETH, SOL, ZEC...), акций (TSLA, NVDA, AAPL...) и металлов (XAU, XAG, PAXG...).
   - Учет контрактных множителей (ctVal на OKX, multiplier на KuCoin Futures, quanto_multiplier на Gate Futures).
   - Мягкий rate-limit контроль для стабильной работы 24/7 на бесплатном Render.

3. Telegram Bot + Mini App + Tiger.Trade:
   - Мгновенные уведомления в Telegram по аптикам и плотностям.
   - Голосовые и звуковые оповещения в приложении (отдельно для аптика и плотности) с кнопкой вкл/выкл.
   - Интеграция с Tiger.Trade (Local Signal Server, ws://127.0.0.1:7819):
     * Открытие монеты по клику на карточку в нужной группе линковки (A–Z, 0–11).
     * Отдельные переключатели автооткрытия для новых аптиков и плотностей.

Зависимости:
    pip install aiohttp aiosqlite aiogram fastapi uvicorn
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Awaitable, Callable, Dict, List, Optional, Set, Tuple

import aiohttp
import aiosqlite
import uvicorn
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

try:
    import asyncpg  # Опционально: для бесплатного PostgreSQL (Neon / Supabase)
except ImportError:
    asyncpg = None

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("UptickDensityBot")


# ==============================================================================
# КОНФИГУРАЦИЯ, БАЗА ДАННЫХ (PostgreSQL / SQLite) И TELEGRAM-БОТ
# ==============================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()
DB_PATH = os.getenv("DB_PATH", "upticks.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

DEFAULT_MIN_VOLUME = 100_000.0
DEFAULT_MIN_DENSITY_SPOT = 250_000.0
DEFAULT_MIN_DENSITY_FUTURES = 1_000_000.0

bot: Optional[Bot] = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()

_pg_pool = None
_use_pg = False

# Кэш для /api/data (снижает нагрузку на 0.1 CPU при десятках открытых Mini App)
_api_cache: Dict[str, object] = {
    "ts": 0.0,
    "utc_date": "",
    "history": [],
    "densities": [],
}
API_CACHE_TTL_SEC = 2.5


def _to_pg_sql(sql: str) -> str:
    """Преобразует SQLite-плейсхолдеры '?' в PostgreSQL '$1, $2, ...'."""
    parts = sql.split("?")
    if len(parts) == 1:
        return sql
    return "".join(f"{p}${i}" for i, p in enumerate(parts[:-1], 1)) + parts[-1]


async def db_execute(sql: str, params: tuple = ()) -> None:
    """Выполняет INSERT / UPDATE / DDL в PostgreSQL или SQLite."""
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            await conn.execute(_to_pg_sql(sql), *params)
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(sql, params)
            await db.commit()


async def db_fetchall(sql: str, params: tuple = ()) -> List[dict]:
    """Возвращает список строк (dict) из PostgreSQL или SQLite."""
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            rows = await conn.fetch(_to_pg_sql(sql), *params)
            return [dict(r) for r in rows]
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(sql, params) as cursor:
                return [dict(r) for r in await cursor.fetchall()]


async def db_fetchone(sql: str, params: tuple = ()) -> Optional[dict]:
    """Возвращает одну строку (dict) или None из PostgreSQL или SQLite."""
    rows = await db_fetchall(sql, params)
    return rows[0] if rows else None


async def init_db() -> None:
    """Инициализирует PostgreSQL (если задан DATABASE_URL и установлен asyncpg) или SQLite."""
    global _pg_pool, _use_pg

    if DATABASE_URL and asyncpg is not None:
        try:
            _pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
            _use_pg = True
            logger.info("Подключена постоянная база данных PostgreSQL (DATABASE_URL).")
        except Exception as exc:
            logger.error(f"Не удалось подключиться к PostgreSQL ({exc}). Переключаюсь на SQLite.")
            _use_pg = False
    elif DATABASE_URL and asyncpg is None:
        logger.warning("DATABASE_URL задан, но пакет asyncpg не установлен. Используется SQLite.")

    id_col = "SERIAL PRIMARY KEY" if _use_pg else "INTEGER PRIMARY KEY AUTOINCREMENT"
    user_id_col = "BIGINT PRIMARY KEY" if _use_pg else "INTEGER PRIMARY KEY"

    await db_execute(
        f"""
        CREATE TABLE IF NOT EXISTS users (
            user_id {user_id_col},
            min_volume REAL DEFAULT 100000,
            min_density_spot REAL DEFAULT 250000,
            min_density_futures REAL DEFAULT 1000000
        )
        """
    )

    for col_sql in (
        "ALTER TABLE users ADD COLUMN min_density_spot REAL DEFAULT 250000",
        "ALTER TABLE users ADD COLUMN min_density_futures REAL DEFAULT 1000000",
    ):
        try:
            await db_execute(col_sql)
        except Exception:
            pass

    await db_execute(
        f"""
        CREATE TABLE IF NOT EXISTS history (
            id {id_col},
            exchange TEXT,
            symbol TEXT,
            side TEXT,
            price REAL,
            mark_price REAL,
            limit_price REAL,
            volume REAL,
            distance_pct REAL,
            timestamp_utc TEXT,
            utc_date TEXT
        )
        """
    )
    await db_execute(
        f"""
        CREATE TABLE IF NOT EXISTS densities (
            id {id_col},
            exchange TEXT,
            market_type TEXT,
            symbol TEXT,
            side TEXT,
            price REAL,
            current_price REAL,
            size_usd REAL,
            distance_pct REAL,
            volume REAL,
            timestamp_utc TEXT,
            utc_date TEXT
        )
        """
    )


async def close_db() -> None:
    """Закрывает пул PostgreSQL при остановке сервиса."""
    global _pg_pool
    if _pg_pool is not None:
        await _pg_pool.close()
        _pg_pool = None


def _invalidate_api_cache() -> None:
    """Сбрасывает кэш /api/data при появлении нового события."""
    _api_cache["ts"] = 0.0


def _build_main_keyboard() -> Optional[InlineKeyboardMarkup]:
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Открыть Аптик & Плотности", web_app=WebAppInfo(url=WEBAPP_URL))]
        ]
    )


async def _broadcast_messages(user_ids: List[int], text: str, kb: Optional[InlineKeyboardMarkup]) -> None:
    """Отправляет уведомления подписчикам с мягкой паузой (соблюдение лимитов Telegram ~25 msg/sec)."""
    if not bot or not user_ids:
        return
    for uid in user_ids:
        try:
            await bot.send_message(uid, text, parse_mode="HTML", reply_markup=kb)
        except Exception as exc:
            logger.warning(f"Не удалось отправить уведомление {uid}: {exc}")
        if len(user_ids) > 5:
            await asyncio.sleep(0.04)


async def handle_uptick_alert(event: UptickEvent) -> None:
    """Сохраняет аптик в БД и рассылает уведомление подписчикам."""
    logger.info(
        f"🚨 АПТИК | {event.exchange} | {event.symbol} | {event.side.value} | "
        f"Цена: {event.price} | Планка: {event.limit_price} | Оборот: ${event.volume_24h_usd:,.0f}"
    )

    try:
        await db_execute(
            """
            INSERT INTO history (
                exchange, symbol, side, price, mark_price,
                limit_price, volume, distance_pct, timestamp_utc, utc_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.exchange,
                event.symbol,
                event.side.value,
                event.price,
                event.mark_price,
                event.limit_price,
                event.volume_24h_usd,
                event.distance_pct,
                event.timestamp_utc,
                event.utc_date,
            ),
        )
        _invalidate_api_cache()
        users = await db_fetchall("SELECT user_id, min_volume FROM users")
    except Exception as exc:
        logger.error(f"Ошибка записи аптика в БД: {exc}")
        return

    if not bot:
        return

    side_emoji = "🟢" if event.side == UptickSide.LONG else "🔴"
    text = (
        f"🚨 <b>АПТИК | {event.exchange}</b>\n"
        f"Монета: <code>{event.symbol}</code>\n"
        f"{side_emoji} Сторона: <b>{event.side.value}</b>\n"
        f"Цена: <code>{event.price}</code> (Mark: {event.mark_price})\n"
        f"Планка: <code>{event.limit_price:.6g}</code> (до планки: {event.distance_pct}%)\n"
        f"Оборот 24ч: <b>${event.volume_24h_usd:,.0f}</b>\n"
        f"Время (UTC): {event.timestamp_utc}"
    )

    target_ids = [
        int(u["user_id"])
        for u in users
        if event.volume_24h_usd >= float(u.get("min_volume") or DEFAULT_MIN_VOLUME)
    ]
    await _broadcast_messages(target_ids, text, _build_main_keyboard())


async def handle_density_alert(event: DensityEvent) -> None:
    """Сохраняет плотность в БД и рассылает уведомление подписчикам с учетом их фильтров Спот/Фьючерс."""
    logger.info(
        f"🧱 ПЛОТНОСТЬ | {event.exchange} ({event.market_type.value}) | {event.symbol} | "
        f"{event.side.value} | ${event.size_usd:,.0f} @ {event.price} ({event.distance_pct}%)"
    )

    try:
        await db_execute(
            """
            INSERT INTO densities (
                exchange, market_type, symbol, side, price, current_price,
                size_usd, distance_pct, volume, timestamp_utc, utc_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.exchange,
                event.market_type.value,
                event.symbol,
                event.side.value,
                event.price,
                event.current_price,
                event.size_usd,
                event.distance_pct,
                event.volume_24h_usd,
                event.timestamp_utc,
                event.utc_date,
            ),
        )
        _invalidate_api_cache()
        users = await db_fetchall(
            "SELECT user_id, min_density_spot, min_density_futures FROM users"
        )
    except Exception as exc:
        logger.error(f"Ошибка записи плотности в БД: {exc}")
        return

    if not bot:
        return

    side_emoji = "🟢" if event.side == DensitySide.BID else "🔴"
    market_ru = "СПОТ" if event.market_type == MarketType.SPOT else "ФЬЮЧЕРС"
    text = (
        f"🧱 <b>ПЛОТНОСТЬ | {event.exchange} ({market_ru})</b>\n"
        f"Монета: <code>{event.symbol}</code>\n"
        f"{side_emoji} Сторона: <b>{event.side.value}</b>\n"
        f"Размер плотности: <b>${event.size_usd:,.0f}</b>\n"
        f"Цена плотности: <code>{event.price}</code> (до цены: {event.distance_pct}%)\n"
        f"Оборот 24ч: ${event.volume_24h_usd:,.0f}\n"
        f"Время (UTC): {event.timestamp_utc}"
    )

    target_ids: List[int] = []
    for u in users:
        threshold = (
            float(u.get("min_density_spot") or DEFAULT_MIN_DENSITY_SPOT)
            if event.market_type == MarketType.SPOT
            else float(u.get("min_density_futures") or DEFAULT_MIN_DENSITY_FUTURES)
        )
        if event.size_usd >= threshold:
            target_ids.append(int(u["user_id"]))

    await _broadcast_messages(target_ids, text, _build_main_keyboard())


# Глобальные экземпляры ядер
engine = UptickEngine(
    min_volume_usd=DEFAULT_MIN_VOLUME,
    trigger_tolerance=0.0008,
    release_tolerance=0.0060,
    book_confirm_tol=0.0080,
    min_cooldown_sec=180.0,
    on_uptick=handle_uptick_alert,
)

density_engine = DensityEngine(
    base_min_spot_usd=DEFAULT_MIN_DENSITY_SPOT,
    base_min_futures_usd=DEFAULT_MIN_DENSITY_FUTURES,
    max_distance_pct=5.0,
    cooldown_sec=600.0,
    active_ttl_sec=90.0,
    on_density=handle_density_alert,
)


# === КОМАНДЫ TELEGRAM-БОТА ===

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    await db_execute(
        """
        INSERT INTO users (user_id, min_volume, min_density_spot, min_density_futures)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO NOTHING
        """,
        (
            message.from_user.id,
            DEFAULT_MIN_VOLUME,
            DEFAULT_MIN_DENSITY_SPOT,
            DEFAULT_MIN_DENSITY_FUTURES,
        ),
    )

    kb = _build_main_keyboard()
    hint = (
        "Нажмите кнопку ниже, чтобы открыть приложение (со звуком и интеграцией Tiger.Trade)."
        if kb
        else "⚠️ Переменная WEBAPP_URL еще не задана."
    )
    await message.answer(
        "👋 <b>Аптик + Сканер плотностей запущен!</b>\n\n"
        "⚡ <b>Аптики (4):</b> Binance, Bybit, OKX, Aster.\n"
        "🧱 <b>Плотности (7):</b> Binance, Bybit, OKX, KuCoin, Aster, Gate, Bitget.\n"
        "🐯 <b>Tiger.Trade:</b> открытие по клику или автооткрытие в Mini App.\n"
        "Исключены: тяжелые монеты (BTC, ETH, SOL, ZEC...), акции и металлы.\n\n"
        "<b>Команды фильтров:</b>\n"
        "• <code>/vol 500000</code> — мин. оборот 24ч ($) для аптиков\n"
        "• <code>/dspot 250000</code> — мин. плотность на Споте ($)\n"
        "• <code>/dfut 1000000</code> — мин. плотность на Фьючерсах ($)\n"
        "• <code>/status</code> — активные аптики и плотности сейчас\n\n"
        f"{hint}",
        parse_mode="HTML",
        reply_markup=kb,
    )


@dp.message(Command("vol"))
async def cmd_vol(message: types.Message) -> None:
    try:
        val = max(100_000.0, float((message.text or "").split()[1].replace(",", "").replace("$", "")))
        await db_execute(
            "INSERT INTO users (user_id, min_volume) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET min_volume = excluded.min_volume",
            (message.from_user.id, val),
        )
        await message.answer(f"✅ Мин. оборот для аптиков: <b>${val:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/vol 250000</code>", parse_mode="HTML")


@dp.message(Command("dspot"))
async def cmd_dspot(message: types.Message) -> None:
    try:
        val = max(250_000.0, float((message.text or "").split()[1].replace(",", "").replace("$", "")))
        await db_execute(
            "INSERT INTO users (user_id, min_density_spot) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET min_density_spot = excluded.min_density_spot",
            (message.from_user.id, val),
        )
        await message.answer(f"✅ Мин. плотность на Споте: <b>${val:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/dspot 250000</code> (минимум $250,000)", parse_mode="HTML")


@dp.message(Command("dfut"))
async def cmd_dfut(message: types.Message) -> None:
    try:
        val = max(1_000_000.0, float((message.text or "").split()[1].replace(",", "").replace("$", "")))
        await db_execute(
            "INSERT INTO users (user_id, min_density_futures) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET min_density_futures = excluded.min_density_futures",
            (message.from_user.id, val),
        )
        await message.answer(f"✅ Мин. плотность на Фьючерсах: <b>${val:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/dfut 1000000</code> (минимум $1,000,000)", parse_mode="HTML")


@dp.message(Command("status"))
async def cmd_status(message: types.Message) -> None:
    active_u = engine.get_active_upticks()
    active_d = density_engine.get_active_densities()

    lines = []
    if active_u:
        lines.append("⚡ <b>Активные аптики сейчас:</b>")
        for ex, sym, side in active_u:
            emoji = "🟢" if side == UptickSide.LONG else "🔴"
            lines.append(f"{emoji} <b>{sym}</b> ({ex}) — {side.value}")
    else:
        lines.append("⚡ Активных аптиков прямо сейчас нет.")

    lines.append("")
    if active_d:
        lines.append(f"🧱 <b>Активные плотности в стакане ({len(active_d)}):</b>")
        for d in active_d[:12]:
            emoji = "🟢" if d.side == DensitySide.BID else "🔴"
            lines.append(
                f"{emoji} <b>{d.symbol}</b> ({d.exchange} {d.market_type.value}) — "
                f"<b>${d.size_usd:,.0f}</b> @ {d.price} ({d.distance_pct}%)"
            )
    else:
        lines.append("🧱 Крупных плотностей прямо сейчас не найдено.")

    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=_build_main_keyboard())


# ==============================================================================
# FASTAPI + TELEGRAM MINI APP (С ИНТЕГРАЦИЕЙ TIGER.TRADE)
# ==============================================================================

WEBAPP_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Аптик & Плотности</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        :root {
            --bg: var(--tg-theme-bg-color, #111318);
            --card: var(--tg-theme-secondary-bg-color, #1b1f27);
            --text: var(--tg-theme-text-color, #f2f4f8);
            --hint: var(--tg-theme-hint-color, #8e96a4);
            --accent: var(--tg-theme-button-color, #2aabee);
            --accent-text: var(--tg-theme-button-text-color, #ffffff);
            --long: #16c784;
            --short: #ea3943;
            --gold: #f5b700;
            --border: rgba(255, 255, 255, 0.08);
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            padding: 14px 14px 80px;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: var(--bg);
            color: var(--text);
            -webkit-font-smoothing: antialiased;
        }
        .header {
            display: flex; justify-content: space-between; align-items: center;
            margin-bottom: 12px; gap: 6px; flex-wrap: wrap;
        }
        .header-right { display: flex; align-items: center; gap: 6px; }
        .title { font-size: 17px; font-weight: 700; display: flex; align-items: center; gap: 7px; }
        .live-dot {
            width: 8px; height: 8px; border-radius: 50%; background: var(--long);
            box-shadow: 0 0 8px var(--long); display: inline-block;
        }
        .utc-badge {
            font-size: 11px; font-weight: 600; color: var(--hint);
            background: var(--card); border: 1px solid var(--border);
            padding: 5px 7px; border-radius: 8px; font-variant-numeric: tabular-nums;
        }
        .sound-btn, .tiger-pill {
            border: 1px solid var(--border); background: var(--card); color: var(--text);
            padding: 5px 8px; border-radius: 8px; font-size: 12px; font-weight: 600;
            cursor: pointer; display: flex; align-items: center; gap: 4px; transition: all 0.2s;
        }
        .sound-btn.on, .tiger-pill.connected {
            border-color: rgba(22, 199, 132, 0.45);
            background: rgba(22, 199, 132, 0.14);
            color: var(--long);
        }
        .sound-btn.off, .tiger-pill.disconnected { color: var(--hint); opacity: 0.85; }
        .tiger-dot {
            width: 7px; height: 7px; border-radius: 50%; background: var(--hint); display: inline-block;
        }
        .tiger-pill.connected .tiger-dot { background: var(--long); box-shadow: 0 0 6px var(--long); }
        .tabs {
            display: grid; grid-template-columns: repeat(4, 1fr); gap: 4px;
            background: var(--card); padding: 4px; border-radius: 12px;
            margin-bottom: 12px; border: 1px solid var(--border);
        }
        .tab-btn {
            border: none; background: transparent; color: var(--hint);
            padding: 8px 3px; border-radius: 9px; font-size: 12px; font-weight: 600;
            cursor: pointer; transition: all 0.2s; white-space: nowrap;
        }
        .tab-btn.active { background: var(--accent); color: var(--accent-text); }
        .filters {
            display: flex; gap: 6px; overflow-x: auto; padding-bottom: 8px;
            margin-bottom: 4px; scrollbar-width: none;
        }
        .filters::-webkit-scrollbar { display: none; }
        .chip {
            border: 1px solid var(--border); background: var(--card); color: var(--hint);
            padding: 5px 10px; border-radius: 16px; font-size: 12px; font-weight: 600;
            cursor: pointer; white-space: nowrap; flex-shrink: 0;
            display: inline-flex; align-items: center; gap: 5px;
        }
        .chip.active { background: var(--text); color: var(--bg); border-color: var(--text); }
        .card {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 14px; padding: 12px; margin-bottom: 9px;
            display: flex; justify-content: space-between; align-items: center; gap: 10px;
            cursor: pointer; transition: transform 0.12s, border-color 0.2s;
        }
        .card:hover { border-color: rgba(245, 183, 0, 0.45); }
        .card:active { transform: scale(0.985); }
        .sym-row { display: flex; align-items: center; gap: 6px; margin-bottom: 4px; flex-wrap: wrap; }
        .symbol { font-size: 15px; font-weight: 700; }
        .tiger-open-hint {
            font-size: 10px; font-weight: 700; padding: 2px 5px; border-radius: 5px;
            background: rgba(245, 183, 0, 0.14); color: var(--gold);
        }
        .ex-logo {
            width: 22px; height: 22px; border-radius: 6px;
            display: inline-flex; align-items: center; justify-content: center;
            flex-shrink: 0; overflow: hidden;
            box-shadow: 0 1px 4px rgba(0, 0, 0, 0.35);
        }
        .ex-logo svg { width: 100%; height: 100%; display: block; }
        .chip .ex-logo { width: 16px; height: 16px; border-radius: 4px; box-shadow: none; }
        .ex-tag {
            font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 6px;
            background: rgba(255, 255, 255, 0.08); color: var(--text);
        }
        .mkt-spot { background: rgba(42, 171, 238, 0.18); color: #5ec6ff; }
        .mkt-fut { background: rgba(245, 183, 0, 0.18); color: var(--gold); }
        .meta { font-size: 12px; color: var(--hint); line-height: 1.45; }
        .right-col { text-align: right; flex-shrink: 0; }
        .side-badge {
            display: inline-block; font-size: 11px; font-weight: 700;
            padding: 4px 8px; border-radius: 7px; margin-bottom: 4px;
        }
        .side-long { background: rgba(22, 199, 132, 0.16); color: var(--long); }
        .side-short { background: rgba(234, 57, 67, 0.16); color: var(--short); }
        .size-big { font-size: 14px; font-weight: 800; color: var(--text); margin-bottom: 3px; }
        .time-str { font-size: 11px; color: var(--hint); font-variant-numeric: tabular-nums; }
        .section-title {
            font-size: 13px; font-weight: 700; color: var(--hint);
            margin: 12px 0 8px; text-transform: uppercase; letter-spacing: 0.4px;
        }
        .empty {
            text-align: center; padding: 30px 14px; color: var(--hint);
            background: var(--card); border-radius: 14px; font-size: 13px;
        }
        .panel { display: none; }
        .panel.active { display: block; }
        .setting-box {
            background: var(--card); border-radius: 14px; padding: 14px; border: 1px solid var(--border);
            margin-bottom: 12px;
        }
        .setting-row {
            display: flex; justify-content: space-between; align-items: center; gap: 10px;
            padding: 6px 0;
        }
        .setting-row + .setting-row { border-top: 1px solid var(--border); margin-top: 4px; padding-top: 10px; }
        .btn-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 10px; }
        .secondary-btn {
            background: var(--bg); color: var(--text); border: 1px solid var(--border);
            padding: 9px 10px; border-radius: 10px; font-weight: 600; font-size: 12px; cursor: pointer;
        }
        .setting-label { font-size: 13px; font-weight: 600; margin-bottom: 7px; display: block; }
        .input-row { display: flex; gap: 8px; margin-bottom: 10px; }
        input[type="number"], select {
            flex: 1; padding: 9px 11px; border-radius: 10px; border: 1px solid var(--border);
            background: var(--bg); color: var(--text); font-size: 14px; outline: none;
        }
        .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin: 10px 0; }
        .save-btn {
            background: var(--accent); color: var(--accent-text); border: none;
            padding: 11px 16px; border-radius: 10px; font-weight: 700; font-size: 14px;
            cursor: pointer; width: 100%; margin-top: 4px;
        }
        .presets { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; margin-bottom: 12px; }
        .preset-btn {
            background: var(--bg); color: var(--text); border: 1px solid var(--border);
            padding: 7px 4px; border-radius: 8px; font-size: 11px; font-weight: 600; cursor: pointer;
        }
        .toast {
            position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%);
            background: #222733; color: #fff; padding: 10px 16px; border-radius: 10px;
            font-size: 13px; font-weight: 600; opacity: 0; pointer-events: none;
            transition: opacity 0.25s; box-shadow: 0 6px 20px rgba(0,0,0,0.4); z-index: 99;
            max-width: 92%; text-align: center;
        }
        .toast.show { opacity: 1; }
    </style>
</head>
<body>
    <div class="header">
        <div class="title"><span class="live-dot"></span> Скринер</div>
        <div class="header-right">
            <button class="tiger-pill disconnected" id="tiger-status-btn" onclick="onTigerPillClick()" title="Статус Tiger.Trade">
                <span class="tiger-dot"></span> 🐯 Tiger
            </button>
            <button class="sound-btn on" id="sound-toggle-btn" onclick="toggleSound()">🔊</button>
            <div class="utc-badge" id="utc-clock">UTC --:--:--</div>
        </div>
    </div>

    <div class="tabs">
        <button class="tab-btn active" onclick="switchTab('upticks', this)">Аптики</button>
        <button class="tab-btn" onclick="switchTab('densities', this)">Плотности</button>
        <button class="tab-btn" onclick="switchTab('live', this)">Сейчас (<span id="live-count">0</span>)</button>
        <button class="tab-btn" onclick="switchTab('settings', this)">Настройки</button>
    </div>

    <!-- ВКЛАДКА 1: АПТИКИ СЕГОДНЯ -->
    <div id="panel-upticks" class="panel active">
        <div class="filters" id="uptick-ex-filters">
            <button class="chip active" onclick="setUptickEx('ALL', this)">Все (4)</button>
            <button class="chip" data-ex="Binance" onclick="setUptickEx('Binance', this)"></button>
            <button class="chip" data-ex="Bybit" onclick="setUptickEx('Bybit', this)"></button>
            <button class="chip" data-ex="OKX" onclick="setUptickEx('OKX', this)"></button>
            <button class="chip" data-ex="Aster" onclick="setUptickEx('Aster', this)"></button>
        </div>
        <div id="history-list"><div class="empty">Загрузка данных...</div></div>
    </div>

    <!-- ВКЛАДКА 2: ПЛОТНОСТИ СЕГОДНЯ -->
    <div id="panel-densities" class="panel">
        <div class="filters" id="density-mkt-filters">
            <button class="chip active" onclick="setDensityMkt('ALL', this)">Спот + Фьючи</button>
            <button class="chip" onclick="setDensityMkt('SPOT', this)">Только СПОТ</button>
            <button class="chip" onclick="setDensityMkt('FUTURES', this)">Только ФЬЮЧЕРСЫ</button>
        </div>
        <div class="filters" id="density-ex-filters">
            <button class="chip active" onclick="setDensityEx('ALL', this)">Все (7)</button>
            <button class="chip" data-ex="Binance" onclick="setDensityEx('Binance', this)"></button>
            <button class="chip" data-ex="Bybit" onclick="setDensityEx('Bybit', this)"></button>
            <button class="chip" data-ex="OKX" onclick="setDensityEx('OKX', this)"></button>
            <button class="chip" data-ex="KuCoin" onclick="setDensityEx('KuCoin', this)"></button>
            <button class="chip" data-ex="Aster" onclick="setDensityEx('Aster', this)"></button>
            <button class="chip" data-ex="Gate" onclick="setDensityEx('Gate', this)"></button>
            <button class="chip" data-ex="Bitget" onclick="setDensityEx('Bitget', this)"></button>
        </div>
        <div id="density-list"><div class="empty">Загрузка плотностей...</div></div>
    </div>

    <!-- ВКЛАДКА 3: СЕЙЧАС В РЕАЛЬНОМ ВРЕМЕНИ -->
    <div id="panel-live" class="panel">
        <div class="section-title">⚡ Аптики прямо сейчас (клик — открыть в Tiger)</div>
        <div id="live-upticks-list"><div class="empty">Сейчас активных планок нет.</div></div>

        <div class="section-title">🧱 Плотности в стакане сейчас (клик — открыть в Tiger)</div>
        <div id="live-densities-list"><div class="empty">Сейчас крупных плотностей в стакане не найдено.</div></div>
    </div>

    <!-- ВКЛАДКА 4: НАСТРОЙКИ -->
    <div id="panel-settings" class="panel">
        <!-- БЛОК TIGER.TRADE -->
        <div class="setting-box">
            <div class="setting-row">
                <div>
                    <div class="setting-label" style="margin-bottom:2px;">🐯 Терминал Tiger.Trade</div>
                    <div class="meta" id="tiger-conn-text">Статус: не подключен (ws://127.0.0.1:7819)</div>
                </div>
                <button class="sound-btn on" id="tiger-enable-btn" onclick="toggleTigerEnabled()">Вкл</button>
            </div>

            <div class="setting-row">
                <div>
                    <div class="setting-label" style="margin-bottom:2px;">Автооткрытие: Аптики</div>
                    <div class="meta">Сам открывает монету при новом аптике</div>
                </div>
                <button class="sound-btn off" id="tiger-auto-uptick-btn" onclick="toggleTigerAuto('uptick')">Выкл</button>
            </div>

            <div class="setting-row">
                <div>
                    <div class="setting-label" style="margin-bottom:2px;">Автооткрытие: Плотности</div>
                    <div class="meta">Сам открывает монету при новой плотности</div>
                </div>
                <button class="sound-btn off" id="tiger-auto-density-btn" onclick="toggleTigerAuto('density')">Выкл</button>
            </div>

            <div class="setting-row">
                <div>
                    <div class="setting-label" style="margin-bottom:2px;">Подключение TigerX (брокер)</div>
                    <div class="meta">Включите, если торгуете через Tiger.com Broker</div>
                </div>
                <button class="sound-btn off" id="tiger-broker-btn" onclick="toggleTigerBroker()">Выкл</button>
            </div>

            <div class="two-col">
                <div>
                    <label class="setting-label">Группа линковки</label>
                    <select id="tiger-group-select" onchange="saveTigerConfig()"></select>
                </div>
                <div>
                    <label class="setting-label">Порт (по умолч. 7819)</label>
                    <input type="number" id="tiger-port-input" value="7819" min="1024" max="65535" onchange="saveTigerConfig(true)">
                </div>
            </div>

            <div class="btn-grid">
                <button class="secondary-btn" onclick="reconnectTiger(true)">🔄 Подключить</button>
                <button class="secondary-btn" onclick="testTigerOpen()">▶ Тест: PEPEUSDT</button>
            </div>

            <p class="meta" style="margin:10px 0 0;">
                В терминале Tiger.Trade откройте: <b>Настройки → Локальный сервер сигналов → Включить сервер сигналов → Сохранить</b>.<br>
                Нажмите на любую монету в списке, чтобы открыть её в терминале. Работает в Telegram на ПК.
            </p>
        </div>

        <!-- БЛОК ЗВУКА -->
        <div class="setting-box">
            <div class="setting-row">
                <div>
                    <div class="setting-label" style="margin-bottom:2px;">Голосовые оповещения</div>
                    <div class="meta">Озвучивает аптики и плотности с биржей</div>
                </div>
                <button class="sound-btn on" id="sound-settings-btn" onclick="toggleSound()">🔊 Вкл</button>
            </div>
            <div class="btn-grid">
                <button class="secondary-btn" onclick="testUptickSound()">▶ Тест: Аптик</button>
                <button class="secondary-btn" onclick="testDensitySound()">▶ Тест: Плотность</button>
            </div>
        </div>

        <!-- БЛОК ФИЛЬТРОВ -->
        <div class="setting-box">
            <label class="setting-label">🧱 Мин. плотность на СПОТЕ ($)</label>
            <div class="input-row">
                <input type="number" id="min-dspot-input" min="250000" step="50000" placeholder="250000">
            </div>
            <div class="presets">
                <button class="preset-btn" onclick="setField('min-dspot-input', 250000)">$250K</button>
                <button class="preset-btn" onclick="setField('min-dspot-input', 500000)">$500K</button>
                <button class="preset-btn" onclick="setField('min-dspot-input', 1000000)">$1M</button>
                <button class="preset-btn" onclick="setField('min-dspot-input', 2000000)">$2M</button>
            </div>

            <label class="setting-label">🧱 Мин. плотность на ФЬЮЧЕРСАХ ($)</label>
            <div class="input-row">
                <input type="number" id="min-dfut-input" min="1000000" step="250000" placeholder="1000000">
            </div>
            <div class="presets">
                <button class="preset-btn" onclick="setField('min-dfut-input', 1000000)">$1M</button>
                <button class="preset-btn" onclick="setField('min-dfut-input', 2000000)">$2M</button>
                <button class="preset-btn" onclick="setField('min-dfut-input', 3000000)">$3M</button>
                <button class="preset-btn" onclick="setField('min-dfut-input', 5000000)">$5M</button>
            </div>

            <label class="setting-label">⚡ Мин. оборот 24ч ($) для Аптиков</label>
            <div class="input-row">
                <input type="number" id="min-vol-input" min="100000" step="50000" placeholder="100000">
            </div>
            <div class="presets">
                <button class="preset-btn" onclick="setField('min-vol-input', 100000)">$100K</button>
                <button class="preset-btn" onclick="setField('min-vol-input', 500000)">$500K</button>
                <button class="preset-btn" onclick="setField('min-vol-input', 1000000)">$1M</button>
                <button class="preset-btn" onclick="setField('min-vol-input', 5000000)">$5M</button>
            </div>

            <button class="save-btn" onclick="saveSettings()">💾 Сохранить все фильтры</button>

            <p class="meta" style="margin-top:12px;">
                Аптики (4): Binance, Bybit, OKX, Aster.<br>
                Плотности (7): Binance, Bybit, OKX, KuCoin, Aster, Gate, Bitget.<br>
                Тяжелые монеты (BTC, ETH, SOL, ZEC...), акции и металлы исключены.
            </p>
        </div>
    </div>

    <div class="toast" id="toast">Сохранено</div>

    <script>
        const tg = window.Telegram.WebApp;
        tg.expand();

        const userId = tg.initDataUnsafe?.user?.id || 0;
        let selectedUptickEx = 'ALL';
        let selectedDensityEx = 'ALL';
        let selectedDensityMkt = 'ALL';

        let cachedHistory = [];
        let cachedDensities = [];
        let settingsInitialized = false;

        let isFirstLoad = true;
        let lastSeenUptickId = 0;
        let lastSeenDensityId = 0;

        // =====================================================================
        // TIGER.TRADE LOCAL SIGNAL SERVER (WebSocket ws://127.0.0.1:7819)
        // =====================================================================
        let tigerEnabled = localStorage.getItem('tiger_enabled') !== 'off';
        let tigerAutoUptick = localStorage.getItem('tiger_auto_uptick') === 'on';
        let tigerAutoDensity = localStorage.getItem('tiger_auto_density') === 'on';
        let tigerUseBroker = localStorage.getItem('tiger_use_broker') === 'on';
        let tigerPort = Number(localStorage.getItem('tiger_port')) || 7819;
        let tigerLinkGroup = localStorage.getItem('tiger_link_group') || 'A';

        let tigerWs = null;
        let tigerConnected = false;
        let tigerReconnectTimer = null;
        let tigerPingTimer = null;

        // Заполняем селект групп линковки A–Z и 0–11
        (function initLinkGroups() {
            const sel = document.getElementById('tiger-group-select');
            const groups = [];
            for (let c = 65; c <= 90; c++) groups.push(String.fromCharCode(c));
            for (let n = 0; n <= 11; n++) groups.push(String(n));
            sel.innerHTML = groups.map(g => `<option value="${g}">Группа ${g}</option>`).join('');
            sel.value = tigerLinkGroup;
            document.getElementById('tiger-port-input').value = tigerPort;
        })();

        function mapExchangeForTiger(exchange) {
            if (tigerUseBroker) {
                if (exchange === 'Binance') return 'TigerX Binance';
                if (exchange === 'Bybit') return 'TigerX Bybit';
                if (exchange === 'OKX') return 'TigerX OKX';
            }
            const map = {
                'Binance': 'BINANCE',
                'Bybit': 'BYBIT',
                'OKX': 'OKX',
                'Gate': 'Gate.io',
                'Bitget': 'Bitget',
                'KuCoin': 'KuCoin',
                'Aster': 'Aster'
            };
            return map[exchange] || exchange.toUpperCase();
        }

        function updateTigerUI() {
            const pill = document.getElementById('tiger-status-btn');
            const connText = document.getElementById('tiger-conn-text');
            const enBtn = document.getElementById('tiger-enable-btn');
            const autoUBtn = document.getElementById('tiger-auto-uptick-btn');
            const autoDBtn = document.getElementById('tiger-auto-density-btn');
            const brokerBtn = document.getElementById('tiger-broker-btn');

            if (tigerConnected) {
                pill.className = 'tiger-pill connected';
                connText.textContent = `Статус: подключен к ws://127.0.0.1:${tigerPort} (группа ${tigerLinkGroup})`;
            } else {
                pill.className = 'tiger-pill disconnected';
                connText.textContent = tigerEnabled
                    ? `Статус: ожидание Tiger.Trade на порту ${tigerPort}...`
                    : 'Статус: интеграция выключена';
            }

            enBtn.textContent = tigerEnabled ? 'Вкл' : 'Выкл';
            enBtn.className = tigerEnabled ? 'sound-btn on' : 'sound-btn off';

            autoUBtn.textContent = tigerAutoUptick ? 'Вкл' : 'Выкл';
            autoUBtn.className = tigerAutoUptick ? 'sound-btn on' : 'sound-btn off';

            autoDBtn.textContent = tigerAutoDensity ? 'Вкл' : 'Выкл';
            autoDBtn.className = tigerAutoDensity ? 'sound-btn on' : 'sound-btn off';

            brokerBtn.textContent = tigerUseBroker ? 'Вкл' : 'Выкл';
            brokerBtn.className = tigerUseBroker ? 'sound-btn on' : 'sound-btn off';
        }

        function connectTiger() {
            if (tigerReconnectTimer) { clearTimeout(tigerReconnectTimer); tigerReconnectTimer = null; }
            if (tigerPingTimer) { clearInterval(tigerPingTimer); tigerPingTimer = null; }

            if (!tigerEnabled) {
                if (tigerWs) { try { tigerWs.close(); } catch (e) {} tigerWs = null; }
                tigerConnected = false;
                updateTigerUI();
                return;
            }

            if (tigerWs && (tigerWs.readyState === WebSocket.OPEN || tigerWs.readyState === WebSocket.CONNECTING)) {
                return;
            }

            try {
                tigerWs = new WebSocket(`ws://127.0.0.1:${tigerPort}`);

                tigerWs.onopen = () => {
                    tigerConnected = true;
                    updateTigerUI();
                    tigerPingTimer = setInterval(() => {
                        if (tigerWs && tigerWs.readyState === WebSocket.OPEN) {
                            try { tigerWs.send('ping'); } catch (e) {}
                        }
                    }, 25000);
                };

                tigerWs.onclose = () => {
                    tigerConnected = false;
                    updateTigerUI();
                    if (tigerPingTimer) { clearInterval(tigerPingTimer); tigerPingTimer = null; }
                    if (tigerEnabled) {
                        tigerReconnectTimer = setTimeout(connectTiger, 5000);
                    }
                };

                tigerWs.onerror = () => {
                    tigerConnected = false;
                    updateTigerUI();
                };

                tigerWs.onmessage = (evt) => {
                    try {
                        const msg = JSON.parse(evt.data);
                        if (msg?.response?.type === 'error' && msg?.message) {
                            showToast(`🐯 Tiger: ${msg.message}`);
                        }
                    } catch (e) {}
                };
            } catch (e) {
                tigerConnected = false;
                updateTigerUI();
            }
        }

        function reconnectTiger(manual = false) {
            if (tigerWs) { try { tigerWs.close(); } catch (e) {} tigerWs = null; }
            tigerConnected = false;
            connectTiger();
            if (manual) showToast(`🔄 Подключение к Tiger.Trade (порт ${tigerPort})...`);
        }

        function onTigerPillClick() {
            if (tigerConnected) {
                showToast(`🐯 Tiger подключен (группа ${tigerLinkGroup}). Нажмите на любую монету!`);
            } else {
                reconnectTiger(true);
            }
        }

        function toggleTigerEnabled() {
            tigerEnabled = !tigerEnabled;
            localStorage.setItem('tiger_enabled', tigerEnabled ? 'on' : 'off');
            updateTigerUI();
            if (tigerEnabled) connectTiger();
            else {
                if (tigerWs) { try { tigerWs.close(); } catch (e) {} tigerWs = null; }
                tigerConnected = false;
                updateTigerUI();
            }
        }

        function toggleTigerAuto(kind) {
            if (kind === 'uptick') {
                tigerAutoUptick = !tigerAutoUptick;
                localStorage.setItem('tiger_auto_uptick', tigerAutoUptick ? 'on' : 'off');
                showToast(tigerAutoUptick ? '🐯 Автооткрытие аптиков ВКЛ' : 'Автооткрытие аптиков ВЫКЛ');
            } else {
                tigerAutoDensity = !tigerAutoDensity;
                localStorage.setItem('tiger_auto_density', tigerAutoDensity ? 'on' : 'off');
                showToast(tigerAutoDensity ? '🐯 Автооткрытие плотностей ВКЛ' : 'Автооткрытие плотностей ВЫКЛ');
            }
            updateTigerUI();
        }

        function toggleTigerBroker() {
            tigerUseBroker = !tigerUseBroker;
            localStorage.setItem('tiger_use_broker', tigerUseBroker ? 'on' : 'off');
            updateTigerUI();
            showToast(tigerUseBroker ? '🐯 Режим TigerX Broker ВКЛ' : '🐯 Обычные подключения бирж');
        }

        function saveTigerConfig(needReconnect = false) {
            tigerLinkGroup = document.getElementById('tiger-group-select').value || 'A';
            tigerPort = Number(document.getElementById('tiger-port-input').value) || 7819;
            localStorage.setItem('tiger_link_group', tigerLinkGroup);
            localStorage.setItem('tiger_port', String(tigerPort));
            updateTigerUI();
            if (needReconnect) reconnectTiger(true);
            else showToast(`🐯 Группа линковки: ${tigerLinkGroup}`);
        }

        function openInTiger(exchange, marketType, symbol, isAuto = false) {
            if (!tigerEnabled) {
                if (!isAuto) showToast('Включите Tiger.Trade во вкладке Настройки');
                return;
            }
            if (!tigerWs || tigerWs.readyState !== WebSocket.OPEN) {
                if (!isAuto) {
                    showToast('🐯 Tiger.Trade не подключен! Включите Локальный сервер сигналов (порт ' + tigerPort + ')');
                    connectTiger();
                }
                return;
            }

            const payload = {
                type: 'setLinkSymbol',
                e: mapExchangeForTiger(exchange),
                m: (marketType || 'FUTURES').toUpperCase(),
                symbol: (symbol || '').toUpperCase(),
                linkGroup: tigerLinkGroup
            };

            try {
                tigerWs.send(JSON.stringify(payload));
                try { tg.HapticFeedback?.impactOccurred('light'); } catch (e) {}
                const mktLabel = payload.m === 'SPOT' ? 'SPOT' : 'FUT';
                showToast(`🐯 ${isAuto ? 'Авто: ' : ''}${payload.symbol} (${exchange} ${mktLabel}, гр. ${tigerLinkGroup})`);
            } catch (e) {
                if (!isAuto) showToast('Ошибка отправки команды в Tiger.Trade');
            }
        }

        function testTigerOpen() {
            openInTiger('Binance', 'FUTURES', 'PEPEUSDT', false);
        }

        updateTigerUI();
        connectTiger();

        // =====================================================================
        // ЗВУК И ГОЛОСОВАЯ ОЗВУЧКА
        // =====================================================================
        let soundEnabled = localStorage.getItem('uptick_sound') !== 'off';
        let audioCtx = null;

        const EXCHANGE_PRONOUNCE = {
            'Binance': 'Бинанс',
            'Bybit': 'Байбит',
            'OKX': 'О Кей Икс',
            'KuCoin': 'Кукоин',
            'Aster': 'Астер',
            'Gate': 'Гейт',
            'Bitget': 'Битгет'
        };

        const EXCHANGE_LOGOS = {
            'Binance': `<svg viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">
                <rect width="32" height="32" rx="7" fill="#1E2026"/>
                <path fill="#F3BA2F" d="M16 6l3.5 3.5-6.2 6.2-3.5-3.5L16 6zm0 7.2l2.8 2.8-2.8 2.8-2.8-2.8 2.8-2.8zm-7.5 0l2.8 2.8-2.8 2.8-2.8-2.8 2.8-2.8zm15 0l2.8 2.8-2.8 2.8-2.8-2.8 2.8-2.8zM16 26l-6.2-6.2 3.5-3.5 2.7 2.7 2.7-2.7 3.5 3.5L16 26z"/>
            </svg>`,
            'Bybit': `<svg viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">
                <rect width="32" height="32" rx="7" fill="#17181E"/>
                <text x="5" y="21" font-family="Arial,sans-serif" font-weight="900" font-size="13" fill="#FFFFFF">BY</text>
                <rect x="23" y="10" width="3.5" height="11" rx="1" fill="#F7A600"/>
            </svg>`,
            'OKX': `<svg viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">
                <rect width="32" height="32" rx="7" fill="#0A0A0A"/>
                <rect x="7" y="7" width="5.5" height="5.5" rx="1" fill="#FFFFFF"/>
                <rect x="19.5" y="7" width="5.5" height="5.5" rx="1" fill="#FFFFFF"/>
                <rect x="13.25" y="13.25" width="5.5" height="5.5" rx="1" fill="#FFFFFF"/>
                <rect x="7" y="19.5" width="5.5" height="5.5" rx="1" fill="#FFFFFF"/>
                <rect x="19.5" y="19.5" width="5.5" height="5.5" rx="1" fill="#FFFFFF"/>
            </svg>`,
            'KuCoin': `<svg viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">
                <rect width="32" height="32" rx="7" fill="#23AF91"/>
                <path fill="none" stroke="#FFFFFF" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round" d="M10 9v14M10 16l8-7 5 4.5M10 16l8 7 5-4.5"/>
                <circle cx="18" cy="16" r="1.8" fill="#FFFFFF"/>
            </svg>`,
            'Aster': `<svg viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">
                <rect width="32" height="32" rx="7" fill="#241B4B"/>
                <path fill="#A78BFA" d="M16 5l2.6 7.4L26 15l-7.4 2.6L16 25l-2.6-7.4L6 15l7.4-2.6L16 5z"/>
                <circle cx="16" cy="15" r="2.2" fill="#FFFFFF"/>
            </svg>`,
            'Gate': `<svg viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">
                <rect width="32" height="32" rx="7" fill="#0F172A"/>
                <path fill="#2354E6" d="M16 6a10 10 0 1 0 10 10h-4.5A5.5 5.5 0 1 1 16 10.5V6z"/>
                <rect x="16" y="10.5" width="5.5" height="5.5" fill="#17E6A1"/>
            </svg>`,
            'Bitget': `<svg viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">
                <rect width="32" height="32" rx="7" fill="#00F0FF"/>
                <path fill="#111318" d="M10 11l6.5 5L10 21h4.5l6.5-5-6.5-5H10zm7.5 0L24 16l-6.5 5H22l6-5-6-5h-4.5z"/>
            </svg>`
        };

        function getExchangeLogoHtml(exchange) {
            const svg = EXCHANGE_LOGOS[exchange];
            if (!svg) return `<span class="ex-tag">${exchange}</span>`;
            return `<span class="ex-logo" title="${exchange}" aria-label="${exchange}">${svg}</span>`;
        }

        document.querySelectorAll('.chip[data-ex]').forEach(btn => {
            const ex = btn.getAttribute('data-ex');
            btn.innerHTML = getExchangeLogoHtml(ex);
            btn.title = ex;
        });

        function unlockAudio() {
            try {
                if (!audioCtx) {
                    const AudioContextClass = window.AudioContext || window.webkitAudioContext;
                    if (AudioContextClass) audioCtx = new AudioContextClass();
                }
                if (audioCtx && audioCtx.state === 'suspended') {
                    audioCtx.resume();
                }
            } catch (e) {}
        }
        document.addEventListener('click', unlockAudio, { once: true });
        document.addEventListener('touchstart', unlockAudio, { once: true });

        function updateSoundButtons() {
            const topBtn = document.getElementById('sound-toggle-btn');
            const setBtn = document.getElementById('sound-settings-btn');
            if (soundEnabled) {
                topBtn.textContent = '🔊';
                topBtn.className = 'sound-btn on';
                setBtn.textContent = '🔊 Вкл';
                setBtn.className = 'sound-btn on';
            } else {
                topBtn.textContent = '🔇';
                topBtn.className = 'sound-btn off';
                setBtn.textContent = '🔇 Выкл';
                setBtn.className = 'sound-btn off';
            }
        }

        function toggleSound() {
            unlockAudio();
            soundEnabled = !soundEnabled;
            localStorage.setItem('uptick_sound', soundEnabled ? 'on' : 'off');
            updateSoundButtons();
            if (soundEnabled) {
                playChime([660, 880]);
                showToast('🔊 Звук и голос включены');
            } else {
                if ('speechSynthesis' in window) window.speechSynthesis.cancel();
                showToast('🔇 Звук выключен');
            }
        }

        function playChime(freqs) {
            try {
                unlockAudio();
                if (!audioCtx) return;
                const now = audioCtx.currentTime;
                freqs.forEach((freq, idx) => {
                    const osc = audioCtx.createOscillator();
                    const gain = audioCtx.createGain();
                    osc.type = 'sine';
                    osc.frequency.setValueAtTime(freq, now + idx * 0.11);
                    gain.gain.setValueAtTime(0.001, now + idx * 0.11);
                    gain.gain.exponentialRampToValueAtTime(0.18, now + idx * 0.11 + 0.02);
                    gain.gain.exponentialRampToValueAtTime(0.001, now + idx * 0.11 + 0.22);
                    osc.connect(gain);
                    gain.connect(audioCtx.destination);
                    osc.start(now + idx * 0.11);
                    osc.stop(now + idx * 0.11 + 0.24);
                });
            } catch (e) {}
        }

        function speakText(phrase) {
            if ('speechSynthesis' in window) {
                try {
                    const utter = new SpeechSynthesisUtterance(phrase);
                    utter.lang = 'ru-RU';
                    utter.rate = 1.05;
                    utter.volume = 1.0;
                    window.speechSynthesis.speak(utter);
                } catch (e) {}
            }
        }

        function speakUptick(exchange, symbol, isLong) {
            if (!soundEnabled) return;
            playChime(isLong ? [660, 880] : [660, 440]);
            try { tg.HapticFeedback?.notificationOccurred('warning'); } catch (e) {}
            const exName = EXCHANGE_PRONOUNCE[exchange] || exchange;
            const cleanSym = (symbol || '').replace(/USDT$/i, '');
            const sideText = isLong ? 'лонг' : 'шорт';
            speakText(`Аптик на бирже ${exName}. Монета ${cleanSym}. ${sideText}`);
        }

        function speakDensity(exchange, marketType, symbol, isBid) {
            if (!soundEnabled) return;
            playChime([523, 659, 784]);
            try { tg.HapticFeedback?.notificationOccurred('success'); } catch (e) {}
            const exName = EXCHANGE_PRONOUNCE[exchange] || exchange;
            const mktName = marketType === 'SPOT' ? 'спот' : 'фьючерс';
            const cleanSym = (symbol || '').replace(/USDT$/i, '');
            const sideText = isBid ? 'покупка' : 'продажа';
            speakText(`Плотность на бирже ${exName}, ${mktName}. Монета ${cleanSym}. ${sideText}`);
        }

        function testUptickSound() {
            unlockAudio();
            if (!soundEnabled) toggleSound();
            speakUptick('Aster', 'PEPEUSDT', true);
            showToast('🔊 Тест: Аптик на бирже Aster');
        }

        function testDensitySound() {
            unlockAudio();
            if (!soundEnabled) toggleSound();
            speakDensity('Binance', 'SPOT', 'WIFUSDT', true);
            showToast('🔊 Тест: Плотность на Binance Спот');
        }

        updateSoundButtons();

        function updateClock() {
            const now = new Date();
            const hh = String(now.getUTCHours()).padStart(2, '0');
            const mm = String(now.getUTCMinutes()).padStart(2, '0');
            const ss = String(now.getUTCSeconds()).padStart(2, '0');
            document.getElementById('utc-clock').textContent = `UTC ${hh}:${mm}:${ss}`;
        }
        setInterval(updateClock, 1000);
        updateClock();

        function showToast(msg) {
            const el = document.getElementById('toast');
            el.textContent = msg;
            el.classList.add('show');
            setTimeout(() => el.classList.remove('show'), 2500);
        }

        function switchTab(name, btn) {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
            btn.classList.add('active');
            document.getElementById('panel-' + name).classList.add('active');
        }

        function setUptickEx(ex, btn) {
            selectedUptickEx = ex;
            document.querySelectorAll('#uptick-ex-filters .chip').forEach(c => c.classList.remove('active'));
            btn.classList.add('active');
            renderHistory();
        }

        function setDensityEx(ex, btn) {
            selectedDensityEx = ex;
            document.querySelectorAll('#density-ex-filters .chip').forEach(c => c.classList.remove('active'));
            btn.classList.add('active');
            renderDensities();
        }

        function setDensityMkt(mkt, btn) {
            selectedDensityMkt = mkt;
            document.querySelectorAll('#density-mkt-filters .chip').forEach(c => c.classList.remove('active'));
            btn.classList.add('active');
            renderDensities();
        }

        function formatVol(v) {
            if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
            if (v >= 1e3) return '$' + Math.round(v / 1e3) + 'K';
            return '$' + Math.round(v);
        }

        function renderHistory() {
            const list = document.getElementById('history-list');
            const filtered = selectedUptickEx === 'ALL'
                ? cachedHistory
                : cachedHistory.filter(i => i.exchange === selectedUptickEx);

            if (!filtered.length) {
                list.innerHTML = '<div class="empty">Сегодня касаний планки по выбранному фильтру еще не было.</div>';
                return;
            }

            list.innerHTML = filtered.map(item => {
                const isLong = item.side.includes('LONG');
                return `
                    <div class="card" onclick="openInTiger('${item.exchange}', 'FUTURES', '${item.symbol}', false)">
                        <div>
                            <div class="sym-row">
                                ${getExchangeLogoHtml(item.exchange)}
                                <span class="symbol">${item.symbol}</span>
                                <span class="tiger-open-hint">🐯</span>
                            </div>
                            <div class="meta">
                                Цена: ${item.price} • Планка: ${Number(item.limit_price).toPrecision(5)}<br>
                                Оборот 24ч: <b>${formatVol(item.volume)}</b>
                            </div>
                        </div>
                        <div class="right-col">
                            <div class="side-badge ${isLong ? 'side-long' : 'side-short'}">
                                ${isLong ? '▲ LONG' : '▼ SHORT'}
                            </div>
                            <div class="time-str">${item.timestamp_utc}</div>
                        </div>
                    </div>
                `;
            }).join('');
        }

        function renderDensities() {
            const list = document.getElementById('density-list');
            const filtered = cachedDensities.filter(i => {
                if (selectedDensityEx !== 'ALL' && i.exchange !== selectedDensityEx) return false;
                if (selectedDensityMkt !== 'ALL' && i.market_type !== selectedDensityMkt) return false;
                return true;
            });

            if (!filtered.length) {
                list.innerHTML = '<div class="empty">Сегодня крупных плотностей по выбранным фильтрам еще не найдено.</div>';
                return;
            }

            list.innerHTML = filtered.map(item => {
                const isBid = item.side.includes('BID');
                const isSpot = item.market_type === 'SPOT';
                return `
                    <div class="card" onclick="openInTiger('${item.exchange}', '${item.market_type}', '${item.symbol}', false)">
                        <div>
                            <div class="sym-row">
                                ${getExchangeLogoHtml(item.exchange)}
                                <span class="symbol">${item.symbol}</span>
                                <span class="ex-tag ${isSpot ? 'mkt-spot' : 'mkt-fut'}">${isSpot ? 'SPOT' : 'FUT'}</span>
                                <span class="tiger-open-hint">🐯</span>
                            </div>
                            <div class="meta">
                                Уровень: <b>${item.price}</b> (до цены: ${item.distance_pct}%)<br>
                                Оборот 24ч: ${formatVol(item.volume)}
                            </div>
                        </div>
                        <div class="right-col">
                            <div class="size-big">${formatVol(item.size_usd)}</div>
                            <div class="side-badge ${isBid ? 'side-long' : 'side-short'}">
                                ${isBid ? '🟢 BID' : '🔴 ASK'}
                            </div>
                            <div class="time-str">${item.timestamp_utc}</div>
                        </div>
                    </div>
                `;
            }).join('');
        }

        function renderLive(activeUpticks, activeDensities) {
            document.getElementById('live-count').textContent = activeUpticks.length + activeDensities.length;

            const uList = document.getElementById('live-upticks-list');
            if (!activeUpticks.length) {
                uList.innerHTML = '<div class="empty">Сейчас активных планок нет.</div>';
            } else {
                uList.innerHTML = activeUpticks.map(item => {
                    const isLong = item.side.includes('LONG');
                    return `
                        <div class="card" onclick="openInTiger('${item.exchange}', 'FUTURES', '${item.symbol}', false)">
                            <div class="sym-row" style="margin:0;">
                                ${getExchangeLogoHtml(item.exchange)}
                                <span class="symbol">${item.symbol}</span>
                                <span class="tiger-open-hint">🐯</span>
                            </div>
                            <div class="side-badge ${isLong ? 'side-long' : 'side-short'}" style="margin:0;">
                                ${isLong ? '▲ LONG' : '▼ SHORT'}
                            </div>
                        </div>
                    `;
                }).join('');
            }

            const dList = document.getElementById('live-densities-list');
            if (!activeDensities.length) {
                dList.innerHTML = '<div class="empty">Сейчас крупных плотностей в стакане не найдено.</div>';
            } else {
                dList.innerHTML = activeDensities.map(item => {
                    const isBid = item.side.includes('BID');
                    const isSpot = item.market_type === 'SPOT';
                    return `
                        <div class="card" onclick="openInTiger('${item.exchange}', '${item.market_type}', '${item.symbol}', false)">
                            <div>
                                <div class="sym-row">
                                    ${getExchangeLogoHtml(item.exchange)}
                                    <span class="symbol">${item.symbol}</span>
                                    <span class="ex-tag ${isSpot ? 'mkt-spot' : 'mkt-fut'}">${isSpot ? 'SPOT' : 'FUT'}</span>
                                    <span class="tiger-open-hint">🐯</span>
                                </div>
                                <div class="meta">Цена: ${item.price} (${item.distance_pct}%)</div>
                            </div>
                            <div class="right-col">
                                <div class="size-big">${formatVol(item.size_usd)}</div>
                                <div class="side-badge ${isBid ? 'side-long' : 'side-short'}" style="margin:0;">
                                    ${isBid ? '🟢 BID' : '🔴 ASK'}
                                </div>
                            </div>
                        </div>
                    `;
                }).join('');
            }
        }

        async function loadData() {
            try {
                const res = await fetch(`/api/data?user_id=${userId}`);
                const data = await res.json();
                const history = data.history || [];
                const densities = data.densities || [];

                if (isFirstLoad) {
                    if (history.length) lastSeenUptickId = Math.max(...history.map(i => Number(i.id) || 0));
                    if (densities.length) lastSeenDensityId = Math.max(...densities.map(i => Number(i.id) || 0));
                    isFirstLoad = false;
                } else {
                    // Новые аптики
                    const newUpticks = history
                        .filter(i => (Number(i.id) || 0) > lastSeenUptickId)
                        .sort((a, b) => (Number(a.id) || 0) - (Number(b.id) || 0));
                    if (newUpticks.length) {
                        lastSeenUptickId = Math.max(...newUpticks.map(i => Number(i.id) || 0));
                        newUpticks.forEach(item => {
                            speakUptick(item.exchange, item.symbol, (item.side || '').includes('LONG'));
                            showToast(`🚨 АПТИК: ${item.symbol} (${item.exchange})`);
                            if (tigerAutoUptick) {
                                openInTiger(item.exchange, 'FUTURES', item.symbol, true);
                            }
                        });
                    }

                    // Новые плотности
                    const newDensities = densities
                        .filter(i => (Number(i.id) || 0) > lastSeenDensityId)
                        .sort((a, b) => (Number(a.id) || 0) - (Number(b.id) || 0));
                    if (newDensities.length) {
                        lastSeenDensityId = Math.max(...newDensities.map(i => Number(i.id) || 0));
                        newDensities.slice(0, 2).forEach(item => {
                            speakDensity(
                                item.exchange,
                                item.market_type,
                                item.symbol,
                                (item.side || '').includes('BID')
                            );
                            showToast(`🧱 ПЛОТНОСТЬ: ${item.symbol} ${formatVol(item.size_usd)} (${item.exchange})`);
                            if (tigerAutoDensity) {
                                openInTiger(item.exchange, item.market_type, item.symbol, true);
                            }
                        });
                    }
                }

                cachedHistory = history;
                cachedDensities = densities;
                renderHistory();
                renderDensities();
                renderLive(data.active || [], data.active_densities || []);

                if (!settingsInitialized) {
                    if (data.user_min_volume) document.getElementById('min-vol-input').value = Math.round(data.user_min_volume);
                    if (data.user_min_density_spot) document.getElementById('min-dspot-input').value = Math.round(data.user_min_density_spot);
                    if (data.user_min_density_futures) document.getElementById('min-dfut-input').value = Math.round(data.user_min_density_futures);
                    settingsInitialized = true;
                }
            } catch (e) {
                console.error('Ошибка загрузки:', e);
            }
        }

        function setField(id, val) {
            document.getElementById(id).value = val;
        }

        async function saveSettings() {
            const minVol = Number(document.getElementById('min-vol-input').value);
            const minSpot = Number(document.getElementById('min-dspot-input').value);
            const minFut = Number(document.getElementById('min-dfut-input').value);

            if (!minVol || minVol < 100000) { showToast('Мин. оборот от $100,000'); return; }
            if (!minSpot || minSpot < 250000) { showToast('Мин. плотность Спот от $250,000'); return; }
            if (!minFut || minFut < 1000000) { showToast('Мин. плотность Фьючерс от $1,000,000'); return; }
            if (!userId) { showToast('Откройте приложение через Telegram'); return; }

            try {
                const res = await fetch('/api/settings', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        user_id: userId,
                        min_volume: minVol,
                        min_density_spot: minSpot,
                        min_density_futures: minFut
                    })
                });
                if (res.ok) {
                    showToast('✅ Все фильтры сохранены');
                    loadData();
                } else {
                    showToast('Ошибка сохранения');
                }
            } catch {
                showToast('Нет связи с сервером');
            }
        }

        loadData();
        setInterval(loadData, 4000);
    </script>
</body>
</html>
"""


class SettingsPayload(BaseModel):
    user_id: int
    min_volume: float = DEFAULT_MIN_VOLUME
    min_density_spot: float = DEFAULT_MIN_DENSITY_SPOT
    min_density_futures: float = DEFAULT_MIN_DENSITY_FUTURES


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """Управляет запуском БД, 4 аптик-сканеров, 7 сканеров плотностей и Telegram-бота."""
    await init_db()

    connector = aiohttp.TCPConnector(limit=150)
    session = aiohttp.ClientSession(connector=connector)

    uptick_scanners: List[BaseExchangeScanner] = [
        BinanceFuturesScanner(engine, session),
        BybitFuturesScanner(engine, session),
        OKXSwapScanner(engine, session),
        AsterFuturesScanner(engine, session),
    ]

    density_scanners: List[BaseDensityScanner] = [
        BinanceDensityScanner(density_engine, session),
        BybitDensityScanner(density_engine, session),
        OKXDensityScanner(density_engine, session),
        KuCoinDensityScanner(density_engine, session),
        AsterDensityScanner(density_engine, session),
        GateDensityScanner(density_engine, session),
        BitgetDensityScanner(density_engine, session),
    ]

    logger.info("Запуск детектора аптиков (4 биржи) + сканера плотностей (7 бирж, Спот + Фьючерсы)...")
    bg_tasks: List[asyncio.Task] = [
        *[asyncio.create_task(s.run_forever()) for s in uptick_scanners],
        *[asyncio.create_task(d.run_forever()) for d in density_scanners],
    ]

    if bot:
        logger.info("Запуск Telegram-бота (polling)...")
        bg_tasks.append(asyncio.create_task(dp.start_polling(bot)))
    else:
        logger.warning("Переменная BOT_TOKEN не задана — Telegram-бот не запущен.")

    try:
        yield
    finally:
        logger.info("Остановка фоновых задач...")
        for task in bg_tasks:
            task.cancel()
        await asyncio.gather(*bg_tasks, return_exceptions=True)
        await session.close()
        if bot:
            await bot.session.close()
        await close_db()


app = FastAPI(title="Uptick & Density Detector + Telegram Mini App", lifespan=lifespan)


@app.get("/")
async def root() -> dict:
    return {
        "status": "ok",
        "service": "Uptick & Density Detector",
        "storage": "postgresql" if _use_pg else "sqlite",
        "uptick_exchanges": 4,
        "density_exchanges": 7,
    }


@app.get("/ping")
async def ping() -> dict:
    return {"status": "ok", "uptick_exchanges": 4, "density_exchanges": 7}


@app.get("/api/data")
async def api_data(user_id: int = Query(default=0)) -> JSONResponse:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    user_min_vol = DEFAULT_MIN_VOLUME
    user_min_dspot = DEFAULT_MIN_DENSITY_SPOT
    user_min_dfut = DEFAULT_MIN_DENSITY_FUTURES

    try:
        if user_id:
            row = await db_fetchone(
                "SELECT min_volume, min_density_spot, min_density_futures FROM users WHERE user_id = ?",
                (user_id,),
            )
            if row:
                user_min_vol = float(row.get("min_volume") or DEFAULT_MIN_VOLUME)
                user_min_dspot = float(row.get("min_density_spot") or DEFAULT_MIN_DENSITY_SPOT)
                user_min_dfut = float(row.get("min_density_futures") or DEFAULT_MIN_DENSITY_FUTURES)

        now_ts = time.time()
        if (
            now_ts - float(_api_cache["ts"]) > API_CACHE_TTL_SEC
            or _api_cache["utc_date"] != today_utc
        ):
            _api_cache["history"] = await db_fetchall(
                "SELECT * FROM history WHERE utc_date = ? ORDER BY id DESC LIMIT 100",
                (today_utc,),
            )
            _api_cache["densities"] = await db_fetchall(
                "SELECT * FROM densities WHERE utc_date = ? ORDER BY id DESC LIMIT 200",
                (today_utc,),
            )
            _api_cache["utc_date"] = today_utc
            _api_cache["ts"] = now_ts
    except Exception as exc:
        logger.error(f"Ошибка чтения /api/data: {exc}")

    history_rows = list(_api_cache["history"])
    density_rows = [
        d for d in _api_cache["densities"]
        if (
            (d.get("market_type") == "SPOT" and float(d.get("size_usd") or 0) >= user_min_dspot)
            or (d.get("market_type") == "FUTURES" and float(d.get("size_usd") or 0) >= user_min_dfut)
        )
    ][:120]

    active_upticks = [
        {"exchange": ex, "symbol": sym, "side": side.value}
        for ex, sym, side in engine.get_active_upticks()
    ]

    active_densities = [
        {
            "exchange": d.exchange,
            "market_type": d.market_type.value,
            "symbol": d.symbol,
            "side": d.side.value,
            "price": d.price,
            "size_usd": d.size_usd,
            "distance_pct": d.distance_pct,
        }
        for d in density_engine.get_active_densities()
        if (
            (d.market_type == MarketType.SPOT and d.size_usd >= user_min_dspot)
            or (d.market_type == MarketType.FUTURES and d.size_usd >= user_min_dfut)
        )
    ]

    return JSONResponse(
        {
            "utc_date": today_utc,
            "active": active_upticks,
            "active_densities": active_densities,
            "history": history_rows,
            "densities": density_rows,
            "user_min_volume": user_min_vol,
            "user_min_density_spot": user_min_dspot,
            "user_min_density_futures": user_min_dfut,
        }
    )


@app.post("/api/settings")
async def api_settings(payload: SettingsPayload) -> dict:
    safe_vol = max(100_000.0, float(payload.min_volume))
    safe_spot = max(250_000.0, float(payload.min_density_spot))
    safe_fut = max(1_000_000.0, float(payload.min_density_futures))

    await db_execute(
        """
        INSERT INTO users (user_id, min_volume, min_density_spot, min_density_futures)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            min_volume = excluded.min_volume,
            min_density_spot = excluded.min_density_spot,
            min_density_futures = excluded.min_density_futures
        """,
        (payload.user_id, safe_vol, safe_spot, safe_fut),
    )

    return {
        "status": "ok",
        "min_volume": safe_vol,
        "min_density_spot": safe_spot,
        "min_density_futures": safe_fut,
    }


@app.get("/webapp", response_class=HTMLResponse)
async def webapp_page() -> HTMLResponse:
    return HTMLResponse(content=WEBAPP_HTML)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
