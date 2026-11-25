"""
Мультибиржевой детектор «Аптиков» (4 биржи: Binance, Bybit, OKX, Aster)
+ проверка реальной плотности аптика в стакане + Telegram Bot + Mini App + Tiger.Trade.
Поддержка PostgreSQL (Neon/Supabase через DATABASE_URL) с автоматическим откатом на SQLite.
"""

import asyncio
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

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
    import asyncpg  # type: ignore
except ImportError:
    asyncpg = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("uptick_bot")

# ==================== КОНФИГУРАЦИЯ ====================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
SQLITE_PATH = os.getenv("SQLITE_PATH", "upticks.db")

DEFAULT_MIN_VOLUME = 500_000.0   # $500k 24ч оборот для аптиков
UPTICK_TOLERANCE_PCT = 0.0020    # 0.20% допуск до биржевой планки
UPTICK_COOLDOWN_SEC = 180        # 3 минуты между повторами аптика
MIN_UPTICK_WALL_USD = 5_000.0    # Мин. плотность в стакане на планке ($5K), чтобы отсеивать пустые/нерабочие аптики

# ==================== ЧЕРНЫЙ СПИСОК (ТОЛЬКО ТЯЖЕЛОВЕСЫ И СТЕЙБЛКОИНЫ) ====================

# Тяжелые монеты, стейблкоины и обернутые активы (акции, ETF и металлы разрешены в аптиках)
HEAVY_CRYPTO_BLACKLIST: Set[str] = {
    # Исключенные по запросу
    "U", "XUSD", "PUMP",
    # Топ-тяжеловесы и старые мажоры
    "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "TRX", "TON", "AVAX",
    "SHIB", "DOT", "LINK", "BCH", "LTC", "NEAR", "UNI", "ICP", "APT", "ETC",
    "XLM", "XMR", "HBAR", "FIL", "ATOM", "ARB", "OP", "VET", "MKR", "INJ",
    "AAVE", "GRT", "IMX", "RNDR", "RENDER", "TAO", "KAS", "PEPE", "ZEC",
    "ALGO", "CRO", "FTM", "EOS", "NEO", "DASH", "QTUM", "IOTA", "XTZ",
    "THETA", "FLOW", "EGLD", "SAND", "MANA", "AXS", "CHZ", "CRV", "LDO",
    "RUNE", "OKB", "BGB", "GT", "KCS", "LEO", "HT", "MX", "QNT", "BSV",
    "XEC", "KLAY", "KAIA", "MINA", "CFX", "KAVA", "COMP", "SNX", "ZRX",
    "BAT", "ENJ", "ZIL", "RVN", "ICX", "ONT", "WAVES", "SC", "DGB", "ZEN",
    # Обернутые / стейкинговые BTC, ETH, SOL
    "WBTC", "CBBTC", "TBTC", "LBTC", "FBTC", "SOLVBTC", "BTCB", "RENBTC",
    "WETH", "STETH", "WSTETH", "CBETH", "WBETH", "RETH", "METH", "EZETH",
    "WEETH", "EETH", "BETH", "RSETH", "PUFETH", "OSETH", "SWETH", "FRXETH",
    "BNSOL", "JITOSOL", "MSOL", "JUPSOL", "BSOL", "INF", "STSOL", "BBSOL",
    "WBNB", "SLISBNB", "BNBX",
    # Стейблкоины
    "USDT", "USDC", "FDUSD", "TUSD", "USDP", "DAI", "USDE", "USDS", "USDD",
    "PYUSD", "RLUSD", "USD1", "USDG", "USDY", "USDM", "BUSD", "FRAX", "LUSD",
    "GUSD", "SUSD", "CUSD", "CRVUSD", "GHO", "AUSD", "ZUSD", "BFUSD", "RWUSD",
    "LDUSDT", "USTC", "EURC", "EURT", "AEUR", "EURI", "EURS", "GYEN", "XSGD",
    "IDRT", "BIDR", "TRYB", "BRZ", "CADC", "NZDS", "XIDR", "USDR", "OUSG",
}


def extract_base_asset(symbol: str) -> str:
    """Извлекает базовый тикер монеты (1000PEPEUSDT -> PEPE, BTC-USDT-SWAP -> BTC)."""
    s = (symbol or "").upper().strip()
    s = s.replace("/", "").replace("_", "").replace("-", "")
    for suffix in ("USDTM", "USDTSWAP", "USDTPERP", "USDT", "USDC", "USD", "PERP", "SWAP"):
        if s.endswith(suffix) and len(s) > len(suffix):
            s = s[: -len(suffix)]
            break
    s = re.sub(r"^(1000000|100000|10000|1000|100|1M)", "", s)
    return s


def is_blacklisted_symbol(symbol: str) -> bool:
    """Исключает только тяжеловесные монеты, обернутые активы и стейблкоины (акции и металлы разрешены)."""
    base = extract_base_asset(symbol)
    if not base:
        return True
    return base in HEAVY_CRYPTO_BLACKLIST


# ==================== ТИПЫ И МОДЕЛИ ====================

class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class TickerSnapshot:
    exchange: str
    symbol: str
    last_price: float
    mark_price: float
    best_bid: float
    best_ask: float
    upper_limit: float
    lower_limit: float
    volume_24h_usd: float


@dataclass
class UptickEvent:
    exchange: str
    symbol: str
    side: Side
    price: float
    limit_price: float
    volume_24h_usd: float
    wall_usd: float
    timestamp_utc: str


# ==================== БАЗА ДАННЫХ (PostgreSQL + SQLite fallback) ====================

_pg_pool: Any = None
_use_postgres: bool = False


def _pg_sql(sql: str) -> str:
    idx = 0
    out = []
    for ch in sql:
        if ch == "?":
            idx += 1
            out.append(f"${idx}")
        else:
            out.append(ch)
    return "".join(out)


async def init_db() -> None:
    global _pg_pool, _use_postgres

    if DATABASE_URL and asyncpg is not None:
        try:
            dsn = DATABASE_URL.replace("postgres://", "postgresql://", 1)
            _pg_pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5, command_timeout=15)
            async with _pg_pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        min_volume DOUBLE PRECISION DEFAULT 500000,
                        min_density_spot DOUBLE PRECISION DEFAULT 250000,
                        min_density_futures DOUBLE PRECISION DEFAULT 1000000,
                        coin_rules TEXT DEFAULT '{}',
                        banned_coins TEXT DEFAULT '{}'
                    )
                    """
                )
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS upticks (
                        id SERIAL PRIMARY KEY,
                        utc_date TEXT,
                        utc_time TEXT,
                        exchange TEXT,
                        symbol TEXT,
                        side TEXT,
                        price DOUBLE PRECISION,
                        limit_price DOUBLE PRECISION,
                        volume_24h_usd DOUBLE PRECISION,
                        wall_usd DOUBLE PRECISION DEFAULT 0
                    )
                    """
                )
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS densities (
                        id SERIAL PRIMARY KEY,
                        utc_date TEXT,
                        utc_time TEXT,
                        exchange TEXT,
                        market_type TEXT,
                        symbol TEXT,
                        side TEXT,
                        price DOUBLE PRECISION,
                        size_usd DOUBLE PRECISION,
                        distance_pct DOUBLE PRECISION,
                        volume_24h_usd DOUBLE PRECISION
                    )
                    """
                )
                for col_def in (
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS min_density_spot DOUBLE PRECISION DEFAULT 250000",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS min_density_futures DOUBLE PRECISION DEFAULT 1000000",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS coin_rules TEXT DEFAULT '{}'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS banned_coins TEXT DEFAULT '{}'",
                    "ALTER TABLE upticks ADD COLUMN IF NOT EXISTS wall_usd DOUBLE PRECISION DEFAULT 0",
                ):
                    await conn.execute(col_def)
            _use_postgres = True
            logger.info("Подключена постоянная база данных PostgreSQL (Neon/Supabase).")
            return
        except Exception as exc:
            logger.error("Не удалось подключиться к PostgreSQL (%s), откат на SQLite.", exc)
            _use_postgres = False

    async with aiosqlite.connect(SQLITE_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                min_volume REAL DEFAULT 500000,
                min_density_spot REAL DEFAULT 250000,
                min_density_futures REAL DEFAULT 1000000,
                coin_rules TEXT DEFAULT '{}',
                banned_coins TEXT DEFAULT '{}'
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS upticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utc_date TEXT,
                utc_time TEXT,
                exchange TEXT,
                symbol TEXT,
                side TEXT,
                price REAL,
                limit_price REAL,
                volume_24h_usd REAL,
                wall_usd REAL DEFAULT 0
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS densities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utc_date TEXT,
                utc_time TEXT,
                exchange TEXT,
                market_type TEXT,
                symbol TEXT,
                side TEXT,
                price REAL,
                size_usd REAL,
                distance_pct REAL,
                volume_24h_usd REAL
            )
            """
        )
        cur = await db.execute("PRAGMA table_info(users)")
        cols = {row[1] for row in await cur.fetchall()}
        if "min_density_spot" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN min_density_spot REAL DEFAULT 250000")
        if "min_density_futures" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN min_density_futures REAL DEFAULT 1000000")
        if "coin_rules" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN coin_rules TEXT DEFAULT '{}'")
        if "banned_coins" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN banned_coins TEXT DEFAULT '{}'")
        cur_up = await db.execute("PRAGMA table_info(upticks)")
        up_cols = {row[1] for row in await cur_up.fetchall()}
        if "wall_usd" not in up_cols:
            await db.execute("ALTER TABLE upticks ADD COLUMN wall_usd REAL DEFAULT 0")
        await db.commit()
    logger.info("Используется локальная база данных SQLite (%s).", SQLITE_PATH)


async def db_execute(sql: str, params: Tuple[Any, ...] = ()) -> None:
    if _use_postgres and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            await conn.execute(_pg_sql(sql), *params)
    else:
        async with aiosqlite.connect(SQLITE_PATH) as db:
            await db.execute(sql, params)
            await db.commit()


async def db_fetchone(sql: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    if _use_postgres and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            row = await conn.fetchrow(_pg_sql(sql), *params)
            return dict(row) if row else None
    else:
        async with aiosqlite.connect(SQLITE_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(sql, params)
            row = await cur.fetchone()
            return dict(row) if row else None


async def db_fetchall(sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    if _use_postgres and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            rows = await conn.fetch(_pg_sql(sql), *params)
            return [dict(r) for r in rows]
    else:
        async with aiosqlite.connect(SQLITE_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(sql, params)
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


# ==================== ПОЛЬЗОВАТЕЛЬСКИЕ БАНЫ ====================

def normalize_ban_scope(scope: Optional[str]) -> str:
    s = (scope or "ALL").strip().upper()
    if s in ("UPTICK", "UPTICKS", "U", "АПТИК", "АПТИКИ"):
        return "UPTICK"
    if s in ("DENSITY", "DENSITIES", "D", "ПЛОТНОСТЬ", "ПЛОТНОСТИ"):
        return "DENSITY"
    return "ALL"


def parse_banned_coins(raw: Optional[str]) -> Dict[str, str]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        result: Dict[str, str] = {}
        if isinstance(data, list):
            for item in data:
                base = extract_base_asset(str(item))
                if base:
                    result[base] = "ALL"
            return result
        if isinstance(data, dict):
            for k, v in data.items():
                base = extract_base_asset(str(k))
                if base:
                    result[base] = normalize_ban_scope(str(v))
            return result
        return {}
    except Exception:
        return {}


def is_coin_banned(base: str, banned_map: Dict[str, str], target: str) -> bool:
    scope = banned_map.get(base)
    return scope in ("ALL", target)


# ==================== ДВИЖОК АПТИКОВ (4 БИРЖИ) ====================

class UptickEngine:
    def __init__(self, on_uptick: Callable[[UptickEvent], Any]) -> None:
        self.on_uptick = on_uptick
        # (exchange, symbol) -> (Side, ts, volume_24h_usd, wall_usd)
        self._active: Dict[Tuple[str, str], Tuple[Side, float, float, float]] = {}
        self._cooldown: Dict[Tuple[str, str, Side], float] = {}

    async def evaluate(
        self,
        snap: TickerSnapshot,
        wall_fetcher: Optional[Callable[[str, Side, float], Any]] = None,
    ) -> None:
        if is_blacklisted_symbol(snap.symbol):
            return
        if snap.last_price <= 0 or snap.upper_limit <= 0 or snap.lower_limit <= 0:
            return
        if snap.upper_limit <= snap.lower_limit:
            return
        if snap.mark_price > 0 and abs(snap.last_price - snap.mark_price) / snap.mark_price > 0.5:
            return

        now = time.time()
        key = (snap.exchange, snap.symbol)

        upper_trigger = snap.upper_limit * (1.0 - UPTICK_TOLERANCE_PCT)
        lower_trigger = snap.lower_limit * (1.0 + UPTICK_TOLERANCE_PCT)

        hit_long = (
            snap.last_price >= upper_trigger
            or (snap.best_bid > 0 and snap.best_bid >= upper_trigger)
        )
        hit_short = (
            snap.last_price <= lower_trigger
            or (snap.best_ask > 0 and snap.best_ask <= lower_trigger)
        )

        if hit_long and not hit_short:
            side = Side.LONG
            limit_price = snap.upper_limit
        elif hit_short and not hit_long:
            side = Side.SHORT
            limit_price = snap.lower_limit
        else:
            self._active.pop(key, None)
            return

        # Проверяем реальный стакан: подставляет ли аптик рабочую плотность на планке
        wall_usd = 0.0
        if wall_fetcher is not None:
            wall_usd = float(await wall_fetcher(snap.symbol, side, limit_price) or 0.0)
            if wall_usd < MIN_UPTICK_WALL_USD:
                self._active.pop(key, None)
                return

        self._active[key] = (side, now, snap.volume_24h_usd, wall_usd)
        cd_key = (snap.exchange, snap.symbol, side)
        if now - self._cooldown.get(cd_key, 0.0) >= UPTICK_COOLDOWN_SEC:
            self._cooldown[cd_key] = now
            ev = UptickEvent(
                exchange=snap.exchange,
                symbol=snap.symbol,
                side=side,
                price=snap.last_price,
                limit_price=limit_price,
                volume_24h_usd=snap.volume_24h_usd,
                wall_usd=wall_usd,
                timestamp_utc=datetime.now(timezone.utc).strftime("%H:%M:%S"),
            )
            await self.on_uptick(ev)

    def get_active_upticks(self) -> List[Tuple[str, str, Side, float, float]]:
        now = time.time()
        expired = [k for k, (_, ts, _, _) in self._active.items() if now - ts > 30]
        for k in expired:
            self._active.pop(k, None)
        return [
            (ex, sym, side, vol, wall)
            for (ex, sym), (side, _, vol, wall) in self._active.items()
        ]


# ==================== БАЗОВЫЙ HTTP КЛИЕНТ СКАНЕРОВ ====================

class BaseScanner:
    name: str = "Base"
    poll_interval: float = 2.5

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    async def _get_json(self, url: str, timeout: float = 8.0) -> Any:
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
        except Exception:
            return None

    @staticmethod
    def _calc_uptick_wall_usd(
        bids: List[Any],
        asks: List[Any],
        side: Side,
        limit_price: float,
        ct_val: float = 1.0,
    ) -> float:
        """
        Подтверждает, что аптик реально работает в стакане, и считает размер плотности ($) на планке.
        Для LONG считаем плотность на BID у верхней планки (или сдерживающую ASK, если BID уже съели).
        Для SHORT считаем плотность на ASK у нижней планки (или BID, если ASK уже съели).
        """
        if limit_price <= 0:
            return 0.0

        def _sum_near(levels: List[Any], max_dist: float) -> Tuple[float, float]:
            top_px = 0.0
            total = 0.0
            for idx, lvl in enumerate(levels):
                try:
                    px = float(lvl[0])
                    qty = float(lvl[1])
                except (TypeError, ValueError, IndexError):
                    continue
                if px <= 0 or qty <= 0:
                    continue
                if idx == 0:
                    top_px = px
                if abs(px - limit_price) / limit_price <= max_dist:
                    total += px * qty * ct_val
            return top_px, total

        best_bid, bid_wall = _sum_near(bids, 0.0025)
        best_ask, ask_wall = _sum_near(asks, 0.0025)

        # Проверяем, что стакан реально стоит у планки (не старая цена сделки на пустой монете)
        bid_near = best_bid > 0 and abs(best_bid - limit_price) / limit_price <= 0.0045
        ask_near = best_ask > 0 and abs(best_ask - limit_price) / limit_price <= 0.0045
        if not (bid_near or ask_near):
            return 0.0

        if side == Side.LONG:
            return bid_wall if bid_wall >= MIN_UPTICK_WALL_USD else max(bid_wall, ask_wall)
        return ask_wall if ask_wall >= MIN_UPTICK_WALL_USD else max(ask_wall, bid_wall)

    async def scan_once(self) -> None:
        raise NotImplementedError

    async def run(self) -> None:
        while True:
            try:
                await self.scan_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("[%s] ошибка цикла: %s", self.name, exc)
            await asyncio.sleep(self.poll_interval)


# ==================== СКАНЕРЫ АПТИКОВ (4 БИРЖИ: Binance, Bybit, OKX, Aster) ====================

class BinanceUptickScanner(BaseScanner):
    name = "Binance"
    poll_interval = 2.0

    def __init__(self, session: aiohttp.ClientSession, engine: UptickEngine) -> None:
        super().__init__(session)
        self.engine = engine
        self._percent_limits: Dict[str, Tuple[float, float]] = {}
        self._last_info_refresh = 0.0

    async def fetch_wall_usd(self, symbol: str, side: Side, limit_price: float) -> float:
        book = await self._get_json(f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=20", timeout=5.0)
        if not isinstance(book, dict):
            return 0.0
        return self._calc_uptick_wall_usd(book.get("bids") or [], book.get("asks") or [], side, limit_price)

    async def _refresh_exchange_info(self) -> None:
        if time.time() - self._last_info_refresh < 1800 and self._percent_limits:
            return
        info = await self._get_json("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=12.0)
        if not isinstance(info, dict):
            return
        limits: Dict[str, Tuple[float, float]] = {}
        for s in info.get("symbols", []):
            sym = s.get("symbol", "")
            if not sym.endswith("USDT") or s.get("status") != "TRADING":
                continue
            up, down = 1.05, 0.95
            for f in s.get("filters", []):
                if f.get("filterType") == "PERCENT_PRICE":
                    try:
                        up = float(f.get("multiplierUp") or 1.05)
                        down = float(f.get("multiplierDown") or 0.95)
                    except (TypeError, ValueError):
                        pass
            limits[sym] = (up, down)
        if limits:
            self._percent_limits = limits
            self._last_info_refresh = time.time()

    async def scan_once(self) -> None:
        await self._refresh_exchange_info()
        tickers, marks, books = await asyncio.gather(
            self._get_json("https://fapi.binance.com/fapi/v1/ticker/24hr"),
            self._get_json("https://fapi.binance.com/fapi/v1/premiumIndex"),
            self._get_json("https://fapi.binance.com/fapi/v1/ticker/bookTicker"),
        )
        if not isinstance(tickers, list) or not isinstance(marks, list):
            return
        mark_map = {m.get("symbol"): float(m.get("markPrice") or 0) for m in marks if isinstance(m, dict)}
        book_map = {}
        if isinstance(books, list):
            for b in books:
                book_map[b.get("symbol")] = (
                    float(b.get("bidPrice") or 0),
                    float(b.get("askPrice") or 0),
                )

        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or is_blacklisted_symbol(sym):
                continue
            mark = mark_map.get(sym, 0.0)
            if mark <= 0:
                continue
            try:
                last = float(t.get("lastPrice") or 0)
                vol = float(t.get("quoteVolume") or 0)
            except (TypeError, ValueError):
                continue
            up_mult, down_mult = self._percent_limits.get(sym, (1.05, 0.95))
            bid, ask = book_map.get(sym, (0.0, 0.0))
            await self.engine.evaluate(
                TickerSnapshot(
                    exchange=self.name,
                    symbol=sym,
                    last_price=last,
                    mark_price=mark,
                    best_bid=bid,
                    best_ask=ask,
                    upper_limit=mark * up_mult,
                    lower_limit=mark * down_mult,
                    volume_24h_usd=vol,
                ),
                wall_fetcher=self.fetch_wall_usd,
            )


class BybitUptickScanner(BaseScanner):
    name = "Bybit"
    poll_interval = 2.5

    def __init__(self, session: aiohttp.ClientSession, engine: UptickEngine) -> None:
        super().__init__(session)
        self.engine = engine

    async def fetch_wall_usd(self, symbol: str, side: Side, limit_price: float) -> float:
        book = await self._get_json(
            f"https://api.bybit.com/v5/market/orderbook?category=linear&symbol={symbol}&limit=25",
            timeout=5.0,
        )
        if not isinstance(book, dict):
            return 0.0
        res = book.get("result") or {}
        return self._calc_uptick_wall_usd(res.get("b") or [], res.get("a") or [], side, limit_price)

    async def scan_once(self) -> None:
        data = await self._get_json("https://api.bybit.com/v5/market/tickers?category=linear")
        if not isinstance(data, dict):
            return
        candidates = []
        for t in data.get("result", {}).get("list", []):
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or is_blacklisted_symbol(sym):
                continue
            try:
                last = float(t.get("lastPrice") or 0)
                mark = float(t.get("markPrice") or 0)
                bid = float(t.get("bid1Price") or 0)
                ask = float(t.get("ask1Price") or 0)
                vol = float(t.get("turnover24h") or 0)
            except (TypeError, ValueError):
                continue
            if last <= 0 or mark <= 0:
                continue
            dev = abs(last - mark) / mark
            if dev >= 0.004:
                candidates.append((dev, sym, last, mark, bid, ask, vol))

        candidates.sort(key=lambda x: x[0], reverse=True)
        sem = asyncio.Semaphore(8)

        async def check_one(item: Tuple[float, str, float, float, float, float, float]) -> None:
            _, sym, last, mark, bid, ask, vol = item
            async with sem:
                pl = await self._get_json(
                    f"https://api.bybit.com/v5/market/price-limit?category=linear&symbol={sym}"
                )
            if not isinstance(pl, dict):
                return
            res = pl.get("result") or {}
            try:
                buy_lmt = float(res.get("buyLmt") or 0)
                sell_lmt = float(res.get("sellLmt") or 0)
            except (TypeError, ValueError):
                return
            if buy_lmt > 0 and sell_lmt > 0:
                await self.engine.evaluate(
                    TickerSnapshot(
                        exchange=self.name,
                        symbol=sym,
                        last_price=last,
                        mark_price=mark,
                        best_bid=bid,
                        best_ask=ask,
                        upper_limit=buy_lmt,
                        lower_limit=sell_lmt,
                        volume_24h_usd=vol,
                    ),
                    wall_fetcher=self.fetch_wall_usd,
                )

        if candidates:
            await asyncio.gather(*(check_one(c) for c in candidates[:40]))


class OKXUptickScanner(BaseScanner):
    name = "OKX"
    poll_interval = 2.5

    def __init__(self, session: aiohttp.ClientSession, engine: UptickEngine) -> None:
        super().__init__(session)
        self.engine = engine
        self._ct_vals: Dict[str, float] = {}
        self._inst_ids: Dict[str, str] = {}
        self._last_inst_refresh = 0.0

    async def _refresh_instruments(self) -> None:
        if time.time() - self._last_inst_refresh < 1800 and self._ct_vals:
            return
        data = await self._get_json("https://www.okx.com/api/v5/public/instruments?instType=SWAP", timeout=10.0)
        if not isinstance(data, dict):
            return
        vals: Dict[str, float] = {}
        ids: Dict[str, str] = {}
        for item in data.get("data", []):
            inst_id = item.get("instId", "")
            if not inst_id.endswith("-USDT-SWAP"):
                continue
            sym = inst_id.replace("-USDT-SWAP", "USDT")
            try:
                vals[sym] = float(item.get("ctVal") or 1.0)
                ids[sym] = inst_id
            except (TypeError, ValueError):
                continue
        if vals:
            self._ct_vals = vals
            self._inst_ids = ids
            self._last_inst_refresh = time.time()

    async def fetch_wall_usd(self, symbol: str, side: Side, limit_price: float) -> float:
        inst_id = self._inst_ids.get(symbol)
        if not inst_id:
            inst_id = f"{symbol[:-4]}-USDT-SWAP" if symbol.endswith("USDT") else symbol
        book = await self._get_json(f"https://www.okx.com/api/v5/market/books?instId={inst_id}&sz=20", timeout=5.0)
        if not isinstance(book, dict):
            return 0.0
        arr = book.get("data") or []
        if not arr:
            return 0.0
        ct_val = self._ct_vals.get(symbol, 1.0)
        return self._calc_uptick_wall_usd(
            arr[0].get("bids") or [],
            arr[0].get("asks") or [],
            side,
            limit_price,
            ct_val=ct_val,
        )

    async def scan_once(self) -> None:
        await self._refresh_instruments()
        tickers_raw, marks_raw = await asyncio.gather(
            self._get_json("https://www.okx.com/api/v5/market/tickers?instType=SWAP"),
            self._get_json("https://www.okx.com/api/v5/public/mark-price?instType=SWAP"),
        )
        if not isinstance(tickers_raw, dict):
            return
        mark_map: Dict[str, float] = {}
        if isinstance(marks_raw, dict):
            for m in marks_raw.get("data", []):
                mark_map[m.get("instId", "")] = float(m.get("markPx") or 0)

        candidates = []
        for t in tickers_raw.get("data", []):
            inst_id = t.get("instId", "")
            if not inst_id.endswith("-USDT-SWAP"):
                continue
            sym = inst_id.replace("-USDT-SWAP", "USDT")
            if is_blacklisted_symbol(sym):
                continue
            try:
                last = float(t.get("last") or 0)
                bid = float(t.get("bidPx") or 0)
                ask = float(t.get("askPx") or 0)
                mark = mark_map.get(inst_id, last)
                vol = float(t.get("volCcy24h") or 0) * last
            except (TypeError, ValueError):
                continue
            if last > 0 and mark > 0:
                dev = abs(last - mark) / mark
                if dev >= 0.004:
                    candidates.append((dev, inst_id, sym, last, mark, bid, ask, vol))

        candidates.sort(key=lambda x: x[0], reverse=True)
        sem = asyncio.Semaphore(8)

        async def check_one(item: Tuple[float, str, str, float, float, float, float, float]) -> None:
            _, inst_id, sym, last, mark, bid, ask, vol = item
            async with sem:
                pl = await self._get_json(f"https://www.okx.com/api/v5/public/price-limit?instId={inst_id}")
            if not isinstance(pl, dict):
                return
            arr = pl.get("data") or []
            if not arr:
                return
            try:
                buy_lmt = float(arr[0].get("buyLmt") or 0)
                sell_lmt = float(arr[0].get("sellLmt") or 0)
            except (TypeError, ValueError):
                return
            if buy_lmt > 0 and sell_lmt > 0:
                await self.engine.evaluate(
                    TickerSnapshot(
                        exchange=self.name,
                        symbol=sym,
                        last_price=last,
                        mark_price=mark,
                        best_bid=bid,
                        best_ask=ask,
                        upper_limit=buy_lmt,
                        lower_limit=sell_lmt,
                        volume_24h_usd=vol,
                    ),
                    wall_fetcher=self.fetch_wall_usd,
                )

        if candidates:
            await asyncio.gather(*(check_one(c) for c in candidates[:40]))


class AsterUptickScanner(BaseScanner):
    name = "Aster"
    poll_interval = 3.0

    def __init__(self, session: aiohttp.ClientSession, engine: UptickEngine) -> None:
        super().__init__(session)
        self.engine = engine
        self._percent_limits: Dict[str, Tuple[float, float]] = {}
        self._last_info_refresh = 0.0

    async def fetch_wall_usd(self, symbol: str, side: Side, limit_price: float) -> float:
        book = await self._get_json(f"https://fapi.asterdex.com/fapi/v3/depth?symbol={symbol}&limit=20", timeout=5.0)
        if not isinstance(book, dict):
            return 0.0
        return self._calc_uptick_wall_usd(book.get("bids") or [], book.get("asks") or [], side, limit_price)

    async def _refresh_info(self) -> None:
        if time.time() - self._last_info_refresh < 1800 and self._percent_limits:
            return
        info = await self._get_json("https://fapi.asterdex.com/fapi/v3/exchangeInfo", timeout=10.0)
        if not isinstance(info, dict):
            return
        limits: Dict[str, Tuple[float, float]] = {}
        for s in info.get("symbols", []):
            sym = s.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            up, down = 1.05, 0.95
            for f in s.get("filters", []):
                if f.get("filterType") == "PERCENT_PRICE":
                    try:
                        up = float(f.get("multiplierUp") or 1.05)
                        down = float(f.get("multiplierDown") or 0.95)
                    except (TypeError, ValueError):
                        pass
            limits[sym] = (up, down)
        if limits:
            self._percent_limits = limits
            self._last_info_refresh = time.time()

    async def scan_once(self) -> None:
        await self._refresh_info()
        tickers, marks = await asyncio.gather(
            self._get_json("https://fapi.asterdex.com/fapi/v3/ticker/24hr"),
            self._get_json("https://fapi.asterdex.com/fapi/v3/premiumIndex"),
        )
        if not isinstance(tickers, list) or not isinstance(marks, list):
            return
        mark_map = {m.get("symbol"): float(m.get("markPrice") or 0) for m in marks if isinstance(m, dict)}
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or is_blacklisted_symbol(sym):
                continue
            mark = mark_map.get(sym, 0.0)
            if mark <= 0:
                continue
            try:
                last = float(t.get("lastPrice") or 0)
                vol = float(t.get("quoteVolume") or 0)
            except (TypeError, ValueError):
                continue
            up_mult, down_mult = self._percent_limits.get(sym, (1.05, 0.95))
            await self.engine.evaluate(
                TickerSnapshot(
                    exchange=self.name,
                    symbol=sym,
                    last_price=last,
                    mark_price=mark,
                    best_bid=0.0,
                    best_ask=0.0,
                    upper_limit=mark * up_mult,
                    lower_limit=mark * down_mult,
                    volume_24h_usd=vol,
                ),
                wall_fetcher=self.fetch_wall_usd,
            )


# ==================== TELEGRAM БОТ И РАССЫЛКА ====================

bot: Optional[Bot] = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()


def _build_main_keyboard() -> Optional[InlineKeyboardMarkup]:
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Открыть Аптик-скринер", web_app=WebAppInfo(url=WEBAPP_URL))]
        ]
    )


async def _broadcast_messages(user_ids: List[int], text: str, kb: Optional[InlineKeyboardMarkup]) -> None:
    if not bot or not user_ids:
        return
    for uid in user_ids:
        try:
            await bot.send_message(uid, text, parse_mode="HTML", reply_markup=kb)
            await asyncio.sleep(0.04)
        except Exception:
            continue


async def handle_uptick_event(event: UptickEvent) -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await db_execute(
        "INSERT INTO upticks (utc_date, utc_time, exchange, symbol, side, price, limit_price, volume_24h_usd, wall_usd) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            today_utc,
            event.timestamp_utc,
            event.exchange,
            event.symbol,
            event.side.value,
            event.price,
            event.limit_price,
            event.volume_24h_usd,
            event.wall_usd,
        ),
    )

    emoji = "🟢" if event.side == Side.LONG else "🔴"
    wall_line = f"🧱 Плотность аптика: <b>${event.wall_usd:,.0f}</b>\n" if event.wall_usd > 0 else ""
    text = (
        f"⚡ <b>АПТИК | {event.exchange}</b>\n"
        f"Монета: <code>{event.symbol}</code>\n"
        f"{emoji} Сторона: <b>{event.side.value}</b>\n"
        f"{wall_line}"
        f"Цена: <code>{event.price:g}</code> (планка: <code>{event.limit_price:g}</code>)\n"
        f"Оборот 24ч: <b>${event.volume_24h_usd:,.0f}</b>\n"
        f"Время (UTC): {event.timestamp_utc}"
    )

    users = await db_fetchall("SELECT user_id, min_volume, banned_coins FROM users")
    base = extract_base_asset(event.symbol)
    target_ids: List[int] = []
    for u in users:
        banned_map = parse_banned_coins(u.get("banned_coins"))
        if is_coin_banned(base, banned_map, "UPTICK"):
            continue
        if event.volume_24h_usd >= float(u.get("min_volume") or DEFAULT_MIN_VOLUME):
            target_ids.append(int(u["user_id"]))

    await _broadcast_messages(target_ids, text, _build_main_keyboard())


engine = UptickEngine(on_uptick=handle_uptick_event)


# ==================== КОМАНДЫ TELEGRAM БОТА ====================

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    await db_execute(
        "INSERT INTO users (user_id) VALUES (?) ON CONFLICT(user_id) DO NOTHING",
        (message.from_user.id,),
    )
    warn = "" if WEBAPP_URL else "\n\n⚠️ Переменная <code>WEBAPP_URL</code> еще не задана."
    text = (
        "👋 <b>Аптик-скринер запущен!</b>\n\n"
        "⚡ <b>Биржи (4):</b> Binance, Bybit, OKX, Aster.\n"
        "🐯 <b>Tiger.Trade:</b> открытие по клику или автооткрытие в Mini App.\n"
        "📈 <b>Включены:</b> альткоины, акции и металлы.\n"
        "🚫 <b>Исключены:</b> тяжеловесы (BTC, ETH, SOL, PEPE...) и стейблкоины.\n\n"
        "<b>Команды:</b>\n"
        "• <code>/vol 500000</code> — мин. оборот 24ч ($) для аптиков\n"
        "• <code>/ban MOODENG</code> — убрать монету\n"
        "• <code>/unban MOODENG</code> — вернуть монету\n"
        "• <code>/status</code> — активные аптики сейчас"
        f"{warn}"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=_build_main_keyboard())


@dp.message(Command("vol"))
async def cmd_vol(message: types.Message) -> None:
    try:
        val = max(0.0, float((message.text or "").split()[1].replace(",", "").replace("$", "")))
        await db_execute(
            "INSERT INTO users (user_id, min_volume) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET min_volume = excluded.min_volume",
            (message.from_user.id, val),
        )
        await message.answer(f"✅ Мин. оборот 24ч для аптиков: <b>${val:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/vol 500000</code>", parse_mode="HTML")


@dp.message(Command("ban"))
async def cmd_ban(message: types.Message) -> None:
    try:
        parts = (message.text or "").split()
        base = extract_base_asset(parts[1])
        if not base:
            raise ValueError()
        row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (message.from_user.id,))
        banned = parse_banned_coins(row.get("banned_coins") if row else None)
        banned[base] = "ALL"
        await db_execute(
            "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
            (message.from_user.id, json.dumps(banned)),
        )
        await message.answer(f"🚫 Монета <b>{base}</b> скрыта.", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/ban MOODENG</code>", parse_mode="HTML")


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message) -> None:
    try:
        base = extract_base_asset((message.text or "").split()[1])
        row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (message.from_user.id,))
        banned = parse_banned_coins(row.get("banned_coins") if row else None)
        if base in banned:
            banned.pop(base, None)
            await db_execute("UPDATE users SET banned_coins = ? WHERE user_id = ?", (json.dumps(banned), message.from_user.id))
            await message.answer(f"✅ Монета <b>{base}</b> возвращена в алерты.", parse_mode="HTML")
        else:
            await message.answer(f"Монета <b>{base}</b> не была скрыта.", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/unban MOODENG</code>", parse_mode="HTML")


@dp.message(Command("status"))
async def cmd_status(message: types.Message) -> None:
    upticks = engine.get_active_upticks()
    if upticks:
        lines = ["⚡ <b>Активные аптики сейчас:</b>\n"]
        for ex, sym, side, _, wall in upticks[:15]:
            wall_txt = f" · 🧱 <b>${wall:,.0f}</b>" if wall > 0 else ""
            lines.append(f"• {ex} | <code>{sym}</code> — <b>{side.value}</b>{wall_txt}")
        text = "\n".join(lines)
    else:
        text = "⚡ Активных аптиков сейчас нет."
    await message.answer(text, parse_mode="HTML", reply_markup=_build_main_keyboard())


# ==================== TELEGRAM MINI APP (HTML + JS + TIGER.TRADE) ====================

WEBAPP_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Аптик-скринер</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        :root {
            --bg: #0b0e14;
            --card: #141923;
            --border: #232b3a;
            --text: #e6edf3;
            --hint: #8b949e;
            --accent: #3b82f6;
            --long: #16c784;
            --short: #ea3943;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        body { background: var(--bg); color: var(--text); padding: 12px 12px 28px; -webkit-tap-highlight-color: transparent; }

        .topbar {
            display: flex; align-items: center; justify-content: space-between;
            gap: 8px; margin-bottom: 12px;
        }
        .brand { display: flex; align-items: center; gap: 8px; font-weight: 800; font-size: 15px; }
        .live-dot {
            width: 9px; height: 9px; border-radius: 50%; background: var(--long);
            box-shadow: 0 0 8px var(--long); animation: pulse 1.6s infinite;
        }
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.35; } }

        .top-actions { display: flex; align-items: center; gap: 6px; }
        .tiger-pill {
            display: inline-flex; align-items: center; gap: 5px;
            font-size: 11px; font-weight: 700; padding: 5px 9px; border-radius: 999px;
            border: 1px solid var(--border); background: var(--card); color: var(--hint);
            cursor: pointer;
        }
        .tiger-pill.online { border-color: rgba(22,199,132,0.5); color: var(--long); background: rgba(22,199,132,0.12); }
        .tiger-pill.muted { border-color: rgba(234,57,67,0.5); color: var(--short); background: rgba(234,57,67,0.12); }

        .tabs {
            display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px;
            margin-bottom: 12px; background: var(--card); padding: 4px;
            border-radius: 12px; border: 1px solid var(--border);
        }
        .tab-btn {
            border: none; background: transparent; color: var(--hint);
            padding: 9px 6px; border-radius: 9px; font-size: 13px; font-weight: 700;
            cursor: pointer; transition: 0.15s;
        }
        .tab-btn.active { background: var(--accent); color: #fff; }

        .panel { display: none; }
        .panel.active { display: block; }

        .section-title {
            font-size: 12px; font-weight: 800; color: var(--hint);
            margin: 10px 0 8px; text-transform: uppercase; letter-spacing: 0.4px;
            display: flex; justify-content: space-between; align-items: center;
        }

        .live-box {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 12px; padding: 10px; margin-bottom: 12px;
        }

        .filter-row {
            display: flex; gap: 6px; overflow-x: auto; padding-bottom: 6px; margin-bottom: 10px;
            scrollbar-width: none;
        }
        .filter-row::-webkit-scrollbar { display: none; }
        .chip {
            flex: 0 0 auto; display: inline-flex; align-items: center; gap: 5px;
            padding: 6px 10px; border-radius: 999px; font-size: 12px; font-weight: 700;
            background: var(--card); color: var(--hint); border: 1px solid var(--border);
            cursor: pointer;
        }
        .chip.active { background: rgba(59,130,246,0.2); border-color: var(--accent); color: #fff; }

        .card {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 12px; padding: 10px 12px; margin-bottom: 8px;
            cursor: pointer; transition: transform 0.1s, border-color 0.15s;
        }
        .card:active { transform: scale(0.99); }
        .card:hover { border-color: var(--accent); }
        .card-top { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 6px; }
        .card-left { display: flex; align-items: center; gap: 8px; min-width: 0; }
        .ex-logo {
            width: 24px; height: 24px; border-radius: 6px; flex-shrink: 0;
            display: inline-flex; align-items: center; justify-content: center;
            overflow: hidden; background: #101620;
        }
        .ex-logo svg { width: 22px; height: 22px; display: block; }
        .sym { font-weight: 800; font-size: 15px; letter-spacing: 0.2px; }
        .mkt-tag {
            font-size: 10px; font-weight: 800; padding: 2px 6px; border-radius: 6px;
            background: rgba(59,130,246,0.18); color: #7cb3ff;
        }

        .card-right { display: flex; align-items: center; gap: 6px; }
        .side-badge { font-size: 11px; font-weight: 800; padding: 3px 8px; border-radius: 7px; }
        .side-badge.LONG { background: rgba(22,199,132,0.18); color: var(--long); }
        .side-badge.SHORT { background: rgba(234,57,67,0.18); color: var(--short); }
        .ban-x {
            border: 1px solid rgba(234,57,67,0.35); background: rgba(234,57,67,0.12);
            color: var(--short); width: 24px; height: 24px; border-radius: 7px;
            font-size: 12px; font-weight: 800; cursor: pointer;
            display: inline-flex; align-items: center; justify-content: center;
        }

        .card-meta {
            display: flex; flex-wrap: wrap; justify-content: space-between;
            gap: 6px; font-size: 12px; color: var(--hint);
        }
        .card-meta b { color: var(--text); }
        .empty { text-align: center; padding: 16px 10px; color: var(--hint); font-size: 13px; }

        .rule-item {
            display: flex; align-items: center; justify-content: space-between;
            background: var(--bg); border: 1px solid var(--border);
            border-radius: 9px; padding: 8px 10px; margin-top: 6px; font-size: 12px;
        }

        .setting-box {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 12px; padding: 12px; margin-bottom: 10px;
        }
        .setting-label { display: block; font-size: 13px; font-weight: 800; margin-bottom: 6px; }
        .meta { font-size: 12px; color: var(--hint); line-height: 1.4; }
        .input-row { display: flex; gap: 6px; margin-bottom: 8px; }
        input[type="text"], input[type="number"], select {
            width: 100%; padding: 9px 10px; border-radius: 9px;
            border: 1px solid var(--border); background: var(--bg); color: var(--text);
            font-size: 13px; outline: none;
        }
        .btn {
            padding: 9px 12px; border-radius: 9px; border: none; font-size: 13px;
            font-weight: 700; cursor: pointer; background: var(--accent); color: #fff;
            white-space: nowrap;
        }
        .btn.secondary { background: rgba(255,255,255,0.08); color: var(--text); }
        .danger-btn {
            padding: 9px 12px; border-radius: 9px; border: 1px solid rgba(234,57,67,0.45);
            background: rgba(234,57,67,0.16); color: var(--short); font-size: 13px;
            font-weight: 700; cursor: pointer; white-space: nowrap;
        }

        .toggle-row {
            display: flex; align-items: center; justify-content: space-between;
            padding: 7px 0; border-bottom: 1px solid rgba(255,255,255,0.05);
            font-size: 13px;
        }
        .toggle-row:last-child { border-bottom: none; }
        input[type="checkbox"] { width: 18px; height: 18px; accent-color: var(--accent); cursor: pointer; }

        .banned-tags { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
        .banned-chip {
            display: inline-flex; align-items: center; gap: 5px;
            background: var(--bg); border: 1px solid var(--border);
            padding: 5px 8px; border-radius: 8px; font-size: 12px; font-weight: 700;
        }
        .banned-chip button {
            border: none; background: transparent; color: var(--short);
            font-weight: 800; cursor: pointer; padding: 0; font-size: 13px;
        }

        .toast {
            position: fixed; bottom: 18px; left: 50%; transform: translateX(-50%);
            background: #1f2937; color: #fff; border: 1px solid var(--border);
            padding: 9px 14px; border-radius: 10px; font-size: 12px; font-weight: 700;
            opacity: 0; pointer-events: none; transition: opacity 0.2s; z-index: 200;
        }
        .toast.show { opacity: 1; }
    </style>
</head>
<body>
    <div class="topbar">
        <div class="brand">
            <span class="live-dot"></span>
            <span>⚡ Аптик-скринер</span>
        </div>
        <div class="top-actions">
            <button class="tiger-pill online" id="mute-pill" onclick="toggleMute()">🔊 Звук</button>
            <button class="tiger-pill" id="tiger-pill" onclick="connectTiger(true)">🐯 Tiger: OFF</button>
        </div>
    </div>

    <div class="tabs">
        <button class="tab-btn active" data-tab="upticks" onclick="switchTab('upticks')">⚡ Аптики</button>
        <button class="tab-btn" data-tab="settings" onclick="switchTab('settings')">⚙️ Настройки</button>
    </div>

    <!-- ВКЛАДКА АПТИКИ -->
    <div id="tab-upticks" class="panel active">
        <div class="section-title">
            <span>🔴 Активные аптики сейчас</span>
            <span id="live-up-cnt">0</span>
        </div>
        <div class="live-box" id="live-active-upticks"></div>

        <div class="section-title"><span>📜 История аптиков за сегодня (UTC)</span></div>
        <div class="filter-row" id="uptick-ex-filters"></div>
        <div id="upticks-list"></div>
    </div>

    <!-- ВКЛАДКА НАСТРОЙКИ -->
    <div id="tab-settings" class="panel">
        <div class="setting-box">
            <label class="setting-label">💰 Фильтр по обороту 24ч ($)</label>
            <div class="meta" style="margin-bottom:6px;">Мин. суточный оборот монеты для уведомлений об аптиках ($)</div>
            <div class="input-row">
                <input type="number" id="set-vol" value="500000">
                <button class="btn" onclick="saveGlobalSettings()">Сохранить</button>
            </div>
        </div>

        <div class="setting-box">
            <label class="setting-label">🚫 Убрать монеты (черный список)</label>
            <div class="meta" style="margin-bottom:8px;">Скрытые монеты не приходят в Telegram и не показываются в списке (или нажмите ✕ на карточке).</div>
            <div class="input-row">
                <input type="text" id="ban-coin-input" placeholder="Тикер (напр. MOODENG, TRUMP)">
                <button class="danger-btn" onclick="banCoinFromInput()">Убрать</button>
            </div>
            <div class="banned-tags" id="banned-coins-list"></div>
        </div>

        <div class="setting-box">
            <label class="setting-label">🔊 Звук и голосовая озвучка (LONG / SHORT)</label>
            <div class="toggle-row">
                <span>Убрать весь звук (без звука)</span>
                <input type="checkbox" id="snd-mute-all" onchange="setMuted(this.checked)">
            </div>
            <div class="toggle-row">
                <span>Озвучивать новые Аптики</span>
                <input type="checkbox" id="snd-uptick" checked onchange="saveLocalPrefs()">
            </div>
            <div class="input-row" style="margin-top:8px;">
                <button class="btn secondary" style="width:100%" onclick="testVoice()">▶ Тест озвучки</button>
            </div>
        </div>

        <div class="setting-box">
            <label class="setting-label">🐯 Терминал Tiger.Trade (Local Signal Server :7819)</label>
            <div class="meta" style="margin-bottom:8px;">
                В терминале: <b>Настройки → Локальный сервер сигналов → Включить (порт 7819)</b>. Клик по карточке откроет монету в стакане.
            </div>
            <div class="toggle-row">
                <span>Автооткрытие новых Аптиков</span>
                <input type="checkbox" id="tiger-auto-up" onchange="saveLocalPrefs()">
            </div>
            <div class="toggle-row">
                <span>Режим TigerX Broker</span>
                <input type="checkbox" id="tiger-broker" onchange="saveLocalPrefs()">
            </div>
            <div class="input-row" style="margin-top:8px;">
                <select id="tiger-group" onchange="saveLocalPrefs()"></select>
                <button class="btn secondary" onclick="openInTiger('Binance','WIFUSDT')">▶ Тест: WIFUSDT</button>
            </div>
        </div>
    </div>

    <div class="toast" id="toast">Сохранено</div>

    <script>
        const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
        if (tg) { tg.ready(); tg.expand(); }
        const userId = tg && tg.initDataUnsafe && tg.initDataUnsafe.user ? tg.initDataUnsafe.user.id : 0;

        const UPTICK_EXCHANGES = ['Binance', 'Bybit', 'OKX', 'Aster'];
        const EX_VOICE = { 'Binance': 'Бинанс', 'Bybit': 'Байбит', 'OKX': 'ОКИКС', 'Aster': 'Астер' };
        const EX_SVG = {
            'Binance': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#F3BA2F"/><path d="M16 7l3.5 3.5-3.5 3.5-3.5-3.5L16 7zm-5.5 5.5L14 16l-3.5 3.5L7 16l3.5-3.5zm11 0L25 16l-3.5 3.5L18 16l3.5-3.5zM16 18l3.5 3.5L16 25l-3.5-3.5L16 18z" fill="#111"/></svg>',
            'Bybit': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#121418"/><text x="16" y="21" text-anchor="middle" font-size="12" font-weight="900" fill="#F7A600">BYB</text></svg>',
            'OKX': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#000"/><rect x="7" y="7" width="6" height="6" fill="#fff"/><rect x="19" y="7" width="6" height="6" fill="#fff"/><rect x="13" y="13" width="6" height="6" fill="#fff"/><rect x="7" y="19" width="6" height="6" fill="#fff"/><rect x="19" y="19" width="6" height="6" fill="#fff"/></svg>',
            'Aster': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#6366F1"/><path d="M16 6l2.8 7.2L26 16l-7.2 2.8L16 26l-2.8-7.2L6 16l7.2-2.8L16 6z" fill="#fff"/></svg>'
        };

        let uptickExFilter = 'ALL';
        let isMuted = false;
        let cachedHistory = [];
        let cachedActiveUp = [];
        let userBannedCoins = {};
        let settingsInitialized = false;
        let lastUptickId = 0;
        let firstLoadDone = false;
        let tigerWs = null;

        function updateMuteUI() {
            const pill = document.getElementById('mute-pill');
            const chk = document.getElementById('snd-mute-all');
            if (pill) {
                pill.classList.toggle('muted', isMuted);
                pill.classList.toggle('online', !isMuted);
                pill.textContent = isMuted ? '🔇 Выкл' : '🔊 Звук';
            }
            if (chk) chk.checked = isMuted;
        }

        function setMuted(muted, silentToast = false) {
            isMuted = Boolean(muted);
            if (isMuted && 'speechSynthesis' in window) {
                try { window.speechSynthesis.cancel(); } catch {}
            }
            updateMuteUI();
            saveLocalPrefs();
            if (!silentToast) showToast(isMuted ? '🔇 Звук выключен' : '🔊 Звук включен');
        }

        function toggleMute() { setMuted(!isMuted); }

        function setTigerPill(ok) {
            const el = document.getElementById('tiger-pill');
            el.classList.toggle('online', ok);
            el.textContent = ok ? '🐯 Tiger: ON' : '🐯 Tiger: OFF';
        }

        function connectTiger(manual = false) {
            if (tigerWs && tigerWs.readyState === WebSocket.OPEN) {
                if (manual) showToast('Tiger.Trade уже подключен');
                return;
            }
            try {
                tigerWs = new WebSocket('ws://127.0.0.1:7819');
                tigerWs.onopen = () => { setTigerPill(true); if (manual) showToast('🐯 Tiger.Trade подключен'); };
                tigerWs.onclose = () => setTigerPill(false);
                tigerWs.onerror = () => setTigerPill(false);
            } catch { setTigerPill(false); }
        }

        function mapTigerExchange(ex, useBroker) {
            if (useBroker) {
                if (ex === 'Binance') return 'TigerX Binance';
                if (ex === 'Bybit') return 'TigerX Bybit';
                if (ex === 'OKX') return 'TigerX OKX';
            }
            if (ex === 'Binance') return 'BINANCE-FUT';
            if (ex === 'Bybit') return 'BYBIT';
            if (ex === 'OKX') return 'OKX';
            return ex.toUpperCase();
        }

        function openInTiger(exchange, symbol) {
            if (!tigerWs || tigerWs.readyState !== WebSocket.OPEN) {
                connectTiger(false);
                showToast('🐯 Нет связи с Tiger.Trade (порт 7819)');
                return;
            }
            const group = document.getElementById('tiger-group').value || 'A';
            const useBroker = document.getElementById('tiger-broker').checked;
            tigerWs.send(JSON.stringify({
                type: 'setLinkSymbol',
                e: mapTigerExchange(exchange, useBroker),
                m: 'FUTURES',
                symbol: symbol,
                linkGroup: group
            }));
            showToast(`🐯 ${symbol} (${exchange}) → группа ${group}`);
        }

        function playTone(freq, dur = 0.18) {
            if (isMuted) return;
            try {
                const ctx = new (window.AudioContext || window.webkitAudioContext)();
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                osc.type = 'sine';
                osc.frequency.value = freq;
                gain.gain.value = 0.14;
                osc.connect(gain);
                gain.connect(ctx.destination);
                osc.start();
                osc.stop(ctx.currentTime + dur);
            } catch {}
        }

        function speakRu(text) {
            if (isMuted) return;
            try {
                if (!('speechSynthesis' in window)) return;
                window.speechSynthesis.cancel();
                const u = new SpeechSynthesisUtterance(text);
                u.lang = 'ru-RU';
                u.rate = 1.08;
                window.speechSynthesis.speak(u);
            } catch {}
        }

        function formatUsdVoice(v) {
            v = Number(v || 0);
            if (v >= 1e6) return (v / 1e6).toFixed(1).replace('.0', '') + ' миллиона';
            if (v >= 1e3) return Math.round(v / 1e3) + ' тысяч';
            return Math.round(v) + ' долларов';
        }

        function announceUptick(ev) {
            if (isMuted || !document.getElementById('snd-uptick').checked) return;
            playTone(880, 0.18);
            const exRu = EX_VOICE[ev.exchange] || ev.exchange;
            const sideRu = ev.side === 'LONG' ? 'лонг' : 'шорт';
            const symClean = (ev.symbol || '').replace('USDT', '');
            const wallPart = Number(ev.wall_usd || 0) > 0 ? `. Плотность ${formatUsdVoice(ev.wall_usd)}` : '';
            speakRu(`Аптик на бирже ${exRu}. ${symClean}. ${sideRu}${wallPart}`);
        }

        function testVoice() {
            if (isMuted) { showToast('🔇 Сначала включите звук'); return; }
            announceUptick({ exchange: 'Binance', symbol: 'WIFUSDT', side: 'LONG', wall_usd: 145000 });
        }

        function initTigerGroups() {
            const sel = document.getElementById('tiger-group');
            const items = [...'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split(''), ...Array.from({length: 12}, (_, i) => String(i))];
            sel.innerHTML = items.map(g => `<option value="${g}">Группа ${g}</option>`).join('');
        }

        function loadLocalPrefs() {
            try {
                const p = JSON.parse(localStorage.getItem('uptick_prefs_v2') || '{}');
                if (typeof p.muted === 'boolean') isMuted = p.muted;
                if (typeof p.sndUp === 'boolean') document.getElementById('snd-uptick').checked = p.sndUp;
                if (typeof p.autoUp === 'boolean') document.getElementById('tiger-auto-up').checked = p.autoUp;
                if (typeof p.broker === 'boolean') document.getElementById('tiger-broker').checked = p.broker;
                if (p.group) document.getElementById('tiger-group').value = p.group;
            } catch {}
            updateMuteUI();
        }

        function saveLocalPrefs() {
            localStorage.setItem('uptick_prefs_v2', JSON.stringify({
                muted: isMuted,
                sndUp: document.getElementById('snd-uptick').checked,
                autoUp: document.getElementById('tiger-auto-up').checked,
                broker: document.getElementById('tiger-broker').checked,
                group: document.getElementById('tiger-group').value
            }));
        }

        function switchTab(tab) {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
            document.querySelectorAll('.panel').forEach(p => p.classList.toggle('active', p.id === `tab-${tab}`));
        }

        function buildExchangeFilters() {
            const upBox = document.getElementById('uptick-ex-filters');
            upBox.innerHTML = `<button class="chip active" data-uex="ALL" onclick="setUptickEx('ALL')">Все (4)</button>` +
                UPTICK_EXCHANGES.map(ex => `<button class="chip" data-uex="${ex}" onclick="setUptickEx('${ex}')"><span class="ex-logo" style="width:16px;height:16px">${EX_SVG[ex]||''}</span>${ex}</button>`).join('');
        }

        function setUptickEx(ex) {
            uptickExFilter = ex;
            document.querySelectorAll('[data-uex]').forEach(b => b.classList.toggle('active', b.dataset.uex === ex));
            renderAll();
        }

        function fmtUsd(v) {
            v = Number(v || 0);
            if (v >= 1e9) return '$' + (v / 1e9).toFixed(2) + 'B';
            if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
            if (v >= 1e3) return '$' + (v / 1e3).toFixed(0) + 'K';
            return '$' + v.toFixed(0);
        }

        function uptickCardHtml(h) {
            const wallBadge = Number(h.wall_usd || 0) > 0
                ? `<span class="mkt-tag" style="background:rgba(245,158,11,0.18);color:#fbbf24;">🧱 ${fmtUsd(h.wall_usd)}</span>`
                : `<span class="mkt-tag">АПТИК</span>`;
            return `
            <div class="card" onclick="openInTiger('${h.exchange}','${h.symbol}')">
                <div class="card-top">
                    <div class="card-left">
                        <span class="ex-logo" title="${h.exchange}">${EX_SVG[h.exchange] || ''}</span>
                        <span class="sym">${h.symbol}</span>
                        ${wallBadge}
                    </div>
                    <div class="card-right">
                        <span class="side-badge ${h.side}">${h.side}</span>
                        <button class="ban-x" onclick="banCoin('${h.symbol}', event)" title="Скрыть монету">✕</button>
                    </div>
                </div>
                <div class="card-meta">
                    <span>Цена: <b>${h.price}</b> (планка ${h.limit_price})</span>
                    <span>24ч: <b>${fmtUsd(h.volume_24h_usd)}</b> · ${h.utc_time}</span>
                </div>
            </div>`;
        }

        function renderAll() {
            document.getElementById('live-up-cnt').textContent = cachedActiveUp.length;
            document.getElementById('live-active-upticks').innerHTML = cachedActiveUp.length
                ? cachedActiveUp.map(a => `
                    <div class="rule-item" style="cursor:pointer" onclick="openInTiger('${a.exchange}','${a.symbol}')">
                        <span style="display:flex;align-items:center;gap:6px">
                            <span class="ex-logo" style="width:18px;height:18px">${EX_SVG[a.exchange]||''}</span>
                            <b>${a.symbol}</b>
                            ${Number(a.wall_usd || 0) > 0 ? `<span style="color:#fbbf24;font-weight:700">🧱 ${fmtUsd(a.wall_usd)}</span>` : ''}
                        </span>
                        <span class="side-badge ${a.side}">${a.side}</span>
                    </div>`).join('')
                : '<div class="empty" style="padding:8px 0">Активных аптиков сейчас нет</div>';

            const upList = cachedHistory.filter(h => uptickExFilter === 'ALL' || h.exchange === uptickExFilter);
            document.getElementById('upticks-list').innerHTML = upList.length
                ? upList.map(uptickCardHtml).join('')
                : '<div class="empty">Аптиков за сегодня по выбранному фильтру пока нет.</div>';
        }

        function renderBannedCoins() {
            const box = document.getElementById('banned-coins-list');
            const keys = Object.keys(userBannedCoins || {}).sort();
            if (!keys.length) {
                box.innerHTML = '<div class="meta">Скрытых вручную монет пока нет.</div>';
                return;
            }
            box.innerHTML = keys.map(sym => `
                <span class="banned-chip">
                    ${sym}
                    <button onclick="unbanCoin('${sym}')" title="Вернуть">✕</button>
                </span>
            `).join('');
        }

        async function banCoin(symbol, evt) {
            if (evt) evt.stopPropagation();
            if (!userId) { showToast('Откройте приложение через Telegram'); return; }
            const clean = (symbol || '').trim().toUpperCase();
            if (!clean) return;
            try {
                const res = await fetch('/api/ban-coin', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, symbol: clean, scope: 'ALL' })
                });
                const data = await res.json();
                if (res.ok && data.status === 'ok') {
                    userBannedCoins = data.banned_coins || {};
                    renderBannedCoins();
                    showToast(`🚫 Монета ${data.base} скрыта`);
                    loadData();
                }
            } catch { showToast('Нет связи с сервером'); }
        }

        function banCoinFromInput() {
            const inp = document.getElementById('ban-coin-input');
            const val = (inp.value || '').trim();
            if (!val) { showToast('Введите тикер монеты'); return; }
            inp.value = '';
            banCoin(val);
        }

        async function unbanCoin(symbol) {
            if (!userId) return;
            try {
                const res = await fetch('/api/unban-coin', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, symbol })
                });
                const data = await res.json();
                if (res.ok) {
                    userBannedCoins = data.banned_coins || {};
                    renderBannedCoins();
                    showToast(`✅ Монета ${symbol} возвращена`);
                    loadData();
                }
            } catch {}
        }

        async function saveGlobalSettings() {
            if (!userId) { showToast('Откройте приложение через Telegram'); return; }
            try {
                const res = await fetch('/api/settings', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        user_id: userId,
                        min_volume: Number(document.getElementById('set-vol').value || 500000)
                    })
                });
                if (res.ok) { showToast('✅ Фильтр сохранен'); loadData(); }
            } catch { showToast('Ошибка сохранения'); }
        }

        function showToast(msg) {
            const el = document.getElementById('toast');
            el.textContent = msg;
            el.classList.add('show');
            clearTimeout(window._toastTimer);
            window._toastTimer = setTimeout(() => el.classList.remove('show'), 2200);
        }

        async function loadData() {
            try {
                const res = await fetch(`/api/data?user_id=${userId}`);
                if (!res.ok) return;
                const data = await res.json();

                cachedHistory = data.history || [];
                cachedActiveUp = data.active || [];

                if (!settingsInitialized) {
                    document.getElementById('set-vol').value = data.user_min_volume || 500000;
                    userBannedCoins = data.user_banned_coins || {};
                    renderBannedCoins();
                    settingsInitialized = true;
                }

                const newestUp = cachedHistory[0];
                if (firstLoadDone && newestUp && newestUp.id > lastUptickId) {
                    announceUptick(newestUp);
                    if (document.getElementById('tiger-auto-up').checked) {
                        openInTiger(newestUp.exchange, newestUp.symbol);
                    }
                }
                if (newestUp) lastUptickId = Math.max(lastUptickId, newestUp.id || 0);
                firstLoadDone = true;

                renderAll();
            } catch {}
        }

        initTigerGroups();
        loadLocalPrefs();
        buildExchangeFilters();
        connectTiger(false);
        loadData();
        setInterval(loadData, 3000);
    </script>
</body>
</html>
"""


# ==================== FASTAPI СЕРВЕР И КЭШ ====================

_api_cache: Dict[str, Any] = {"ts": 0.0, "utc_date": "", "history": []}


class SettingsPayload(BaseModel):
    user_id: int
    min_volume: float = DEFAULT_MIN_VOLUME


class SymbolPayload(BaseModel):
    user_id: int
    symbol: str


class BanCoinPayload(BaseModel):
    user_id: int
    symbol: str
    scope: str = "ALL"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await init_db()

    uptick_session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=60, ttl_dns_cache=300),
        headers={"User-Agent": "UptickScanner/3.0"},
    )

    scanners: List[BaseScanner] = [
        BinanceUptickScanner(uptick_session, engine),
        BybitUptickScanner(uptick_session, engine),
        OKXUptickScanner(uptick_session, engine),
        AsterUptickScanner(uptick_session, engine),
    ]

    tasks = [asyncio.create_task(s.run()) for s in scanners]

    if bot is not None:
        tasks.append(asyncio.create_task(dp.start_polling(bot, handle_signals=False)))
        logger.info("Запуск детектора аптиков (4 биржи: Binance, Bybit, OKX, Aster) + Telegram Bot...")
    else:
        logger.warning("BOT_TOKEN не задан — запущен только веб-сервер и сканеры аптиков.")

    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await uptick_session.close()
        if bot is not None:
            await bot.session.close()
        if _pg_pool is not None:
            await _pg_pool.close()


app = FastAPI(lifespan=lifespan)


@app.api_route("/", methods=["GET", "HEAD"])
@app.api_route("/ping", methods=["GET", "HEAD"])
async def ping() -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "service": "Uptick Detector",
            "storage": "postgresql" if _use_postgres else "sqlite",
            "uptick_exchanges": 4,
        }
    )


@app.get("/api/data")
async def api_data(user_id: int = Query(default=0)) -> JSONResponse:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = time.time()

    if now - _api_cache["ts"] > 2.0 or _api_cache["utc_date"] != today_utc:
        _api_cache["history"] = await db_fetchall(
            "SELECT id, utc_time, exchange, symbol, side, price, limit_price, volume_24h_usd, wall_usd "
            "FROM upticks WHERE utc_date = ? ORDER BY id DESC LIMIT 120",
            (today_utc,),
        )
        _api_cache["utc_date"] = today_utc
        _api_cache["ts"] = now

    user_min_vol = DEFAULT_MIN_VOLUME
    user_banned_map: Dict[str, str] = {}

    if user_id:
        row = await db_fetchone(
            "SELECT min_volume, banned_coins FROM users WHERE user_id = ?",
            (user_id,),
        )
        if row:
            user_min_vol = float(row.get("min_volume") or DEFAULT_MIN_VOLUME)
            user_banned_map = parse_banned_coins(row.get("banned_coins"))

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

    return JSONResponse(
        {
            "utc_date": today_utc,
            "active": active_upticks,
            "history": history_rows,
            "user_min_volume": user_min_vol,
            "user_banned_coins": user_banned_map,
        }
    )


@app.post("/api/settings")
async def api_settings(payload: SettingsPayload) -> JSONResponse:
    vol = max(0.0, payload.min_volume)
    await db_execute(
        "INSERT INTO users (user_id, min_volume) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET min_volume = excluded.min_volume",
        (payload.user_id, vol),
    )
    return JSONResponse({"status": "ok"})


@app.post("/api/ban-coin")
async def api_ban_coin(payload: BanCoinPayload) -> JSONResponse:
    base = extract_base_asset(payload.symbol)
    if not base:
        return JSONResponse({"status": "error", "detail": "Некорректный тикер"}, status_code=400)

    row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (payload.user_id,))
    banned = parse_banned_coins(row.get("banned_coins") if row else None)
    banned[base] = "ALL"

    await db_execute(
        "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
        (payload.user_id, json.dumps(banned)),
    )
    return JSONResponse({"status": "ok", "base": base, "banned_coins": banned})


@app.post("/api/unban-coin")
async def api_unban_coin(payload: SymbolPayload) -> JSONResponse:
    base = extract_base_asset(payload.symbol)
    row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (payload.user_id,))
    banned = parse_banned_coins(row.get("banned_coins") if row else None)
    banned.pop(base, None)

    await db_execute(
        "UPDATE users SET banned_coins = ? WHERE user_id = ?",
        (json.dumps(banned), payload.user_id),
    )
    return JSONResponse({"status": "ok", "banned_coins": banned})


@app.get("/webapp", response_class=HTMLResponse)
async def webapp_page() -> HTMLResponse:
    return HTMLResponse(content=WEBAPP_HTML)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)