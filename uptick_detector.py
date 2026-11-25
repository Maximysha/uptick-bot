"""
Мультибиржевой детектор «Аптиков» (4 биржи) + Сканер «Плотностей» (7 бирж, Спот и Фьючерсы)
+ Telegram Bot + Telegram Mini App + Tiger.Trade Local Signal Server API.
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
logger = logging.getLogger("uptick_density_bot")

# ==================== КОНФИГУРАЦИЯ ====================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
SQLITE_PATH = os.getenv("SQLITE_PATH", "upticks.db")

DEFAULT_MIN_VOLUME = 500_000.0          # $500k 24ч оборот для аптиков
DEFAULT_MIN_DENSITY_SPOT = 250_000.0    # $250k плотность на споте
DEFAULT_MIN_DENSITY_FUTURES = 1_000_000.0  # $1M плотность на фьючерсах

UPTICK_TOLERANCE_PCT = 0.0008           # 0.08% допуск до биржевой планки
UPTICK_COOLDOWN_SEC = 180               # 3 минуты между повторами аптика
DENSITY_MAX_DISTANCE_PCT = 5.0          # Плотности в пределах 5% от цены
DENSITY_COOLDOWN_SEC = 180              # 3 минуты между повторами плотности
DENSITY_HARD_MIN_SPOT = 10_000.0        # Минимальный порог хранения для кастомных правил
DENSITY_HARD_MIN_FUTURES = 25_000.0

# ==================== ЧЕРНЫЙ СПИСОК (ТЯЖЕЛОВЕСЫ + АКЦИИ + МЕТАЛЛЫ + НЕФТЬ + FOREX) ====================

# 1. Тяжелые монеты, стейблкоины и обернутые активы (не нужны для скальпинга щиткоинов)
HEAVY_CRYPTO_BLACKLIST: Set[str] = {
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

# 2. Металлы, нефть, газ, сырье, индексы и Forex (под любыми биржевыми тикерами)
METALS_COMMODITIES_FOREX_BLACKLIST: Set[str] = {
    # Драгоценные и промышленные металлы
    "XAU", "XAG", "XPT", "XPD", "XCU", "XAL", "XNI", "XZN", "XPB", "XSN",
    "PAXG", "XAUT", "KAU", "KAG", "PMGT", "DGX", "MCAU", "AUSDT",
    "GOLD", "SILVER", "COPPER", "PLATINUM", "PALLADIUM", "ALUMINUM",
    "ALUMINIUM", "NICKEL", "ZINC", "COBALT", "LITHIUM", "URANIUM", "IRON",
    "GC", "SI", "HG", "PL", "PA", "MGC", "SIL",
    # Нефть, газ, энергетика и сырье
    "CL", "BZ", "WTI", "BRENT", "USOIL", "UKOIL", "CRUDE", "CRUDEOIL",
    "OIL", "QM", "MCL", "NG", "NATGAS", "NATURALGAS", "RB", "HO", "GASOLINE",
    "HEATINGOIL", "COAL", "WHEAT", "CORN", "SOY", "SOYBEAN", "COFFEE",
    "SUGAR", "COCOA", "COTTON", "LUMBER", "CATTLE", "HOGS", "ZW", "ZC", "ZS",
    # Индексы и волатильность
    "DXY", "US30", "US100", "US500", "US2000", "NAS100", "NDX", "SPX500",
    "SP500", "DJI", "DJIA", "DOW", "RUT", "RUS2000", "GER40", "GER30", "DAX",
    "UK100", "FTSE", "FRA40", "CAC40", "EU50", "EUSTX50", "JP225", "NIKKEI",
    "HK50", "HSI", "CN50", "CHINA50", "A50", "AUS200", "VIX", "KOSPI", "TW50",
    # Валюты Forex
    "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD", "CNH", "CNY", "HKD",
    "SGD", "KRW", "TRY", "MXN", "ZAR", "BRL", "INR", "PLN", "SEK", "NOK",
    "DKK", "CZK", "HUF", "ILS", "THB", "TWD", "PHP", "MYR", "IDR", "VND",
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    "EURGBP", "EURJPY", "GBPJPY", "USDCNH", "USDTRY", "USDMXN", "USDZAR",
}

# 3. Базовые тикеры акций и ETF (США, Азия, Европа, крипто-акции, плечевые ETF)
# Автоматически блокируются также с суффиксами: X (xStocks), B (Binance bStocks), ON (Ondo), T
STOCKS_ETFS_BLACKLIST: Set[str] = {
    # Бигтех и полупроводники
    "AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "GOOG", "GOOGL", "META", "NFLX",
    "AMD", "INTC", "MU", "AVGO", "QCOM", "TXN", "TSM", "ASML", "ARM", "MRVL",
    "SMCI", "DELL", "HPE", "HPQ", "ORCL", "IBM", "CSCO", "CRM", "ADBE", "NOW",
    "PLTR", "SNOW", "PANW", "CRWD", "DDOG", "NET", "MDB", "ZS", "FTNT", "OKTA",
    "TEAM", "WDAY", "INTU", "CDNS", "SNPS", "ANET", "MPWR", "AMAT", "LRCX",
    "KLAC", "WDC", "STX", "SNDK", "SKHY", "LITE", "COHR", "ON", "NXPI", "ADI",
    "MCHP", "GFS", "UMC", "STM", "TER", "ENTG", "ACLS", "ALAB", "CRDO",
    # Космос, ИИ, кванты, популярные тикеры
    "SPCX", "RKLB", "ASTS", "LUNR", "PL", "SPCE", "IONQ", "RGTI", "QUBT",
    "QBTS", "SOUN", "BBAI", "TEM", "APP", "DUOL", "AXON", "VRT", "VST", "CEG",
    "OKLO", "NNE", "SMR", "GEV", "ETN", "PWR", "ANT", "OPENAI", "ANTHROPIC",
    # Крипто-акции и финтех
    "COIN", "MSTR", "CRCL", "HOOD", "MARA", "RIOT", "CLSK", "IREN", "CIFR",
    "WULF", "HUT", "BITF", "BTBT", "BTDR", "CORZ", "GLXY", "BMNR", "STRC",
    "DFDV", "SBET", "UPXI", "PYPL", "SQ", "XYZ", "SOFI", "AFRM", "UPST",
    "NU", "MELI", "SE", "GRAB", "IBKR", "SCHW", "BLK", "BX", "KKR", "APO",
    "JPM", "BAC", "WFC", "GS", "MS", "USB", "PNC", "TFC", "MA", "AXP",
    "COF", "DFS", "SPGI", "MCO", "ICE", "CME", "NDAQ", "FISV", "ADP", "BRK",
    "BRKB", "BRKA", "CITI",
    # Потребительский сектор, медиа, авто, ритейл
    "UBER", "LYFT", "ABNB", "DASH", "BKNG", "EXPE", "SHOP", "EBAY", "ETSY",
    "SPOT", "ROKU", "RBLX", "TTWO", "EA", "DIS", "CMCSA", "WBD", "PARA",
    "SNAP", "PINS", "RDDT", "DJT", "MTCH", "BMBL", "CVNA", "DKNG", "PENN",
    "WMT", "COST", "TGT", "HD", "LOW", "NKE", "LULU", "ONON", "DECK", "CROX",
    "SBUX", "MCD", "CMG", "CAVA", "DPZ", "YUM", "KO", "PEP", "MNST", "CELH",
    "PG", "KMB", "CLX", "CHD", "ELF", "EL", "UL", "KHC", "GIS", "MDLZ", "HSY",
    "STZ", "BUD", "DEO", "MO", "PM", "BTI", "GM", "RIVN", "LCID", "RACE",
    "TM", "HMC", "SONY", "NTDOY", "LVMH", "NVO",
    # Фарма и медицина
    "LLY", "UNH", "JNJ", "PFE", "MRNA", "BNTX", "ABBV", "MRK", "BMY", "GILD",
    "AMGN", "REGN", "VRTX", "ISRG", "SYK", "MDT", "ABT", "TMO", "DHR", "CVS",
    "CI", "HUM", "ELV", "HIMS", "OSCR", "AZN", "GSK", "SNY", "NVS",
    # Энергетика, промышленность, оборона, добыча, авиа
    "XOM", "CVX", "OXY", "SLB", "COP", "EOG", "MPC", "VLO", "PSX", "HAL",
    "BKR", "KMI", "WMB", "TTE", "BP", "SHEL", "PLUG", "FSLR", "ENPH", "SEDG",
    "NEE", "DUK", "SO", "BA", "LMT", "RTX", "NOC", "GD", "LHX", "CAT", "DE",
    "GE", "HON", "MMM", "UPS", "FDX", "UNP", "CSX", "NSC", "AAL", "DAL",
    "UAL", "LUV", "CCL", "RCL", "NCLH", "VZ", "TMUS", "AMT", "PLD", "EQIX",
    "FCX", "SCCO", "NEM", "AEM", "WPM", "FNV", "KGC", "PAAS", "HL", "CDE",
    "VALE", "BHP", "RIO", "ALB", "SQM", "LAC", "MP", "CCJ", "UEC", "UUUU",
    "NXE", "DNN", "LEU",
    # Китай / Азия ADR
    "BABA", "JD", "PDD", "NIO", "XPEV", "LI", "BIDU", "BILI", "TCEHY", "NTES",
    "TME", "FUTU", "TIGR", "CPNG", "INFY", "HDB", "IBN", "SAP",
    # Мем-акции
    "GME", "AMC", "KOSS", "TLRY", "CGC", "SNDL", "NKLA", "MULN", "FFIE",
    "KODK", "BYND", "CHWY", "Z", "ZG", "RDFN", "CLOV", "WISH", "SPWR",
    # ETF (индексные, отраслевые, товарные, крипто, плечевые 2x/3x)
    "SPY", "QQQ", "QQQM", "IWM", "DIA", "VTI", "VOO", "IVV", "VT", "VEA",
    "VWO", "EEM", "EFA", "EWY", "EWJ", "EWZ", "FXI", "KWEB", "MCHI", "INDA",
    "SMH", "SOXX", "SOXL", "SOXS", "TQQQ", "SQQQ", "UPRO", "SPXU", "SPXL",
    "SPXS", "UDOW", "SDOW", "TNA", "TZA", "LABU", "LABD", "FNGU", "FNGD",
    "BULZ", "BERZ", "MAGS", "MAGX", "IGV", "ARTY", "BOTZ", "AIQ", "CIBR",
    "HACK", "ARKK", "ARKG", "ARKW", "ARKF", "ARKQ",
    "NVDL", "NVDX", "NVDU", "NVDS", "NVD", "TSLL", "TSLT", "TSLQ", "TSLZ",
    "TSDD", "AMDL", "AMDS", "MSTU", "MSTX", "MSTZ", "MSTP", "CONL", "CONI",
    "COINX", "PLTU", "PLTD", "METU", "AMZU", "AAPU", "GGLL", "MSFU", "NFXL",
    "GLD", "GLDM", "IAU", "SGOL", "SLV", "SIVR", "PSLV", "GDX", "GDXJ",
    "SIL", "SILJ", "PPLT", "PALL", "COPX", "CPER", "URA", "URNM", "USO",
    "UCO", "SCO", "UNG", "BOIL", "KOLD", "DBC", "GSG", "DBA",
    "TLT", "TMF", "TMV", "IEF", "SHY", "SGOV", "BIL", "BND", "AGG", "HYG",
    "JNK", "LQD", "TIP",
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE",
    "XLC", "XBI", "XOP", "XME", "XRT", "KRE", "KBE", "ITB", "XHB", "ITA",
    "JETS", "TAN", "ICLN", "LIT", "REMX", "VNQ", "SCHD", "JEPI", "JEPQ",
    "VIG", "VYM", "DVY", "UVXY", "UVIX", "VIXY", "VXX", "SVXY", "SVIX",
    "IBIT", "FBTC", "BITB", "ARKB", "GBTC", "BITO", "BITX", "BITI", "ETHA",
    "FETH", "ETHE", "ETHU", "SOLT", "XXRP",
}

TRADFI_BASE_SET: Set[str] = METALS_COMMODITIES_FOREX_BLACKLIST | STOCKS_ETFS_BLACKLIST
ALL_EXACT_BLACKLIST: Set[str] = HEAVY_CRYPTO_BLACKLIST | TRADFI_BASE_SET

# Суффиксы токенизированных акций/сырья:
# - X: xStocks (TSLAX, NVDAX, SNDKX, SKHYX, GLDX)
# - B: Binance bStocks (SNDKB, NVDAB, TSLAB, AAPLB)
# - ON: Ondo Stocks (SOXLON, NVDAON, SNDKON, SPCXON)
# - T: tCommodities (XAUT, GOLDT, OILT)
TRADFI_SUBSTRINGS = (
    "STOCK", "BSTOCK", "XSTOCK", "TCOMMOD",
    "XAU", "XAG", "XPT", "XPD", "XCU",
    "USOIL", "UKOIL", "CRUDE", "BRENT", "WTI", "NATGAS",
    "GOLD", "SILVER", "COPPER", "PLATINUM", "PALLADIUM",
    "SP500", "SPX500", "NAS100", "US500", "US100", "US30", "GER40", "UK100", "JP225", "HK50",
)


def extract_base_asset(symbol: str) -> str:
    """Извлекает базовый тикер монеты (1000PEPEUSDT -> PEPE, BTC-USDT-SWAP -> BTC)."""
    s = (symbol or "").upper().strip()
    s = s.replace("/", "").replace("_", "").replace("-", "")
    for suffix in ("USDTM", "USDTSWAP", "USDTPERP", "USDT", "USDC", "USD", "PERP", "SWAP"):
        if s.endswith(suffix) and len(s) > len(suffix):
            s = s[: -len(suffix)]
            break
    # Убираем числовые префиксы множителей (1000PEPE -> PEPE, 1000000MOG -> MOG, 1MBABYDOGE -> BABYDOGE)
    s = re.sub(r"^(1000000|100000|10000|1000|100|1M)", "", s)
    return s


def is_blacklisted_symbol(symbol: str) -> bool:
    """Проверяет, является ли символ тяжеловесом, акцией, ETF, металлом, нефтью или Forex."""
    base = extract_base_asset(symbol)
    if not base:
        return True

    # 1. Прямое совпадение
    if base in ALL_EXACT_BLACKLIST:
        return True

    # 2. Токенизированные суффиксы акций/сырья (ON, X, B, T)
    if len(base) > 2 and base.endswith("ON") and base[:-2] in TRADFI_BASE_SET:
        return True
    if len(base) > 2 and base.endswith(("X", "B", "T")) and base[:-1] in TRADFI_BASE_SET:
        return True
    # Двойной суффикс вроде BRK.BX -> BRKBX
    if len(base) > 3 and base.endswith(("BX", "BON", "XB")) and base[:-2] in TRADFI_BASE_SET:
        return True

    # 3. Очевидные TradFi-маркеры в названии тикера
    for marker in TRADFI_SUBSTRINGS:
        if marker in base:
            return True

    # 4. Пары с точкой (например BRK.B)
    raw_up = (symbol or "").upper()
    if "." in raw_up:
        dot_base = raw_up.split(".")[0]
        if dot_base in TRADFI_BASE_SET:
            return True

    return False


# ==================== ТИПЫ И МОДЕЛИ ====================

class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class MarketType(str, Enum):
    SPOT = "SPOT"
    FUTURES = "FUTURES"


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
    timestamp_utc: str


@dataclass
class DensityEvent:
    exchange: str
    market_type: MarketType
    symbol: str
    side: Side
    price: float
    size_usd: float
    distance_pct: float
    volume_24h_usd: float
    timestamp_utc: str


# ==================== БАЗА ДАННЫХ (PostgreSQL + SQLite fallback) ====================

_pg_pool: Any = None
_use_postgres: bool = False


def _pg_sql(sql: str) -> str:
    """Преобразует ?-плейсхолдеры SQLite в $1, $2... для PostgreSQL."""
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
                        volume_24h_usd DOUBLE PRECISION
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
                # Миграция колонок, если таблицы уже существовали
                for col_def in (
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS min_density_spot DOUBLE PRECISION DEFAULT 250000",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS min_density_futures DOUBLE PRECISION DEFAULT 1000000",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS coin_rules TEXT DEFAULT '{}'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS banned_coins TEXT DEFAULT '{}'",
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
                volume_24h_usd REAL
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
        # Миграция старых баз SQLite
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


# ==================== ПОЛЬЗОВАТЕЛЬСКИЕ ПРАВИЛА И БАНЫ ====================

def parse_coin_rules(raw: Optional[str]) -> Dict[str, Dict[str, float]]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        out: Dict[str, Dict[str, float]] = {}
        for k, v in data.items():
            base = extract_base_asset(str(k))
            if base and isinstance(v, dict):
                spot = max(DENSITY_HARD_MIN_SPOT, float(v.get("spot") or DEFAULT_MIN_DENSITY_SPOT))
                fut = max(DENSITY_HARD_MIN_FUTURES, float(v.get("futures") or DEFAULT_MIN_DENSITY_FUTURES))
                out[base] = {"spot": spot, "futures": fut}
        return out
    except Exception:
        return {}


def get_user_density_threshold(
    symbol: str,
    market_type: str,
    default_spot: float,
    default_fut: float,
    coin_rules: Dict[str, Dict[str, float]],
) -> float:
    base = extract_base_asset(symbol)
    rule = coin_rules.get(base)
    if rule:
        return float(rule["spot"] if market_type == "SPOT" else rule["futures"])
    return float(default_spot if market_type == "SPOT" else default_fut)


def normalize_ban_scope(scope: Optional[str]) -> str:
    s = (scope or "ALL").strip().upper()
    if s in ("UPTICK", "UPTICKS", "U", "АПТИК", "АПТИКИ"):
        return "UPTICK"
    if s in ("DENSITY", "DENSITIES", "D", "ПЛОТНОСТЬ", "ПЛОТНОСТИ"):
        return "DENSITY"
    return "ALL"


def parse_banned_coins(raw: Optional[str]) -> Dict[str, str]:
    """Возвращает {BASE: 'ALL' | 'UPTICK' | 'DENSITY'}."""
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
        self._active: Dict[Tuple[str, str], Tuple[Side, float]] = {}
        self._cooldown: Dict[Tuple[str, str, Side], float] = {}

    async def evaluate(self, snap: TickerSnapshot) -> None:
        if is_blacklisted_symbol(snap.symbol):
            return
        if snap.last_price <= 0 or snap.upper_limit <= 0 or snap.lower_limit <= 0:
            return
        if snap.upper_limit <= snap.lower_limit:
            return

        now = time.time()
        key = (snap.exchange, snap.symbol)

        # Проверяем, что реальная цена сделки И стакан поджаты к планке (без ложных алертов на пустом стакане)
        upper_trigger = snap.upper_limit * (1.0 - UPTICK_TOLERANCE_PCT)
        lower_trigger = snap.lower_limit * (1.0 + UPTICK_TOLERANCE_PCT)

        hit_long = (
            snap.last_price >= upper_trigger
            and (snap.best_bid <= 0 or snap.best_bid >= snap.upper_limit * (1.0 - UPTICK_TOLERANCE_PCT * 2.5))
        )
        hit_short = (
            snap.last_price <= lower_trigger
            and (snap.best_ask <= 0 or snap.best_ask <= snap.lower_limit * (1.0 + UPTICK_TOLERANCE_PCT * 2.5))
        )

        if hit_long:
            self._active[key] = (Side.LONG, now)
            cd_key = (snap.exchange, snap.symbol, Side.LONG)
            if now - self._cooldown.get(cd_key, 0.0) >= UPTICK_COOLDOWN_SEC:
                self._cooldown[cd_key] = now
                ev = UptickEvent(
                    exchange=snap.exchange,
                    symbol=snap.symbol,
                    side=Side.LONG,
                    price=snap.last_price,
                    limit_price=snap.upper_limit,
                    volume_24h_usd=snap.volume_24h_usd,
                    timestamp_utc=datetime.now(timezone.utc).strftime("%H:%M:%S"),
                )
                await self.on_uptick(ev)
        elif hit_short:
            self._active[key] = (Side.SHORT, now)
            cd_key = (snap.exchange, snap.symbol, Side.SHORT)
            if now - self._cooldown.get(cd_key, 0.0) >= UPTICK_COOLDOWN_SEC:
                self._cooldown[cd_key] = now
                ev = UptickEvent(
                    exchange=snap.exchange,
                    symbol=snap.symbol,
                    side=Side.SHORT,
                    price=snap.last_price,
                    limit_price=snap.lower_limit,
                    volume_24h_usd=snap.volume_24h_usd,
                    timestamp_utc=datetime.now(timezone.utc).strftime("%H:%M:%S"),
                )
                await self.on_uptick(ev)
        else:
            self._active.pop(key, None)

    def get_active_upticks(self) -> List[Tuple[str, str, Side]]:
        now = time.time()
        expired = [k for k, (_, ts) in self._active.items() if now - ts > 60]
        for k in expired:
            self._active.pop(k, None)
        return [(ex, sym, side) for (ex, sym), (side, _) in self._active.items()]


# ==================== ДВИЖОК ПЛОТНОСТЕЙ (7 БИРЖ) ====================

class DensityEngine:
    def __init__(self, on_density: Callable[[DensityEvent], Any]) -> None:
        self.on_density = on_density
        self._active: Dict[Tuple[str, str, str, str], Tuple[DensityEvent, float]] = {}
        self._cooldown: Dict[Tuple[str, str, str, str], float] = {}
        self.watched_bases: Set[str] = set()
        self.min_spot_active: float = DEFAULT_MIN_DENSITY_SPOT
        self.min_fut_active: float = DEFAULT_MIN_DENSITY_FUTURES

    async def refresh_watched_bases(self) -> None:
        """Обновляет список монет из кастомных правил пользователей и минимальные пороги."""
        try:
            rows = await db_fetchall("SELECT min_density_spot, min_density_futures, coin_rules FROM users")
            bases: Set[str] = set()
            min_s = DEFAULT_MIN_DENSITY_SPOT
            min_f = DEFAULT_MIN_DENSITY_FUTURES
            for r in rows:
                us = float(r.get("min_density_spot") or DEFAULT_MIN_DENSITY_SPOT)
                uf = float(r.get("min_density_futures") or DEFAULT_MIN_DENSITY_FUTURES)
                if us < min_s:
                    min_s = us
                if uf < min_f:
                    min_f = uf
                rules = parse_coin_rules(r.get("coin_rules"))
                for b, rv in rules.items():
                    bases.add(b)
                    if rv["spot"] < min_s:
                        min_s = rv["spot"]
                    if rv["futures"] < min_f:
                        min_f = rv["futures"]
            self.watched_bases = bases
            self.min_spot_active = max(DENSITY_HARD_MIN_SPOT, min_s)
            self.min_fut_active = max(DENSITY_HARD_MIN_FUTURES, min_f)
        except Exception as exc:
            logger.debug("Ошибка обновления watched_bases: %s", exc)

    async def process_orderbook(
        self,
        exchange: str,
        market_type: MarketType,
        symbol: str,
        current_price: float,
        volume_24h_usd: float,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
        contract_multiplier: float = 1.0,
    ) -> None:
        base = extract_base_asset(symbol)
        if (is_blacklisted_symbol(symbol) and base not in self.watched_bases) or current_price <= 0:
            return

        threshold = self.min_spot_active if market_type == MarketType.SPOT else self.min_fut_active
        now = time.time()

        # В стакане: BID (покупка / поддержка) = LONG, ASK (продажа / сопротивление) = SHORT
        for side, levels in ((Side.LONG, bids), (Side.SHORT, asks)):
            best_wall: Optional[Tuple[float, float, float]] = None  # (size_usd, price, dist_pct)
            for px, qty in levels:
                if px <= 0 or qty <= 0:
                    continue
                dist_pct = abs(px - current_price) / current_price * 100.0
                if dist_pct > DENSITY_MAX_DISTANCE_PCT:
                    continue
                size_usd = px * qty * contract_multiplier
                if size_usd >= threshold:
                    if best_wall is None or size_usd > best_wall[0]:
                        best_wall = (size_usd, px, dist_pct)

            key = (exchange, market_type.value, symbol, side.value)
            if best_wall is not None:
                size_usd, wall_px, dist_pct = best_wall
                ev = DensityEvent(
                    exchange=exchange,
                    market_type=market_type,
                    symbol=symbol,
                    side=side,
                    price=wall_px,
                    size_usd=size_usd,
                    distance_pct=round(dist_pct, 2),
                    volume_24h_usd=volume_24h_usd,
                    timestamp_utc=datetime.now(timezone.utc).strftime("%H:%M:%S"),
                )
                self._active[key] = (ev, now)
                if now - self._cooldown.get(key, 0.0) >= DENSITY_COOLDOWN_SEC:
                    self._cooldown[key] = now
                    await self.on_density(ev)
            else:
                self._active.pop(key, None)

    def get_active_densities(self) -> List[DensityEvent]:
        now = time.time()
        expired = [k for k, (_, ts) in self._active.items() if now - ts > 90]
        for k in expired:
            self._active.pop(k, None)
        items = [ev for ev, _ in self._active.values()]
        items.sort(key=lambda x: x.size_usd, reverse=True)
        return items


# ==================== БАЗОВЫЙ HTTP КЛИЕНТ СКАНЕРОВ ====================

class BaseScanner:
    name: str = "Base"
    poll_interval: float = 3.0

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
    poll_interval = 3.0

    def __init__(self, session: aiohttp.ClientSession, engine: UptickEngine) -> None:
        super().__init__(session)
        self.engine = engine
        self._percent_limits: Dict[str, Tuple[float, float]] = {}
        self._last_info_refresh = 0.0

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
                )
            )


class BybitUptickScanner(BaseScanner):
    name = "Bybit"
    poll_interval = 3.5

    def __init__(self, session: aiohttp.ClientSession, engine: UptickEngine) -> None:
        super().__init__(session)
        self.engine = engine

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
            # Отбираем монеты с заметным отклонением Last от Mark (>1.2%) для точного запроса /v5/market/price-limit
            if abs(last - mark) / mark >= 0.012:
                candidates.append((sym, last, mark, bid, ask, vol))

        sem = asyncio.Semaphore(8)

        async def check_one(item: Tuple[str, float, float, float, float, float]) -> None:
            sym, last, mark, bid, ask, vol = item
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
                    )
                )

        if candidates:
            await asyncio.gather(*(check_one(c) for c in candidates[:30]))


class OKXUptickScanner(BaseScanner):
    name = "OKX"
    poll_interval = 3.5

    def __init__(self, session: aiohttp.ClientSession, engine: UptickEngine) -> None:
        super().__init__(session)
        self.engine = engine

    async def scan_once(self) -> None:
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
            if last > 0 and mark > 0 and abs(last - mark) / mark >= 0.012:
                candidates.append((inst_id, sym, last, mark, bid, ask, vol))

        sem = asyncio.Semaphore(8)

        async def check_one(item: Tuple[str, str, float, float, float, float, float]) -> None:
            inst_id, sym, last, mark, bid, ask, vol = item
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
                    )
                )

        if candidates:
            await asyncio.gather(*(check_one(c) for c in candidates[:30]))


class AsterUptickScanner(BaseScanner):
    name = "Aster"
    poll_interval = 4.0

    def __init__(self, session: aiohttp.ClientSession, engine: UptickEngine) -> None:
        super().__init__(session)
        self.engine = engine
        self._percent_limits: Dict[str, Tuple[float, float]] = {}
        self._last_info_refresh = 0.0

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
                )
            )


# ==================== СКАНЕРЫ ПЛОТНОСТЕЙ (7 БИРЖ) ====================

class BaseDensityScanner(BaseScanner):
    poll_interval: float = 7.0
    max_pairs_per_market: int = 32
    min_pair_volume_usd: float = 1_500_000.0

    def __init__(self, session: aiohttp.ClientSession, engine: DensityEngine) -> None:
        super().__init__(session)
        self.engine = engine

    @staticmethod
    def _parse_levels(raw_levels: Any) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        if not isinstance(raw_levels, list):
            return out
        for lvl in raw_levels:
            try:
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    out.append((float(lvl[0]), float(lvl[1])))
                elif isinstance(lvl, dict):
                    out.append((float(lvl.get("p") or 0), float(lvl.get("s") or 0)))
            except (TypeError, ValueError):
                continue
        return out


class BinanceDensityScanner(BaseDensityScanner):
    name = "Binance"

    async def _scan_market(self, mkt: MarketType, ticker_url: str, depth_url_tpl: str) -> None:
        data = await self._get_json(ticker_url)
        if not isinstance(data, list):
            return
        pairs = []
        for t in data:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            is_watched = extract_base_asset(sym) in self.engine.watched_bases
            if is_blacklisted_symbol(sym) and not is_watched:
                continue
            try:
                px = float(t.get("lastPrice") or 0)
                vol = float(t.get("quoteVolume") or 0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((is_watched, vol, sym, px))

        pairs.sort(key=lambda x: (x[0], x[1]), reverse=True)
        sem = asyncio.Semaphore(6)

        async def fetch_book(item: Tuple[bool, float, str, float]) -> None:
            _, vol, sym, px = item
            async with sem:
                ob = await self._get_json(depth_url_tpl.format(sym=sym))
            if not isinstance(ob, dict):
                return
            await self.engine.process_orderbook(
                exchange=self.name,
                market_type=mkt,
                symbol=sym,
                current_price=px,
                volume_24h_usd=vol,
                bids=self._parse_levels(ob.get("bids")),
                asks=self._parse_levels(ob.get("asks")),
            )

        await asyncio.gather(*(fetch_book(p) for p in pairs[: self.max_pairs_per_market]))

    async def scan_once(self) -> None:
        await self._scan_market(
            MarketType.SPOT,
            "https://api.binance.com/api/v3/ticker/24hr",
            "https://api.binance.com/api/v3/depth?symbol={sym}&limit=50",
        )
        await self._scan_market(
            MarketType.FUTURES,
            "https://fapi.binance.com/fapi/v1/ticker/24hr",
            "https://fapi.binance.com/fapi/v1/depth?symbol={sym}&limit=50",
        )


class BybitDensityScanner(BaseDensityScanner):
    name = "Bybit"

    async def _scan_category(self, mkt: MarketType, category: str) -> None:
        data = await self._get_json(f"https://api.bybit.com/v5/market/tickers?category={category}")
        if not isinstance(data, dict):
            return
        pairs = []
        for t in data.get("result", {}).get("list", []):
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            is_watched = extract_base_asset(sym) in self.engine.watched_bases
            if is_blacklisted_symbol(sym) and not is_watched:
                continue
            try:
                px = float(t.get("lastPrice") or 0)
                vol = float(t.get("turnover24h") or 0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((is_watched, vol, sym, px))

        pairs.sort(key=lambda x: (x[0], x[1]), reverse=True)
        sem = asyncio.Semaphore(6)

        async def fetch_book(item: Tuple[bool, float, str, float]) -> None:
            _, vol, sym, px = item
            async with sem:
                ob = await self._get_json(
                    f"https://api.bybit.com/v5/market/orderbook?category={category}&symbol={sym}&limit=50"
                )
            if not isinstance(ob, dict):
                return
            res = ob.get("result") or {}
            await self.engine.process_orderbook(
                exchange=self.name,
                market_type=mkt,
                symbol=sym,
                current_price=px,
                volume_24h_usd=vol,
                bids=self._parse_levels(res.get("b")),
                asks=self._parse_levels(res.get("a")),
            )

        await asyncio.gather(*(fetch_book(p) for p in pairs[: self.max_pairs_per_market]))

    async def scan_once(self) -> None:
        await self._scan_category(MarketType.SPOT, "spot")
        await self._scan_category(MarketType.FUTURES, "linear")


class OKXDensityScanner(BaseDensityScanner):
    name = "OKX"
    max_pairs_per_market = 28

    def __init__(self, session: aiohttp.ClientSession, engine: DensityEngine) -> None:
        super().__init__(session, engine)
        self._ct_vals: Dict[str, float] = {}
        self._last_inst_refresh = 0.0

    async def _refresh_ct_vals(self) -> None:
        if time.time() - self._last_inst_refresh < 3600 and self._ct_vals:
            return
        raw = await self._get_json("https://www.okx.com/api/v5/public/instruments?instType=SWAP")
        if not isinstance(raw, dict):
            return
        ct_map: Dict[str, float] = {}
        for item in raw.get("data", []):
            inst_id = item.get("instId", "")
            try:
                ct_map[inst_id] = float(item.get("ctVal") or 1.0)
            except (TypeError, ValueError):
                ct_map[inst_id] = 1.0
        if ct_map:
            self._ct_vals = ct_map
            self._last_inst_refresh = time.time()

    async def _scan_type(self, mkt: MarketType, inst_type: str) -> None:
        raw = await self._get_json(f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}")
        if not isinstance(raw, dict):
            return
        pairs = []
        for t in raw.get("data", []):
            inst_id = t.get("instId", "")
            if inst_type == "SPOT" and not inst_id.endswith("-USDT"):
                continue
            if inst_type == "SWAP" and not inst_id.endswith("-USDT-SWAP"):
                continue
            clean_sym = inst_id.replace("-USDT-SWAP", "USDT").replace("-USDT", "USDT")
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if is_blacklisted_symbol(clean_sym) and not is_watched:
                continue
            try:
                px = float(t.get("last") or 0)
                vol_usd = float(t.get("volCcy24h") or 0) * (px if inst_type == "SWAP" else 1.0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol_usd >= self.min_pair_volume_usd or is_watched):
                pairs.append((is_watched, vol_usd, inst_id, clean_sym, px))

        pairs.sort(key=lambda x: (x[0], x[1]), reverse=True)
        sem = asyncio.Semaphore(6)

        async def fetch_book(item: Tuple[bool, float, str, str, float]) -> None:
            _, vol_usd, inst_id, clean_sym, px = item
            async with sem:
                ob = await self._get_json(f"https://www.okx.com/api/v5/market/books?instId={inst_id}&sz=50")
            if not isinstance(ob, dict):
                return
            arr = ob.get("data") or []
            if not arr:
                return
            mult = self._ct_vals.get(inst_id, 1.0) if inst_type == "SWAP" else 1.0
            await self.engine.process_orderbook(
                exchange=self.name,
                market_type=mkt,
                symbol=clean_sym,
                current_price=px,
                volume_24h_usd=vol_usd,
                bids=self._parse_levels(arr[0].get("bids")),
                asks=self._parse_levels(arr[0].get("asks")),
                contract_multiplier=mult,
            )

        await asyncio.gather(*(fetch_book(p) for p in pairs[: self.max_pairs_per_market]))

    async def scan_once(self) -> None:
        await self._refresh_ct_vals()
        await self._scan_type(MarketType.SPOT, "SPOT")
        await self._scan_type(MarketType.FUTURES, "SWAP")


class KuCoinDensityScanner(BaseDensityScanner):
    name = "KuCoin"
    max_pairs_per_market = 26

    async def _scan_spot(self) -> None:
        raw = await self._get_json("https://api.kucoin.com/api/v1/market/allTickers")
        if not isinstance(raw, dict):
            return
        pairs = []
        for t in raw.get("data", {}).get("ticker", []):
            sym_raw = t.get("symbol", "")
            if not sym_raw.endswith("-USDT"):
                continue
            clean_sym = sym_raw.replace("-", "")
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if is_blacklisted_symbol(clean_sym) and not is_watched:
                continue
            try:
                px = float(t.get("last") or 0)
                vol = float(t.get("volValue") or 0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((is_watched, vol, sym_raw, clean_sym, px))

        pairs.sort(key=lambda x: (x[0], x[1]), reverse=True)
        sem = asyncio.Semaphore(5)

        async def fetch_book(item: Tuple[bool, float, str, str, float]) -> None:
            _, vol, sym_raw, clean_sym, px = item
            async with sem:
                ob = await self._get_json(f"https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol={sym_raw}")
            if not isinstance(ob, dict):
                return
            d = ob.get("data") or {}
            await self.engine.process_orderbook(
                exchange=self.name,
                market_type=MarketType.SPOT,
                symbol=clean_sym,
                current_price=px,
                volume_24h_usd=vol,
                bids=self._parse_levels(d.get("bids")),
                asks=self._parse_levels(d.get("asks")),
            )

        await asyncio.gather(*(fetch_book(p) for p in pairs[: self.max_pairs_per_market]))

    async def _scan_futures(self) -> None:
        raw = await self._get_json("https://api-futures.kucoin.com/api/v1/contracts/active")
        if not isinstance(raw, dict):
            return
        pairs = []
        for c in raw.get("data", []):
            sym_raw = c.get("symbol", "")
            if not sym_raw.endswith("USDTM"):
                continue
            clean_sym = sym_raw[:-1]
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if is_blacklisted_symbol(clean_sym) and not is_watched:
                continue
            try:
                px = float(c.get("markPrice") or c.get("lastTradePrice") or 0)
                vol = float(c.get("turnoverOf24h") or 0)
                mult = float(c.get("multiplier") or 1.0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((is_watched, vol, sym_raw, clean_sym, px, abs(mult)))

        pairs.sort(key=lambda x: (x[0], x[1]), reverse=True)
        sem = asyncio.Semaphore(5)

        async def fetch_book(item: Tuple[bool, float, str, str, float, float]) -> None:
            _, vol, sym_raw, clean_sym, px, mult = item
            async with sem:
                ob = await self._get_json(f"https://api-futures.kucoin.com/api/v1/level2/depth20?symbol={sym_raw}")
            if not isinstance(ob, dict):
                return
            d = ob.get("data") or {}
            await self.engine.process_orderbook(
                exchange=self.name,
                market_type=MarketType.FUTURES,
                symbol=clean_sym,
                current_price=px,
                volume_24h_usd=vol,
                bids=self._parse_levels(d.get("bids")),
                asks=self._parse_levels(d.get("asks")),
                contract_multiplier=mult,
            )

        await asyncio.gather(*(fetch_book(p) for p in pairs[: self.max_pairs_per_market]))

    async def scan_once(self) -> None:
        await self._scan_spot()
        await self._scan_futures()


class AsterDensityScanner(BaseDensityScanner):
    name = "Aster"
    max_pairs_per_market = 28

    async def scan_once(self) -> None:
        data = await self._get_json("https://fapi.asterdex.com/fapi/v3/ticker/24hr")
        if not isinstance(data, list):
            return
        pairs = []
        for t in data:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            is_watched = extract_base_asset(sym) in self.engine.watched_bases
            if is_blacklisted_symbol(sym) and not is_watched:
                continue
            try:
                px = float(t.get("lastPrice") or 0)
                vol = float(t.get("quoteVolume") or 0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((is_watched, vol, sym, px))

        pairs.sort(key=lambda x: (x[0], x[1]), reverse=True)
        sem = asyncio.Semaphore(6)

        async def fetch_book(item: Tuple[bool, float, str, float]) -> None:
            _, vol, sym, px = item
            async with sem:
                ob = await self._get_json(f"https://fapi.asterdex.com/fapi/v3/depth?symbol={sym}&limit=50")
            if not isinstance(ob, dict):
                return
            await self.engine.process_orderbook(
                exchange=self.name,
                market_type=MarketType.FUTURES,
                symbol=sym,
                current_price=px,
                volume_24h_usd=vol,
                bids=self._parse_levels(ob.get("bids")),
                asks=self._parse_levels(ob.get("asks")),
            )

        await asyncio.gather(*(fetch_book(p) for p in pairs[: self.max_pairs_per_market]))


class GateDensityScanner(BaseDensityScanner):
    name = "Gate"
    max_pairs_per_market = 26

    async def _scan_spot(self) -> None:
        data = await self._get_json("https://api.gateio.ws/api/v4/spot/tickers")
        if not isinstance(data, list):
            return
        pairs = []
        for t in data:
            pair_raw = t.get("currency_pair", "")
            if not pair_raw.endswith("_USDT"):
                continue
            clean_sym = pair_raw.replace("_", "")
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if is_blacklisted_symbol(clean_sym) and not is_watched:
                continue
            try:
                px = float(t.get("last") or 0)
                vol = float(t.get("quote_volume") or 0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((is_watched, vol, pair_raw, clean_sym, px))

        pairs.sort(key=lambda x: (x[0], x[1]), reverse=True)
        sem = asyncio.Semaphore(5)

        async def fetch_book(item: Tuple[bool, float, str, str, float]) -> None:
            _, vol, pair_raw, clean_sym, px = item
            async with sem:
                ob = await self._get_json(
                    f"https://api.gateio.ws/api/v4/spot/order_book?currency_pair={pair_raw}&limit=50"
                )
            if not isinstance(ob, dict):
                return
            await self.engine.process_orderbook(
                exchange=self.name,
                market_type=MarketType.SPOT,
                symbol=clean_sym,
                current_price=px,
                volume_24h_usd=vol,
                bids=self._parse_levels(ob.get("bids")),
                asks=self._parse_levels(ob.get("asks")),
            )

        await asyncio.gather(*(fetch_book(p) for p in pairs[: self.max_pairs_per_market]))

    async def _scan_futures(self) -> None:
        data = await self._get_json("https://api.gateio.ws/api/v4/futures/usdt/contracts")
        if not isinstance(data, list):
            return
        pairs = []
        for c in data:
            name_raw = c.get("name", "")
            if not name_raw.endswith("_USDT"):
                continue
            clean_sym = name_raw.replace("_", "")
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if is_blacklisted_symbol(clean_sym) and not is_watched:
                continue
            try:
                px = float(c.get("last_price") or c.get("mark_price") or 0)
                mult = float(c.get("quanto_multiplier") or 1.0)
                vol = float(c.get("trade_size") or 0) * px * mult
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((is_watched, vol, name_raw, clean_sym, px, mult))

        pairs.sort(key=lambda x: (x[0], x[1]), reverse=True)
        sem = asyncio.Semaphore(5)

        async def fetch_book(item: Tuple[bool, float, str, str, float, float]) -> None:
            _, vol, name_raw, clean_sym, px, mult = item
            async with sem:
                ob = await self._get_json(
                    f"https://api.gateio.ws/api/v4/futures/usdt/order_book?contract={name_raw}&limit=50"
                )
            if not isinstance(ob, dict):
                return
            await self.engine.process_orderbook(
                exchange=self.name,
                market_type=MarketType.FUTURES,
                symbol=clean_sym,
                current_price=px,
                volume_24h_usd=vol,
                bids=self._parse_levels(ob.get("bids")),
                asks=self._parse_levels(ob.get("asks")),
                contract_multiplier=mult,
            )

        await asyncio.gather(*(fetch_book(p) for p in pairs[: self.max_pairs_per_market]))

    async def scan_once(self) -> None:
        await self._scan_spot()
        await self._scan_futures()


class BitgetDensityScanner(BaseDensityScanner):
    name = "Bitget"
    max_pairs_per_market = 26

    async def _scan_spot(self) -> None:
        raw = await self._get_json("https://api.bitget.com/api/v2/spot/market/tickers")
        if not isinstance(raw, dict):
            return
        pairs = []
        for t in raw.get("data", []):
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            is_watched = extract_base_asset(sym) in self.engine.watched_bases
            if is_blacklisted_symbol(sym) and not is_watched:
                continue
            try:
                px = float(t.get("lastPr") or 0)
                vol = float(t.get("usdtVolume") or 0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((is_watched, vol, sym, px))

        pairs.sort(key=lambda x: (x[0], x[1]), reverse=True)
        sem = asyncio.Semaphore(5)

        async def fetch_book(item: Tuple[bool, float, str, float]) -> None:
            _, vol, sym, px = item
            async with sem:
                ob = await self._get_json(
                    f"https://api.bitget.com/api/v2/spot/market/orderbook?symbol={sym}&type=step0&limit=50"
                )
            if not isinstance(ob, dict):
                return
            d = ob.get("data") or {}
            await self.engine.process_orderbook(
                exchange=self.name,
                market_type=MarketType.SPOT,
                symbol=sym,
                current_price=px,
                volume_24h_usd=vol,
                bids=self._parse_levels(d.get("bids")),
                asks=self._parse_levels(d.get("asks")),
            )

        await asyncio.gather(*(fetch_book(p) for p in pairs[: self.max_pairs_per_market]))

    async def _scan_futures(self) -> None:
        raw = await self._get_json(
            "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
        )
        if not isinstance(raw, dict):
            return
        pairs = []
        for t in raw.get("data", []):
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            is_watched = extract_base_asset(sym) in self.engine.watched_bases
            if is_blacklisted_symbol(sym) and not is_watched:
                continue
            try:
                px = float(t.get("lastPr") or 0)
                vol = float(t.get("usdtVolume") or 0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((is_watched, vol, sym, px))

        pairs.sort(key=lambda x: (x[0], x[1]), reverse=True)
        sem = asyncio.Semaphore(5)

        async def fetch_book(item: Tuple[bool, float, str, float]) -> None:
            _, vol, sym, px = item
            async with sem:
                ob = await self._get_json(
                    f"https://api.bitget.com/api/v2/mix/market/merge-depth?productType=USDT-FUTURES&symbol={sym}&limit=50"
                )
            if not isinstance(ob, dict):
                return
            d = ob.get("data") or {}
            await self.engine.process_orderbook(
                exchange=self.name,
                market_type=MarketType.FUTURES,
                symbol=sym,
                current_price=px,
                volume_24h_usd=vol,
                bids=self._parse_levels(d.get("bids")),
                asks=self._parse_levels(d.get("asks")),
            )

        await asyncio.gather(*(fetch_book(p) for p in pairs[: self.max_pairs_per_market]))

    async def scan_once(self) -> None:
        await self._scan_spot()
        await self._scan_futures()


# ==================== TELEGRAM БОТ И РАССЫЛКА ====================

bot: Optional[Bot] = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()


def _build_main_keyboard() -> Optional[InlineKeyboardMarkup]:
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Открыть Аптик & Плотности", web_app=WebAppInfo(url=WEBAPP_URL))]
        ]
    )


async def _broadcast_messages(user_ids: List[int], text: str, kb: Optional[InlineKeyboardMarkup]) -> None:
    if not bot or not user_ids:
        return
    for uid in user_ids:
        try:
            await bot.send_message(uid, text, parse_mode="HTML", reply_markup=kb)
            await asyncio.sleep(0.04)  # мягкий лимит (~25 msg/sec)
        except Exception:
            continue


async def handle_uptick_event(event: UptickEvent) -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await db_execute(
        "INSERT INTO upticks (utc_date, utc_time, exchange, symbol, side, price, limit_price, volume_24h_usd) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            today_utc,
            event.timestamp_utc,
            event.exchange,
            event.symbol,
            event.side.value,
            event.price,
            event.limit_price,
            event.volume_24h_usd,
        ),
    )

    emoji = "🟢" if event.side == Side.LONG else "🔴"
    text = (
        f"⚡ <b>АПТИК | {event.exchange}</b>\n"
        f"Монета: <code>{event.symbol}</code>\n"
        f"{emoji} Сторона: <b>{event.side.value}</b>\n"
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


async def handle_density_event(event: DensityEvent) -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await db_execute(
        "INSERT INTO densities "
        "(utc_date, utc_time, exchange, market_type, symbol, side, price, size_usd, distance_pct, volume_24h_usd) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            today_utc,
            event.timestamp_utc,
            event.exchange,
            event.market_type.value,
            event.symbol,
            event.side.value,
            event.price,
            event.size_usd,
            event.distance_pct,
            event.volume_24h_usd,
        ),
    )

    mkt_ru = "СПОТ" if event.market_type == MarketType.SPOT else "ФЬЮЧЕРС"
    emoji = "🟢" if event.side == Side.LONG else "🔴"
    text = (
        f"🧱 <b>ПЛОТНОСТЬ | {event.exchange} ({mkt_ru})</b>\n"
        f"Монета: <code>{event.symbol}</code>\n"
        f"{emoji} Сторона: <b>{event.side.value}</b>\n"
        f"Размер плотности: <b>${event.size_usd:,.0f}</b>\n"
        f"Цена плотности: <code>{event.price:g}</code> (до цены: {event.distance_pct:.2f}%)\n"
        f"Оборот 24ч: ${event.volume_24h_usd:,.0f}\n"
        f"Время (UTC): {event.timestamp_utc}"
    )

    users = await db_fetchall(
        "SELECT user_id, min_density_spot, min_density_futures, coin_rules, banned_coins FROM users"
    )
    base = extract_base_asset(event.symbol)
    globally_blocked = is_blacklisted_symbol(event.symbol)
    target_ids: List[int] = []
    for u in users:
        banned_map = parse_banned_coins(u.get("banned_coins"))
        if is_coin_banned(base, banned_map, "DENSITY"):
            continue
        u_spot = float(u.get("min_density_spot") or DEFAULT_MIN_DENSITY_SPOT)
        u_fut = float(u.get("min_density_futures") or DEFAULT_MIN_DENSITY_FUTURES)
        u_rules = parse_coin_rules(u.get("coin_rules"))
        if globally_blocked and base not in u_rules:
            continue
        threshold = get_user_density_threshold(
            event.symbol, event.market_type.value, u_spot, u_fut, u_rules
        )
        if event.size_usd >= threshold:
            target_ids.append(int(u["user_id"]))

    await _broadcast_messages(target_ids, text, _build_main_keyboard())


engine = UptickEngine(on_uptick=handle_uptick_event)
density_engine = DensityEngine(on_density=handle_density_event)


# ==================== КОМАНДЫ TELEGRAM БОТА ====================

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    await db_execute(
        "INSERT INTO users (user_id) VALUES (?) ON CONFLICT(user_id) DO NOTHING",
        (message.from_user.id,),
    )
    warn = "" if WEBAPP_URL else "\n\n⚠️ Переменная <code>WEBAPP_URL</code> еще не задана."
    text = (
        "👋 <b>Аптик + Сканер плотностей запущен!</b>\n\n"
        "⚡ <b>Аптики (4):</b> Binance, Bybit, OKX, Aster.\n"
        "🧱 <b>Плотности (7):</b> Binance, Bybit, OKX, KuCoin, Aster, Gate, Bitget.\n"
        "🐯 <b>Tiger.Trade:</b> открытие по клику или автооткрытие в Mini App.\n"
        "🚫 Исключены: тяжеловесы (BTC, ETH, SOL, PEPE...), все акции/ETF (bStocks, xStocks, Ondo), металлы, нефть, газ и Forex.\n\n"
        "<b>Команды фильтров:</b>\n"
        "• <code>/vol 500000</code> — мин. оборот 24ч ($) для аптиков\n"
        "• <code>/dspot 250000</code> — мин. плотность на Споте ($)\n"
        "• <code>/dfut 1000000</code> — мин. плотность на Фьючерсах ($)\n"
        "• <code>/coin WIF 100000 500000</code> — свой порог плотности для любой монеты\n"
        "• <code>/delcoin WIF</code> — удалить свой порог монеты\n"
        "• <code>/ban MOODENG [all|uptick|density]</code> — убрать монету\n"
        "• <code>/unban MOODENG</code> — вернуть монету\n"
        "• <code>/status</code> — активные аптики и плотности сейчас"
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


@dp.message(Command("dspot"))
async def cmd_dspot(message: types.Message) -> None:
    try:
        val = max(DENSITY_HARD_MIN_SPOT, float((message.text or "").split()[1].replace(",", "").replace("$", "")))
        await db_execute(
            "INSERT INTO users (user_id, min_density_spot) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET min_density_spot = excluded.min_density_spot",
            (message.from_user.id, val),
        )
        await density_engine.refresh_watched_bases()
        await message.answer(f"✅ Мин. плотность на Споте: <b>${val:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/dspot 250000</code>", parse_mode="HTML")


@dp.message(Command("dfut"))
async def cmd_dfut(message: types.Message) -> None:
    try:
        val = max(DENSITY_HARD_MIN_FUTURES, float((message.text or "").split()[1].replace(",", "").replace("$", "")))
        await db_execute(
            "INSERT INTO users (user_id, min_density_futures) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET min_density_futures = excluded.min_density_futures",
            (message.from_user.id, val),
        )
        await density_engine.refresh_watched_bases()
        await message.answer(f"✅ Мин. плотность на Фьючерсах: <b>${val:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/dfut 1000000</code>", parse_mode="HTML")


@dp.message(Command("coin"))
async def cmd_coin(message: types.Message) -> None:
    try:
        parts = (message.text or "").split()
        if len(parts) < 3:
            raise ValueError()
        base = extract_base_asset(parts[1])
        if not base:
            await message.answer("❌ Укажите корректный тикер монеты (например BTC, WIF, PEPE).")
            return
        spot_val = max(DENSITY_HARD_MIN_SPOT, float(parts[2].replace(",", "").replace("$", "")))
        fut_val = (
            max(DENSITY_HARD_MIN_FUTURES, float(parts[3].replace(",", "").replace("$", "")))
            if len(parts) >= 4
            else max(DENSITY_HARD_MIN_FUTURES, spot_val)
        )
        row = await db_fetchone("SELECT coin_rules FROM users WHERE user_id = ?", (message.from_user.id,))
        rules = parse_coin_rules(row.get("coin_rules") if row else None)
        rules[base] = {"spot": spot_val, "futures": fut_val}
        await db_execute(
            "INSERT INTO users (user_id, coin_rules) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET coin_rules = excluded.coin_rules",
            (message.from_user.id, json.dumps(rules)),
        )
        await density_engine.refresh_watched_bases()
        await message.answer(
            f"🎯 Правило для <b>{base}</b> сохранено:\nСпот от <b>${spot_val:,.0f}</b> | Фьючерс от <b>${fut_val:,.0f}</b>",
            parse_mode="HTML",
        )
    except Exception:
        await message.answer("Использование: <code>/coin WIF 100000 500000</code>", parse_mode="HTML")


@dp.message(Command("delcoin"))
async def cmd_delcoin(message: types.Message) -> None:
    try:
        base = extract_base_asset((message.text or "").split()[1])
        row = await db_fetchone("SELECT coin_rules FROM users WHERE user_id = ?", (message.from_user.id,))
        rules = parse_coin_rules(row.get("coin_rules") if row else None)
        if base in rules:
            rules.pop(base, None)
            await db_execute("UPDATE users SET coin_rules = ? WHERE user_id = ?", (json.dumps(rules), message.from_user.id))
            await density_engine.refresh_watched_bases()
            await message.answer(f"🗑 Правило для <b>{base}</b> удалено.", parse_mode="HTML")
        else:
            await message.answer(f"Для <b>{base}</b> не было отдельного правила.", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/delcoin WIF</code>", parse_mode="HTML")


@dp.message(Command("ban"))
async def cmd_ban(message: types.Message) -> None:
    try:
        parts = (message.text or "").split()
        base = extract_base_asset(parts[1])
        if not base:
            raise ValueError()
        scope = normalize_ban_scope(parts[2] if len(parts) >= 3 else "ALL")
        row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (message.from_user.id,))
        banned = parse_banned_coins(row.get("banned_coins") if row else None)
        banned[base] = scope
        await db_execute(
            "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
            (message.from_user.id, json.dumps(banned)),
        )
        scope_ru = {"ALL": "Аптики + Плотности", "UPTICK": "только Аптики", "DENSITY": "только Плотности"}[scope]
        await message.answer(f"🚫 Монета <b>{base}</b> скрыта ({scope_ru}).", parse_mode="HTML")
    except Exception:
        await message.answer(
            "Использование:\n"
            "• <code>/ban MOODENG</code> — скрыть везде\n"
            "• <code>/ban MOODENG density</code> — только из плотностей\n"
            "• <code>/ban MOODENG uptick</code> — только из аптиков",
            parse_mode="HTML",
        )


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
    densities = density_engine.get_active_densities()[:10]
    lines = ["📊 <b>Текущая ситуация на рынке:</b>\n"]
    if upticks:
        lines.append("⚡ <b>Активные аптики:</b>")
        for ex, sym, side in upticks[:10]:
            lines.append(f"• {ex} | <code>{sym}</code> — <b>{side.value}</b>")
    else:
        lines.append("⚡ Активных аптиков сейчас нет.")

    lines.append("")
    if densities:
        lines.append("🧱 <b>Крупные плотности в стаканах:</b>")
        for d in densities:
            mkt = "SPOT" if d.market_type == MarketType.SPOT else "FUT"
            lines.append(
                f"• {d.exchange} ({mkt}) | <code>{d.symbol}</code> {d.side.value} — "
                f"<b>${d.size_usd:,.0f}</b> @ {d.price:g} ({d.distance_pct:.2f}%)"
            )
    else:
        lines.append("🧱 Крупных плотностей сейчас нет.")

    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=_build_main_keyboard())


# ==================== TELEGRAM MINI APP HTML ====================

WEBAPP_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Аптик & Плотности</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        :root {
            --bg: #0f141b;
            --card: #18202c;
            --border: #263244;
            --text: #eef3f9;
            --hint: #8c9bb0;
            --long: #16c784;
            --short: #ea3943;
            --accent: #3b82f6;
            --gold: #f59e0b;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        body { background: var(--bg); color: var(--text); padding: 12px 12px 28px; -webkit-tap-highlight-color: transparent; }

        .topbar {
            display: flex; align-items: center; justify-content: space-between;
            gap: 8px; margin-bottom: 12px;
        }
        .brand { display: flex; align-items: center; gap: 8px; font-weight: 800; font-size: 16px; }
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

        .tabs {
            display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px;
            margin-bottom: 12px; background: var(--card); padding: 4px;
            border-radius: 12px; border: 1px solid var(--border);
        }
        .tab-btn {
            border: none; background: transparent; color: var(--hint);
            padding: 8px 4px; border-radius: 9px; font-size: 12px; font-weight: 700;
            cursor: pointer; transition: 0.15s;
        }
        .tab-btn.active { background: var(--accent); color: #fff; }

        .panel { display: none; }
        .panel.active { display: block; }

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

        .live-grid {
            display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 12px;
        }
        @media (max-width: 480px) { .live-grid { grid-template-columns: 1fr; } }
        .live-box {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 12px; padding: 10px;
        }
        .live-box-title {
            font-size: 12px; font-weight: 800; color: var(--hint);
            margin-bottom: 8px; display: flex; justify-content: space-between;
        }

        .card {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 12px; padding: 10px 12px; margin-bottom: 8px;
            cursor: pointer; transition: transform 0.1s, border-color 0.15s;
            position: relative;
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
            background: rgba(255,255,255,0.08); color: var(--hint);
        }
        .mkt-tag.spot { background: rgba(245,158,11,0.18); color: var(--gold); }
        .mkt-tag.fut { background: rgba(59,130,246,0.18); color: #7cb3ff; }

        .card-right { display: flex; align-items: center; gap: 6px; }
        .side-badge {
            font-size: 11px; font-weight: 800; padding: 3px 8px; border-radius: 7px;
        }
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
        .empty { text-align: center; padding: 20px 10px; color: var(--hint); font-size: 13px; }

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

        .rule-item {
            display: flex; align-items: center; justify-content: space-between;
            background: var(--bg); border: 1px solid var(--border);
            border-radius: 9px; padding: 8px 10px; margin-top: 6px; font-size: 12px;
        }
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
        .banned-scope {
            font-size: 10px; font-weight: 700; padding: 2px 5px; border-radius: 6px;
            background: rgba(255,255,255,0.1); color: var(--hint);
        }

        .modal-backdrop {
            position: fixed; inset: 0; background: rgba(0,0,0,0.65);
            display: none; align-items: flex-end; justify-content: center;
            z-index: 120; padding: 14px;
        }
        .modal-backdrop.open { display: flex; }
        .modal-sheet {
            width: 100%; max-width: 440px; background: var(--card);
            border: 1px solid var(--border); border-radius: 16px; padding: 16px;
        }
        .modal-title { font-size: 15px; font-weight: 800; margin-bottom: 6px; }
        .modal-actions { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; }
        .modal-btn {
            width: 100%; padding: 11px 12px; border-radius: 10px; font-size: 13px;
            font-weight: 700; cursor: pointer; border: 1px solid var(--border);
            background: var(--bg); color: var(--text); text-align: left;
        }
        .modal-btn.danger { background: rgba(234,57,67,0.16); color: var(--short); border-color: rgba(234,57,67,0.45); }
        .modal-btn.cancel { text-align: center; color: var(--hint); background: transparent; }

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
            <span>Аптик & Плотности</span>
        </div>
        <div class="top-actions">
            <button class="tiger-pill" id="tiger-pill" onclick="connectTiger(true)">🐯 Tiger: OFF</button>
        </div>
    </div>

    <div class="tabs">
        <button class="tab-btn active" data-tab="live" onclick="switchTab('live')">🔴 Лайв</button>
        <button class="tab-btn" data-tab="upticks" onclick="switchTab('upticks')">⚡ Аптики</button>
        <button class="tab-btn" data-tab="densities" onclick="switchTab('densities')">🧱 Плотн.</button>
        <button class="tab-btn" data-tab="settings" onclick="switchTab('settings')">⚙️ Настр.</button>
    </div>

    <!-- ВКЛАДКА ЛАЙВ -->
    <div id="tab-live" class="panel active">
        <div class="live-grid">
            <div class="live-box">
                <div class="live-box-title"><span>⚡ Аптики сейчас</span><span id="live-up-cnt">0</span></div>
                <div id="live-active-upticks"></div>
            </div>
            <div class="live-box">
                <div class="live-box-title"><span>🧱 Плотности в стакане</span><span id="live-dn-cnt">0</span></div>
                <div id="live-active-densities"></div>
            </div>
        </div>
        <div class="filter-row">
            <button class="chip active" data-livef="ALL" onclick="setLiveFilter('ALL')">Все вместе</button>
            <button class="chip" data-livef="UPTICK" onclick="setLiveFilter('UPTICK')">⚡ Только Аптики</button>
            <button class="chip" data-livef="DENSITY" onclick="setLiveFilter('DENSITY')">🧱 Только Плотности</button>
        </div>
        <div id="live-feed"></div>
    </div>

    <!-- ВКЛАДКА АПТИКИ -->
    <div id="tab-upticks" class="panel">
        <div class="filter-row" id="uptick-ex-filters"></div>
        <div id="upticks-list"></div>
    </div>

    <!-- ВКЛАДКА ПЛОТНОСТИ -->
    <div id="tab-densities" class="panel">
        <div class="filter-row">
            <button class="chip active" data-mkt="ALL" onclick="setDensityMarket('ALL')">Все рынки</button>
            <button class="chip" data-mkt="SPOT" onclick="setDensityMarket('SPOT')">Спот</button>
            <button class="chip" data-mkt="FUTURES" onclick="setDensityMarket('FUTURES')">Фьючерсы</button>
        </div>
        <div class="filter-row" id="density-ex-filters"></div>
        <div id="densities-list"></div>
    </div>

    <!-- ВКЛАДКА НАСТРОЙКИ -->
    <div id="tab-settings" class="panel">
        <!-- Свой порог плотности для любой монеты -->
        <div class="setting-box">
            <label class="setting-label">🎯 Свой порог плотности для любой монеты</label>
            <div class="meta" style="margin-bottom:10px;">
                Можно добавить <b>абсолютно любую монету</b> (включая BTC, ETH, SOL, PEPE, WIF и др.) и задать свой порог плотности ($).
            </div>
            <div class="input-row">
                <input type="text" id="rule-symbol" placeholder="Монета (напр. WIF, BTC, PEPE)">
            </div>
            <div class="input-row">
                <input type="number" id="rule-spot" placeholder="Спот от $" value="100000">
                <input type="number" id="rule-fut" placeholder="Фьючерс от $" value="500000">
                <button class="btn" onclick="saveCoinRule()">Добавить</button>
            </div>
            <div id="coin-rules-list"></div>
        </div>

        <!-- Личный черный список -->
        <div class="setting-box">
            <label class="setting-label">🚫 Убрать монеты (личный черный список)</label>
            <div class="meta" style="margin-bottom:10px;">
                Выберите, откуда убрать монету: только из Аптиков, только из Плотностей или из обоих сразу (или нажмите ✕ на карточке).
            </div>
            <div class="input-row">
                <input type="text" id="ban-coin-input" placeholder="Тикер (напр. MOODENG, TRUMP, DOGS)">
            </div>
            <div class="input-row">
                <select id="ban-scope-select">
                    <option value="ALL">🚫 Убрать из обоих (Аптики + Плотности)</option>
                    <option value="DENSITY">🧱 Только из Плотностей</option>
                    <option value="UPTICK">⚡ Только из Аптиков</option>
                </select>
                <button class="danger-btn" onclick="banCoinFromInput()">Убрать</button>
            </div>
            <div class="banned-tags" id="banned-coins-list"></div>
        </div>

        <!-- Общие фильтры -->
        <div class="setting-box">
            <label class="setting-label">💰 Общие пороги фильтров ($)</label>
            <div class="meta" style="margin-bottom:6px;">Мин. оборот 24ч для Аптиков ($)</div>
            <div class="input-row"><input type="number" id="set-vol" value="500000"></div>
            <div class="meta" style="margin-bottom:6px;">Мин. плотность на Споте ($)</div>
            <div class="input-row"><input type="number" id="set-dspot" value="250000"></div>
            <div class="meta" style="margin-bottom:6px;">Мин. плотность на Фьючерсах ($)</div>
            <div class="input-row"><input type="number" id="set-dfut" value="1000000"></div>
            <button class="btn" style="width:100%" onclick="saveGlobalSettings()">Сохранить фильтры</button>
        </div>

        <!-- Звук и голос -->
        <div class="setting-box">
            <label class="setting-label">🔊 Звук и голосовая озвучка (LONG / SHORT)</label>
            <div class="toggle-row">
                <span>Озвучивать новые Аптики</span>
                <input type="checkbox" id="snd-uptick" checked onchange="saveLocalPrefs()">
            </div>
            <div class="toggle-row">
                <span>Озвучивать новые Плотности</span>
                <input type="checkbox" id="snd-density" checked onchange="saveLocalPrefs()">
            </div>
            <div class="input-row" style="margin-top:8px;">
                <button class="btn secondary" style="flex:1" onclick="testVoice('uptick')">▶ Тест Аптика</button>
                <button class="btn secondary" style="flex:1" onclick="testVoice('density')">▶ Тест Плотности</button>
            </div>
        </div>

        <!-- Tiger.Trade -->
        <div class="setting-box">
            <label class="setting-label">🐯 Терминал Tiger.Trade (Local Signal Server :7819)</label>
            <div class="meta" style="margin-bottom:8px;">
                В терминале: <b>Настройки → Локальный сервер сигналов → Включить (порт 7819)</b>. Клик по любой карточке откроет монету в стакане.
            </div>
            <div class="toggle-row">
                <span>Автооткрытие: Аптики</span>
                <input type="checkbox" id="tiger-auto-up" onchange="saveLocalPrefs()">
            </div>
            <div class="toggle-row">
                <span>Автооткрытие: Плотности</span>
                <input type="checkbox" id="tiger-auto-dn" onchange="saveLocalPrefs()">
            </div>
            <div class="toggle-row">
                <span>Режим TigerX Broker</span>
                <input type="checkbox" id="tiger-broker" onchange="saveLocalPrefs()">
            </div>
            <div class="input-row" style="margin-top:8px;">
                <select id="tiger-group" onchange="saveLocalPrefs()"></select>
                <button class="btn secondary" onclick="openInTiger('Binance','FUTURES','PEPEUSDT')">▶ Тест: PEPEUSDT</button>
            </div>
        </div>
    </div>

    <!-- Модалка скрытия монеты -->
    <div class="modal-backdrop" id="ban-modal" onclick="closeBanModal(event)">
        <div class="modal-sheet" onclick="event.stopPropagation()">
            <div class="modal-title" id="ban-modal-title">🚫 Убрать монету</div>
            <div class="meta">Выберите, откуда скрыть эту монету:</div>
            <div class="modal-actions">
                <button class="modal-btn" onclick="submitBanModal('DENSITY')">🧱 Только из Плотностей</button>
                <button class="modal-btn" onclick="submitBanModal('UPTICK')">⚡ Только из Аптиков</button>
                <button class="modal-btn danger" onclick="submitBanModal('ALL')">🚫 Из обоих (Аптики + Плотности)</button>
                <button class="modal-btn cancel" onclick="closeBanModal()">Отмена</button>
            </div>
        </div>
    </div>

    <div class="toast" id="toast">Сохранено</div>

    <script>
        const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
        if (tg) { tg.ready(); tg.expand(); }
        const userId = tg && tg.initDataUnsafe && tg.initDataUnsafe.user ? tg.initDataUnsafe.user.id : 0;

        const UPTICK_EXCHANGES = ['Binance', 'Bybit', 'OKX', 'Aster'];
        const DENSITY_EXCHANGES = ['Binance', 'Bybit', 'OKX', 'KuCoin', 'Aster', 'Gate', 'Bitget'];

        const EX_VOICE = {
            'Binance': 'Бинанс',
            'Bybit': 'Байбит',
            'OKX': 'ОКИКС',
            'KuCoin': 'Кукоин',
            'Aster': 'Астер',
            'Gate': 'Гейт',
            'Bitget': 'Битгет'
        };

        const EX_SVG = {
            'Binance': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#F3BA2F"/><path d="M16 7l3.5 3.5-3.5 3.5-3.5-3.5L16 7zm-5.5 5.5L14 16l-3.5 3.5L7 16l3.5-3.5zm11 0L25 16l-3.5 3.5L18 16l3.5-3.5zM16 18l3.5 3.5L16 25l-3.5-3.5L16 18z" fill="#111"/></svg>',
            'Bybit': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#121418"/><text x="16" y="21" text-anchor="middle" font-size="12" font-weight="900" fill="#F7A600">BYB</text></svg>',
            'OKX': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#000"/><rect x="7" y="7" width="6" height="6" fill="#fff"/><rect x="19" y="7" width="6" height="6" fill="#fff"/><rect x="13" y="13" width="6" height="6" fill="#fff"/><rect x="7" y="19" width="6" height="6" fill="#fff"/><rect x="19" y="19" width="6" height="6" fill="#fff"/></svg>',
            'KuCoin': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#23AF91"/><path d="M11 9v14M11 16l7-7M11 16l7 7M20 16h2" stroke="#fff" stroke-width="2.8" stroke-linecap="round"/></svg>',
            'Aster': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#6366F1"/><path d="M16 6l2.8 7.2L26 16l-7.2 2.8L16 26l-2.8-7.2L6 16l7.2-2.8L16 6z" fill="#fff"/></svg>',
            'Gate': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#2354E6"/><path d="M22 16a6 6 0 1 1-6-6v6h6z" fill="#17E6A1"/></svg>',
            'Bitget': '<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#00F0FF"/><path d="M11 10h7l4 6-4 6h-7l4-6-4-6z" fill="#0B1220"/></svg>'
        };

        let activeTab = 'live';
        let liveFilter = 'ALL';
        let uptickExFilter = 'ALL';
        let densityExFilter = 'ALL';
        let densityMarketFilter = 'ALL';

        let cachedHistory = [];
        let cachedDensities = [];
        let cachedActiveUp = [];
        let cachedActiveDn = [];
        let userCoinRules = {};
        let userBannedCoins = {};
        let pendingBanSymbol = '';
        let settingsInitialized = false;
        let lastUptickId = 0;
        let lastDensityId = 0;
        let firstLoadDone = false;

        // ===== TIGER.TRADE WEBSOCKET =====
        let tigerWs = null;
        let tigerOnline = false;

        function setTigerPill(ok) {
            tigerOnline = ok;
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
                tigerWs.onopen = () => {
                    setTigerPill(true);
                    if (manual) showToast('🐯 Tiger.Trade подключен');
                };
                tigerWs.onclose = () => setTigerPill(false);
                tigerWs.onerror = () => setTigerPill(false);
            } catch {
                setTigerPill(false);
            }
        }

        function mapTigerExchange(ex, mkt, useBroker) {
            if (useBroker) {
                if (ex === 'Binance') return 'TigerX Binance';
                if (ex === 'Bybit') return 'TigerX Bybit';
                if (ex === 'OKX') return 'TigerX OKX';
            }
            if (ex === 'Binance') return mkt === 'FUTURES' ? 'BINANCE-FUT' : 'BINANCE';
            if (ex === 'Bybit') return 'BYBIT';
            if (ex === 'OKX') return 'OKX';
            if (ex === 'Gate') return 'Gate.io';
            if (ex === 'Bitget') return 'Bitget';
            return ex.toUpperCase();
        }

        function openInTiger(exchange, marketType, symbol) {
            if (!tigerWs || tigerWs.readyState !== WebSocket.OPEN) {
                connectTiger(false);
                showToast('🐯 Нет связи с Tiger.Trade (порт 7819)');
                return;
            }
            const group = document.getElementById('tiger-group').value || 'A';
            const useBroker = document.getElementById('tiger-broker').checked;
            const payload = {
                type: 'setLinkSymbol',
                e: mapTigerExchange(exchange, marketType, useBroker),
                m: marketType === 'SPOT' ? 'SPOT' : 'FUTURES',
                symbol: symbol,
                linkGroup: group
            };
            tigerWs.send(JSON.stringify(payload));
            showToast(`🐯 ${symbol} (${exchange}) → группа ${group}`);
        }

        // ===== ЗВУК И ГОЛОС =====
        function playTone(freq, dur = 0.16) {
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
            try {
                if (!('speechSynthesis' in window)) return;
                window.speechSynthesis.cancel();
                const u = new SpeechSynthesisUtterance(text);
                u.lang = 'ru-RU';
                u.rate = 1.08;
                window.speechSynthesis.speak(u);
            } catch {}
        }

        function announceUptick(ev) {
            if (!document.getElementById('snd-uptick').checked) return;
            playTone(880, 0.18);
            const exRu = EX_VOICE[ev.exchange] || ev.exchange;
            const sideRu = ev.side === 'LONG' ? 'лонг' : 'шорт';
            const symClean = (ev.symbol || '').replace('USDT', '');
            speakRu(`Аптик на бирже ${exRu}. ${symClean}. ${sideRu}`);
        }

        function announceDensity(ev) {
            if (!document.getElementById('snd-density').checked) return;
            playTone(620, 0.16);
            const exRu = EX_VOICE[ev.exchange] || ev.exchange;
            const mktRu = ev.market_type === 'SPOT' ? 'спот' : 'фьючерс';
            const sideRu = ev.side === 'LONG' ? 'лонг' : 'шорт';
            const symClean = (ev.symbol || '').replace('USDT', '');
            speakRu(`Плотность на бирже ${exRu}, ${mktRu}. ${symClean}. ${sideRu}`);
        }

        function testVoice(kind) {
            if (kind === 'uptick') announceUptick({ exchange: 'Binance', symbol: 'WIFUSDT', side: 'LONG' });
            else announceDensity({ exchange: 'Bybit', market_type: 'SPOT', symbol: 'WIFUSDT', side: 'SHORT' });
        }

        // ===== ЛОКАЛЬНЫЕ НАСТРОЙКИ =====
        function initTigerGroups() {
            const sel = document.getElementById('tiger-group');
            const items = [...'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split(''), ...Array.from({length: 12}, (_, i) => String(i))];
            sel.innerHTML = items.map(g => `<option value="${g}">Группа ${g}</option>`).join('');
        }

        function loadLocalPrefs() {
            try {
                const p = JSON.parse(localStorage.getItem('uptick_prefs_v2') || '{}');
                if (typeof p.sndUp === 'boolean') document.getElementById('snd-uptick').checked = p.sndUp;
                if (typeof p.sndDn === 'boolean') document.getElementById('snd-density').checked = p.sndDn;
                if (typeof p.autoUp === 'boolean') document.getElementById('tiger-auto-up').checked = p.autoUp;
                if (typeof p.autoDn === 'boolean') document.getElementById('tiger-auto-dn').checked = p.autoDn;
                if (typeof p.broker === 'boolean') document.getElementById('tiger-broker').checked = p.broker;
                if (p.group) document.getElementById('tiger-group').value = p.group;
            } catch {}
        }

        function saveLocalPrefs() {
            const p = {
                sndUp: document.getElementById('snd-uptick').checked,
                sndDn: document.getElementById('snd-density').checked,
                autoUp: document.getElementById('tiger-auto-up').checked,
                autoDn: document.getElementById('tiger-auto-dn').checked,
                broker: document.getElementById('tiger-broker').checked,
                group: document.getElementById('tiger-group').value
            };
            localStorage.setItem('uptick_prefs_v2', JSON.stringify(p));
        }

        // ===== ФИЛЬТРЫ И ВКЛАДКИ =====
        function switchTab(tab) {
            activeTab = tab;
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
            document.querySelectorAll('.panel').forEach(p => p.classList.toggle('active', p.id === `tab-${tab}`));
        }

        function setLiveFilter(f) {
            liveFilter = f;
            document.querySelectorAll('[data-livef]').forEach(b => b.classList.toggle('active', b.dataset.livef === f));
            renderAll();
        }

        function setDensityMarket(m) {
            densityMarketFilter = m;
            document.querySelectorAll('[data-mkt]').forEach(b => b.classList.toggle('active', b.dataset.mkt === m));
            renderAll();
        }

        function buildExchangeFilters() {
            const upBox = document.getElementById('uptick-ex-filters');
            upBox.innerHTML = `<button class="chip active" data-uex="ALL" onclick="setUptickEx('ALL')">Все (4)</button>` +
                UPTICK_EXCHANGES.map(ex => `<button class="chip" data-uex="${ex}" onclick="setUptickEx('${ex}')"><span class="ex-logo" style="width:16px;height:16px">${EX_SVG[ex]||''}</span>${ex}</button>`).join('');

            const dnBox = document.getElementById('density-ex-filters');
            dnBox.innerHTML = `<button class="chip active" data-dex="ALL" onclick="setDensityEx('ALL')">Все (7)</button>` +
                DENSITY_EXCHANGES.map(ex => `<button class="chip" data-dex="${ex}" onclick="setDensityEx('${ex}')"><span class="ex-logo" style="width:16px;height:16px">${EX_SVG[ex]||''}</span>${ex}</button>`).join('');
        }

        function setUptickEx(ex) {
            uptickExFilter = ex;
            document.querySelectorAll('[data-uex]').forEach(b => b.classList.toggle('active', b.dataset.uex === ex));
            renderAll();
        }

        function setDensityEx(ex) {
            densityExFilter = ex;
            document.querySelectorAll('[data-dex]').forEach(b => b.classList.toggle('active', b.dataset.dex === ex));
            renderAll();
        }

        // ===== ФОРМАТИРОВАНИЕ И РЕНДЕР =====
        function fmtUsd(v) {
            v = Number(v || 0);
            if (v >= 1e9) return '$' + (v / 1e9).toFixed(2) + 'B';
            if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
            if (v >= 1e3) return '$' + (v / 1e3).toFixed(0) + 'K';
            return '$' + v.toFixed(0);
        }

        function uptickCardHtml(h) {
            return `
            <div class="card" onclick="openInTiger('${h.exchange}','FUTURES','${h.symbol}')">
                <div class="card-top">
                    <div class="card-left">
                        <span class="ex-logo" title="${h.exchange}">${EX_SVG[h.exchange] || ''}</span>
                        <span class="sym">${h.symbol}</span>
                        <span class="mkt-tag fut">АПТИК</span>
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

        function densityCardHtml(d) {
            const mktCls = d.market_type === 'SPOT' ? 'spot' : 'fut';
            const mktTxt = d.market_type === 'SPOT' ? 'SPOT' : 'FUT';
            return `
            <div class="card" onclick="openInTiger('${d.exchange}','${d.market_type}','${d.symbol}')">
                <div class="card-top">
                    <div class="card-left">
                        <span class="ex-logo" title="${d.exchange}">${EX_SVG[d.exchange] || ''}</span>
                        <span class="sym">${d.symbol}</span>
                        <span class="mkt-tag ${mktCls}">${mktTxt}</span>
                    </div>
                    <div class="card-right">
                        <span class="side-badge ${d.side}">${d.side}</span>
                        <button class="ban-x" onclick="banCoin('${d.symbol}', event)" title="Скрыть монету">✕</button>
                    </div>
                </div>
                <div class="card-meta">
                    <span>Плотность: <b>${fmtUsd(d.size_usd)}</b> @ ${d.price}</span>
                    <span>Дист: <b>${Number(d.distance_pct).toFixed(2)}%</b> · ${d.utc_time || 'сейчас'}</span>
                </div>
            </div>`;
        }

        function renderAll() {
            // Активные сейчас в Лайве
            document.getElementById('live-up-cnt').textContent = cachedActiveUp.length;
            document.getElementById('live-active-upticks').innerHTML = cachedActiveUp.length
                ? cachedActiveUp.slice(0, 6).map(a => `
                    <div class="rule-item" style="cursor:pointer" onclick="openInTiger('${a.exchange}','FUTURES','${a.symbol}')">
                        <span style="display:flex;align-items:center;gap:6px">
                            <span class="ex-logo" style="width:18px;height:18px">${EX_SVG[a.exchange]||''}</span>
                            <b>${a.symbol}</b>
                        </span>
                        <span class="side-badge ${a.side}">${a.side}</span>
                    </div>`).join('')
                : '<div class="empty" style="padding:10px 0">Тихо</div>';

            document.getElementById('live-dn-cnt').textContent = cachedActiveDn.length;
            document.getElementById('live-active-densities').innerHTML = cachedActiveDn.length
                ? cachedActiveDn.slice(0, 6).map(d => `
                    <div class="rule-item" style="cursor:pointer" onclick="openInTiger('${d.exchange}','${d.market_type}','${d.symbol}')">
                        <span style="display:flex;align-items:center;gap:6px">
                            <span class="ex-logo" style="width:18px;height:18px">${EX_SVG[d.exchange]||''}</span>
                            <b>${d.symbol}</b>
                            <span class="mkt-tag ${d.market_type==='SPOT'?'spot':'fut'}">${d.market_type==='SPOT'?'S':'F'}</span>
                        </span>
                        <span><b>${fmtUsd(d.size_usd)}</b> <span class="side-badge ${d.side}">${d.side[0]}</span></span>
                    </div>`).join('')
                : '<div class="empty" style="padding:10px 0">Тихо</div>';

            // Общая лента Лайв
            let feed = [];
            if (liveFilter === 'ALL' || liveFilter === 'UPTICK') {
                feed = feed.concat(cachedHistory.map(x => ({ ...x, _kind: 'UPTICK' })));
            }
            if (liveFilter === 'ALL' || liveFilter === 'DENSITY') {
                feed = feed.concat(cachedDensities.map(x => ({ ...x, _kind: 'DENSITY' })));
            }
            feed.sort((a, b) => (b.utc_time || '').localeCompare(a.utc_time || ''));
            document.getElementById('live-feed').innerHTML = feed.length
                ? feed.slice(0, 80).map(item => item._kind === 'UPTICK' ? uptickCardHtml(item) : densityCardHtml(item)).join('')
                : '<div class="empty">Пока нет сигналов за сегодня (UTC).</div>';

            // Вкладка Аптики
            const upList = cachedHistory.filter(h => uptickExFilter === 'ALL' || h.exchange === uptickExFilter);
            document.getElementById('upticks-list').innerHTML = upList.length
                ? upList.map(uptickCardHtml).join('')
                : '<div class="empty">Аптиков по выбранному фильтру пока нет.</div>';

            // Вкладка Плотности
            const dnList = cachedDensities.filter(d =>
                (densityExFilter === 'ALL' || d.exchange === densityExFilter) &&
                (densityMarketFilter === 'ALL' || d.market_type === densityMarketFilter)
            );
            document.getElementById('densities-list').innerHTML = dnList.length
                ? dnList.map(densityCardHtml).join('')
                : '<div class="empty">Плотностей по выбранному фильтру пока нет.</div>';
        }

        // ===== ПРАВИЛА ПО МОНЕТАМ =====
        function renderCoinRules() {
            const box = document.getElementById('coin-rules-list');
            const entries = Object.entries(userCoinRules || {}).sort((a, b) => a[0].localeCompare(b[0]));
            if (!entries.length) {
                box.innerHTML = '<div class="meta" style="margin-top:6px;">Индивидуальных порогов пока нет.</div>';
                return;
            }
            box.innerHTML = entries.map(([sym, r]) => `
                <div class="rule-item">
                    <span><b>${sym}</b> · Спот ${fmtUsd(r.spot)} · Фьюч ${fmtUsd(r.futures)}</span>
                    <button class="ban-x" onclick="deleteCoinRule('${sym}')">✕</button>
                </div>
            `).join('');
        }

        async function saveCoinRule() {
            if (!userId) { showToast('Откройте приложение через Telegram'); return; }
            const sym = document.getElementById('rule-symbol').value.trim();
            const spot = Number(document.getElementById('rule-spot').value || 100000);
            const fut = Number(document.getElementById('rule-fut').value || 500000);
            if (!sym) { showToast('Введите тикер монеты'); return; }
            try {
                const res = await fetch('/api/coin-rule', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, symbol: sym, spot, futures: fut })
                });
                const data = await res.json();
                if (res.ok && data.status === 'ok') {
                    userCoinRules = data.coin_rules || {};
                    document.getElementById('rule-symbol').value = '';
                    renderCoinRules();
                    showToast(`🎯 Правило для ${data.base} сохранено`);
                    loadData();
                } else {
                    showToast(data.detail || 'Ошибка сохранения');
                }
            } catch { showToast('Нет связи с сервером'); }
        }

        async function deleteCoinRule(sym) {
            if (!userId) return;
            try {
                const res = await fetch('/api/coin-rule/delete', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, symbol: sym })
                });
                const data = await res.json();
                if (res.ok) {
                    userCoinRules = data.coin_rules || {};
                    renderCoinRules();
                    showToast(`🗑 Правило для ${sym} удалено`);
                    loadData();
                }
            } catch {}
        }

        // ===== ЛИЧНЫЙ ЧЕРНЫЙ СПИСОК МОНЕТ =====
        const SCOPE_LABELS = { 'ALL': 'Оба', 'UPTICK': 'Аптики', 'DENSITY': 'Плотн.' };

        function normalizeBannedObj(raw) {
            if (!raw) return {};
            if (Array.isArray(raw)) {
                const obj = {};
                raw.forEach(s => { obj[s] = 'ALL'; });
                return obj;
            }
            return raw;
        }

        function renderBannedCoins() {
            const box = document.getElementById('banned-coins-list');
            const entries = Object.entries(userBannedCoins || {}).sort((a, b) => a[0].localeCompare(b[0]));
            if (!entries.length) {
                box.innerHTML = '<div class="meta">Скрытых вручную монет пока нет.</div>';
                return;
            }
            box.innerHTML = entries.map(([sym, scope]) => `
                <span class="banned-chip">
                    ${sym}
                    <span class="banned-scope">${SCOPE_LABELS[scope] || 'Оба'}</span>
                    <button onclick="unbanCoin('${sym}')" title="Вернуть">✕</button>
                </span>
            `).join('');
        }

        function banCoin(symbol, evt) {
            if (evt) evt.stopPropagation();
            if (!userId) { showToast('Откройте приложение через Telegram'); return; }
            const clean = (symbol || '').trim().toUpperCase();
            if (!clean) return;
            pendingBanSymbol = clean;
            document.getElementById('ban-modal-title').textContent = `🚫 Убрать монету ${clean}`;
            document.getElementById('ban-modal').classList.add('open');
        }

        function closeBanModal(evt) {
            if (evt && evt.target !== evt.currentTarget) return;
            document.getElementById('ban-modal').classList.remove('open');
            pendingBanSymbol = '';
        }

        function submitBanModal(scope) {
            const sym = pendingBanSymbol;
            closeBanModal();
            if (sym) sendBanRequest(sym, scope);
        }

        async function sendBanRequest(symbol, scope) {
            if (!userId) { showToast('Откройте приложение через Telegram'); return; }
            const clean = (symbol || '').trim().toUpperCase();
            if (!clean) return;
            try {
                const res = await fetch('/api/ban-coin', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, symbol: clean, scope: scope || 'ALL' })
                });
                const data = await res.json();
                if (res.ok && data.status === 'ok') {
                    userBannedCoins = normalizeBannedObj(data.banned_coins);
                    renderBannedCoins();
                    showToast(`🚫 ${data.base} скрыта (${SCOPE_LABELS[data.scope] || 'Оба'})`);
                    loadData();
                } else {
                    showToast(data.detail || 'Ошибка скрытия');
                }
            } catch { showToast('Нет связи с сервером'); }
        }

        function banCoinFromInput() {
            const inp = document.getElementById('ban-coin-input');
            const scope = document.getElementById('ban-scope-select').value || 'ALL';
            const val = (inp.value || '').trim();
            if (!val) { showToast('Введите тикер монеты'); return; }
            inp.value = '';
            sendBanRequest(val, scope);
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
                    userBannedCoins = normalizeBannedObj(data.banned_coins);
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
                        min_volume: Number(document.getElementById('set-vol').value || 500000),
                        min_density_spot: Number(document.getElementById('set-dspot').value || 250000),
                        min_density_futures: Number(document.getElementById('set-dfut').value || 1000000)
                    })
                });
                if (res.ok) {
                    showToast('✅ Фильтры сохранены');
                    loadData();
                }
            } catch { showToast('Ошибка сохранения'); }
        }

        function showToast(msg) {
            const el = document.getElementById('toast');
            el.textContent = msg;
            el.classList.add('show');
            clearTimeout(window._toastTimer);
            window._toastTimer = setTimeout(() => el.classList.remove('show'), 2200);
        }

        // ===== ЗАГРУЗКА ДАННЫХ =====
        async function loadData() {
            try {
                const res = await fetch(`/api/data?user_id=${userId}`);
                if (!res.ok) return;
                const data = await res.json();

                cachedHistory = data.history || [];
                cachedDensities = data.densities || [];
                cachedActiveUp = data.active || [];
                cachedActiveDn = data.active_densities || [];

                if (!settingsInitialized) {
                    document.getElementById('set-vol').value = data.user_min_volume || 500000;
                    document.getElementById('set-dspot').value = data.user_min_density_spot || 250000;
                    document.getElementById('set-dfut').value = data.user_min_density_futures || 1000000;
                    userCoinRules = data.user_coin_rules || {};
                    userBannedCoins = normalizeBannedObj(data.user_banned_coins);
                    renderCoinRules();
                    renderBannedCoins();
                    settingsInitialized = true;
                }

                // Проверка новых событий для звука и автооткрытия в Tiger.Trade
                const newestUp = cachedHistory[0];
                const newestDn = cachedDensities[0];
                if (firstLoadDone) {
                    if (newestUp && newestUp.id > lastUptickId) {
                        announceUptick(newestUp);
                        if (document.getElementById('tiger-auto-up').checked) {
                            openInTiger(newestUp.exchange, 'FUTURES', newestUp.symbol);
                        }
                    }
                    if (newestDn && newestDn.id > lastDensityId) {
                        announceDensity(newestDn);
                        if (document.getElementById('tiger-auto-dn').checked) {
                            openInTiger(newestDn.exchange, newestDn.market_type, newestDn.symbol);
                        }
                    }
                }
                if (newestUp) lastUptickId = Math.max(lastUptickId, newestUp.id || 0);
                if (newestDn) lastDensityId = Math.max(lastDensityId, newestDn.id || 0);
                firstLoadDone = true;

                renderAll();
            } catch {}
        }

        initTigerGroups();
        loadLocalPrefs();
        buildExchangeFilters();
        connectTiger(false);
        loadData();
        setInterval(loadData, 4000);
    </script>
</body>
</html>
"""


# ==================== FASTAPI СЕРВЕР И КЭШ ====================

_api_cache: Dict[str, Any] = {"ts": 0.0, "utc_date": "", "history": [], "densities": []}


class SettingsPayload(BaseModel):
    user_id: int
    min_volume: float = DEFAULT_MIN_VOLUME
    min_density_spot: float = DEFAULT_MIN_DENSITY_SPOT
    min_density_futures: float = DEFAULT_MIN_DENSITY_FUTURES


class CoinRulePayload(BaseModel):
    user_id: int
    symbol: str
    spot: float = DEFAULT_MIN_DENSITY_SPOT
    futures: float = DEFAULT_MIN_DENSITY_FUTURES


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
    await density_engine.refresh_watched_bases()

    session = aiohttp.ClientSession(headers={"User-Agent": "UptickDensityScanner/2.0"})
    scanners: List[BaseScanner] = [
        # Аптики (4 биржи)
        BinanceUptickScanner(session, engine),
        BybitUptickScanner(session, engine),
        OKXUptickScanner(session, engine),
        AsterUptickScanner(session, engine),
        # Плотности (7 бирж)
        BinanceDensityScanner(session, density_engine),
        BybitDensityScanner(session, density_engine),
        OKXDensityScanner(session, density_engine),
        KuCoinDensityScanner(session, density_engine),
        AsterDensityScanner(session, density_engine),
        GateDensityScanner(session, density_engine),
        BitgetDensityScanner(session, density_engine),
    ]

    tasks = [asyncio.create_task(s.run()) for s in scanners]

    async def periodic_rules_refresh() -> None:
        while True:
            await asyncio.sleep(60)
            await density_engine.refresh_watched_bases()

    tasks.append(asyncio.create_task(periodic_rules_refresh()))

    if bot is not None:
        tasks.append(asyncio.create_task(dp.start_polling(bot, handle_signals=False)))
        logger.info("Запуск мультибиржевого детектора (4 биржи аптиков, 7 бирж плотностей) + Telegram Bot...")
    else:
        logger.warning("BOT_TOKEN не задан — запущен только веб-сервер и сканеры.")

    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()
        if bot is not None:
            await bot.session.close()
        if _pg_pool is not None:
            await _pg_pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/")
@app.get("/ping")
async def ping() -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "service": "Uptick & Density Detector",
            "storage": "postgresql" if _use_postgres else "sqlite",
            "uptick_exchanges": 4,
            "density_exchanges": 7,
        }
    )


@app.get("/api/data")
async def api_data(user_id: int = Query(default=0)) -> JSONResponse:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = time.time()

    if now - _api_cache["ts"] > 2.5 or _api_cache["utc_date"] != today_utc:
        _api_cache["history"] = await db_fetchall(
            "SELECT id, utc_time, exchange, symbol, side, price, limit_price, volume_24h_usd "
            "FROM upticks WHERE utc_date = ? ORDER BY id DESC LIMIT 120",
            (today_utc,),
        )
        _api_cache["densities"] = await db_fetchall(
            "SELECT id, utc_time, exchange, market_type, symbol, side, price, size_usd, distance_pct, volume_24h_usd "
            "FROM densities WHERE utc_date = ? ORDER BY id DESC LIMIT 250",
            (today_utc,),
        )
        _api_cache["utc_date"] = today_utc
        _api_cache["ts"] = now

    user_min_vol = DEFAULT_MIN_VOLUME
    user_min_dspot = DEFAULT_MIN_DENSITY_SPOT
    user_min_dfut = DEFAULT_MIN_DENSITY_FUTURES
    user_coin_rules: Dict[str, Dict[str, float]] = {}
    user_banned_map: Dict[str, str] = {}

    if user_id:
        row = await db_fetchone(
            "SELECT min_volume, min_density_spot, min_density_futures, coin_rules, banned_coins "
            "FROM users WHERE user_id = ?",
            (user_id,),
        )
        if row:
            user_min_vol = float(row.get("min_volume") or DEFAULT_MIN_VOLUME)
            user_min_dspot = float(row.get("min_density_spot") or DEFAULT_MIN_DENSITY_SPOT)
            user_min_dfut = float(row.get("min_density_futures") or DEFAULT_MIN_DENSITY_FUTURES)
            user_coin_rules = parse_coin_rules(row.get("coin_rules"))
            user_banned_map = parse_banned_coins(row.get("banned_coins"))

    history_rows = [
        h
        for h in _api_cache["history"]
        if not is_blacklisted_symbol(str(h.get("symbol") or ""))
        and not is_coin_banned(extract_base_asset(str(h.get("symbol") or "")), user_banned_map, "UPTICK")
        and float(h.get("volume_24h_usd") or 0) >= user_min_vol
    ]

    density_rows = []
    for d in _api_cache["densities"]:
        sym = str(d.get("symbol") or "")
        base = extract_base_asset(sym)
        if is_coin_banned(base, user_banned_map, "DENSITY"):
            continue
        if is_blacklisted_symbol(sym) and base not in user_coin_rules:
            continue
        threshold = get_user_density_threshold(
            sym,
            str(d.get("market_type") or "FUTURES"),
            user_min_dspot,
            user_min_dfut,
            user_coin_rules,
        )
        if float(d.get("size_usd") or 0) >= threshold:
            density_rows.append(d)
    density_rows = density_rows[:120]

    active_upticks = [
        {"exchange": ex, "symbol": sym, "side": side.value}
        for ex, sym, side in engine.get_active_upticks()
        if not is_blacklisted_symbol(sym)
        and not is_coin_banned(extract_base_asset(sym), user_banned_map, "UPTICK")
    ]

    active_densities = []
    for d in density_engine.get_active_densities():
        base = extract_base_asset(d.symbol)
        if is_coin_banned(base, user_banned_map, "DENSITY"):
            continue
        if is_blacklisted_symbol(d.symbol) and base not in user_coin_rules:
            continue
        threshold = get_user_density_threshold(
            d.symbol, d.market_type.value, user_min_dspot, user_min_dfut, user_coin_rules
        )
        if d.size_usd >= threshold:
            active_densities.append(
                {
                    "exchange": d.exchange,
                    "market_type": d.market_type.value,
                    "symbol": d.symbol,
                    "side": d.side.value,
                    "price": d.price,
                    "size_usd": d.size_usd,
                    "distance_pct": d.distance_pct,
                }
            )

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
            "user_coin_rules": user_coin_rules,
            "user_banned_coins": user_banned_map,
        }
    )


@app.post("/api/settings")
async def api_settings(payload: SettingsPayload) -> JSONResponse:
    vol = max(0.0, payload.min_volume)
    dspot = max(DENSITY_HARD_MIN_SPOT, payload.min_density_spot)
    dfut = max(DENSITY_HARD_MIN_FUTURES, payload.min_density_futures)
    await db_execute(
        "INSERT INTO users (user_id, min_volume, min_density_spot, min_density_futures) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "min_volume = excluded.min_volume, "
        "min_density_spot = excluded.min_density_spot, "
        "min_density_futures = excluded.min_density_futures",
        (payload.user_id, vol, dspot, dfut),
    )
    await density_engine.refresh_watched_bases()
    return JSONResponse({"status": "ok"})


@app.post("/api/coin-rule")
async def api_set_coin_rule(payload: CoinRulePayload) -> JSONResponse:
    base = extract_base_asset(payload.symbol)
    if not base:
        return JSONResponse({"status": "error", "detail": "Укажите корректный тикер монеты"}, status_code=400)
    spot = max(DENSITY_HARD_MIN_SPOT, float(payload.spot))
    fut = max(DENSITY_HARD_MIN_FUTURES, float(payload.futures))

    row = await db_fetchone("SELECT coin_rules FROM users WHERE user_id = ?", (payload.user_id,))
    rules = parse_coin_rules(row.get("coin_rules") if row else None)
    rules[base] = {"spot": spot, "futures": fut}

    await db_execute(
        "INSERT INTO users (user_id, coin_rules) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET coin_rules = excluded.coin_rules",
        (payload.user_id, json.dumps(rules)),
    )
    await density_engine.refresh_watched_bases()
    return JSONResponse({"status": "ok", "base": base, "coin_rules": rules})


@app.post("/api/coin-rule/delete")
async def api_del_coin_rule(payload: SymbolPayload) -> JSONResponse:
    base = extract_base_asset(payload.symbol)
    row = await db_fetchone("SELECT coin_rules FROM users WHERE user_id = ?", (payload.user_id,))
    rules = parse_coin_rules(row.get("coin_rules") if row else None)
    rules.pop(base, None)
    await db_execute(
        "UPDATE users SET coin_rules = ? WHERE user_id = ?",
        (json.dumps(rules), payload.user_id),
    )
    await density_engine.refresh_watched_bases()
    return JSONResponse({"status": "ok", "coin_rules": rules})


@app.post("/api/ban-coin")
async def api_ban_coin(payload: BanCoinPayload) -> JSONResponse:
    base = extract_base_asset(payload.symbol)
    if not base:
        return JSONResponse({"status": "error", "detail": "Некорректный тикер"}, status_code=400)

    scope = normalize_ban_scope(payload.scope)
    row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (payload.user_id,))
    banned = parse_banned_coins(row.get("banned_coins") if row else None)
    banned[base] = scope

    await db_execute(
        "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
        (payload.user_id, json.dumps(banned)),
    )
    return JSONResponse({"status": "ok", "base": base, "scope": scope, "banned_coins": banned})


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
