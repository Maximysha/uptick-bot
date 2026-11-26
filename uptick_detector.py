"""
Мультибиржевой WebSocket-детектор «Аптиков» (Binance, Bybit, OKX, Aster)
+ WebSocket-детектор «Покупаш / Продаваш» (Binance, Bybit, OKX, Bitget, Gate, Aster)
+ Telegram Bot + Mini App + Tiger.Trade.
Поддержка PostgreSQL (через DATABASE_URL) с автоматическим откатом на SQLite.
"""

import asyncio
import json
import logging
import os
import re
import statistics
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

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

DEFAULT_MIN_VOLUME = 500_000.0
UPTICK_TOLERANCE_PCT = 0.20       # Допуск близости цены к планке (%)
UPTICK_COOLDOWN_SEC = 180.0       # 3 минуты между повторами аптика по монете
UPTICK_ACTIVE_TTL_SEC = 15.0      # Сколько секунд аптик считается активным в Лайве
MIN_UPTICK_WALL_USD = 5_000.0     # Мин. реальная плотность в стакане на планке ($)

DEFAULT_MIN_ROBOT_USD = 25_000.0
ROBOT_MAX_SYMBOLS = 40            # Топ активных пар на биржу для WS-ленты сделок
ALL_ROBOT_EXCHANGES = ["Binance", "Bybit", "OKX", "Bitget", "Gate", "Aster"]
ALL_UPTICK_EXCHANGES = ["Binance", "Bybit", "OKX", "Aster"]

DEFAULT_ROBOT_CONFIG: Dict[str, Any] = {
    "window_sec": 25.0,        # Окно анализа ленты (сек)
    "min_print_usd": 1000.0,   # Мин. сумма 1 удара/принта ($)
    "max_print_usd": 0.0,      # Макс. сумма 1 удара/принта ($; 0 = без ограничения)
    "min_total_usd": 25000.0,  # Мин. общий объём робота за окно ($)
    "min_prints": 6,           # Мин. количество ударов за окно
    "min_interval_sec": 0.35,  # Мин. средний интервал между ударами (сек)
    "max_interval_sec": 6.0,   # Макс. средний интервал между ударами (сек)
    "min_dominance": 78.0,     # Мин. перекос дельты в одну сторону (%)
    "min_vol24h": 300000.0,    # Мин. суточный оборот монеты ($)
    "cooldown_sec": 180.0,     # Пауза между повторами по одной монете (сек)
    "side_mode": "BOTH",       # BOTH | BUY (Покупаш) | SELL (Продаваш)
    "strict_pattern": True,    # Требовать ровный лот / ритм TWAP
    "exchanges": list(ALL_ROBOT_EXCHANGES),
}

# Черный список: ТОЛЬКО тяжеловесы, обернутые токены и стейблкоины.
# Акции, ETF и металлы (XAU, XAG, TSLA, NVDA и т.д.) НЕ блокируются и участвуют в аптиках.
HEAVY_COINS_BLACKLIST: Set[str] = {
    # Тяжеловесы крипто-рынка (топ-капа и ликвидные тяжи)
    "BTC", "XBT", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "TRX", "AVAX",
    "TON", "SHIB", "PEPE", "DOT", "LINK", "LTC", "BCH", "SUI", "XLM", "UNI",
    "NEAR", "APT", "ETC", "ICP", "HBAR", "ZEC", "PUMP", "AAVE", "ARB", "OP",
    "FIL", "ATOM", "POL", "MATIC", "TAO", "WLD", "ENA", "ONDO", "VET", "ALGO",
    "CRO", "MNT", "KAS", "RENDER", "RNDR", "FET", "INJ", "STX", "IMX", "SEI",
    "TIA", "JUP", "WIF", "BONK", "FLOKI", "TRUMP", "HYPE", "XMR", "OKB", "BGB",
    "CRV", "LDO", "MKR", "SKY", "PENDLE", "EIGEN", "ENS", "SAND", "MANA", "AXS",
    "GRT", "RUNE", "THETA", "FLOW", "NEO", "EOS", "IOTA", "XTZ", "QNT", "CAKE",
    "PYTH", "JTO", "STRK", "ZK", "ZRO", "PENGU", "VIRTUAL", "FARTCOIN", "PI",
    "IP", "WLFI", "ASTER", "ORDI", "SATS", "LUNC", "BTT", "XEC", "JASMY",
    "DASH", "BSV", "BERA", "KAITO", "BOME", "NOT", "DOGS", "GALA", "CHZ",
    "SNX", "COMP", "DYDX", "GMX", "CFX", "APE", "EGLD", "MINA", "KAVA",
    "SOLX", "BTCX", "ETHX", "XRPX", "BNBX", "ADAX", "SUIX", "DOGE2",
    "BTCDOM", "DEFI",
    # Обернутые и ликвид-стейкинг токены
    "WBTC", "WETH", "STETH", "WSTETH", "BNSOL", "CBETH", "WBETH", "MSOL",
    "JITOSOL", "EZETH", "WEETH", "CBBTC", "TBTC",
    # Стейблкоины, фиат и технические тикеры
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDE", "USDS", "PYUSD",
    "EURC", "EURT", "EURI", "AEUR", "USD1", "RLUSD", "USDD", "USDP", "GUSD",
    "FRAX", "LUSD", "CRVUSD", "XUSD", "USDF", "USTC", "USDG", "USDY", "SUSDE",
    "EUR", "GBP", "TRY", "JPY", "AUD", "CHF", "CAD", "USD", "U",
}

# Дополнительный черный список ТОЛЬКО для Покупаша / Продаваша:
# металлы, нефть/энергетика/сырьё, акции, ETF и фондовые индексы.
ROBOT_STOCKS_METALS_OIL_BLACKLIST: Set[str] = {
    # Металлы и золото/серебро токены
    "XAU", "XAG", "XPT", "XPD", "XCU", "PAXG", "XAUT", "XAUM", "KAU", "KAG",
    "GOLD", "SILVER", "COPPER", "PLATINUM", "PALLADIUM", "NICKEL", "ALUMINIUM",
    "ALUMINUM", "ZINC", "IRON", "GLD", "SLV", "GDX", "GDXJ", "IAU", "PPLT",
    "SIL", "SILJ", "COPX", "URA",
    # Нефть, газ и сырьевые товары
    "USOIL", "UKOIL", "WTI", "BRENT", "CRUDE", "OIL", "USO", "BNO", "UCO",
    "XTI", "XBR", "CL", "BZ", "NG", "NGAS", "NATGAS", "UNG", "GAS", "HO",
    "BOIL", "KOLD", "RBOB", "CORN", "WHEAT", "SOYBEAN", "COFFEE", "SUGAR",
    "COCOA", "COTTON",
    # Фондовые индексы и ETF (включая плечевые ETF)
    "SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "TLT", "SQQQ", "TQQQ", "ARKK",
    "SOXL", "SOXS", "SMH", "XLE", "XLF", "XLK", "XBI", "UVXY", "VIXY", "KWEB",
    "FXI", "EEM", "EWZ", "EWY", "EWJ", "HYG", "LQD", "TNA", "TZA", "SPXL",
    "SPXS", "UPRO", "UDOW", "SDOW", "TSLL", "TSLQ", "NVDL", "NVDX", "NVDS",
    "CONL", "MSTU", "MSTX", "MSTZ", "AMDL", "AMDD", "LABU", "LABD",
    "SPX500", "US500", "NAS100", "US100", "NDX", "US30", "DJI", "DJ30",
    "US2000", "RUS2000", "JP225", "NIKKEI", "DAX", "GER40", "DE40", "UK100",
    "FTSE", "FR40", "CAC40", "EU50", "HK50", "HSI", "CN50", "VIX", "DXY",
    # Акции (US / Global / токенизированные акции)
    "TSLA", "NVDA", "AAPL", "MSFT", "AMZN", "GOOG", "GOOGL", "META", "NFLX",
    "AMD", "INTC", "COIN", "MSTR", "PLTR", "HOOD", "BABA", "CRCL", "RIOT",
    "MARA", "CLSK", "ORCL", "CRM", "PYPL", "UBER", "ABNB", "SHOP", "SNOW",
    "ARM", "TSM", "ASML", "AVGO", "QCOM", "MU", "SMCI", "GME", "AMC", "RDDT",
    "DIS", "BA", "JPM", "GS", "WMT", "KO", "PEP", "MCD", "NKE", "SBUX",
    "PFE", "JNJ", "UNH", "LLY", "NVO", "MRNA", "ABBV", "MRK", "XOM", "CVX",
    "OXY", "SLB", "COP", "BP", "SHEL", "COST", "IBM", "CSCO", "ADBE", "INTU",
    "NOW", "PANW", "CRWD", "NET", "DDOG", "MDB", "SPOT", "ROKU", "RBLX",
    "ZM", "DOCU", "RIVN", "LCID", "NIO", "XPEV", "LI", "SNDK", "SNDKB",
    "MRVL", "TXN", "AMAT", "LRCX", "KLAC", "NXPI", "MCHP", "DELL", "HPE",
    "HPQ", "WDC", "ACN", "BLK", "MS", "BAC", "WFC", "AXP", "BRK", "BRKB",
    "HD", "LOW", "TGT", "PG", "CAT", "DE", "GE", "HON", "LMT", "RTX",
    "UPS", "FDX", "SNAP", "PINS", "JD", "PDD", "BILI", "BIDU", "FUTU",
    "SOFI", "AFRM", "UPST", "SQ", "XYZ", "MA", "VISA", "V", "C", "F", "GM",
    "TM", "SONY", "SAP", "LVMH", "RACE", "OKLO", "SMR", "RKLB", "ASTS",
    "IONQ", "RGTI", "QUBT", "SOUN", "HIMS", "CVNA", "APP", "DUOL", "DASH",
    "LYFT", "DKNG", "PENN", "CCL", "RCL", "AAL", "DAL", "UAL",
    "SVV", "ETN", "SMTC", "CRWV", "FIG", "TEM", "NBIS", "IREN", "WULF",
}

# Динамически загружаемые из API всех 6 бирж списки акций / ETF / металлов / нефти / форекса
DYNAMIC_TRADFI_EX_SYMBOLS: Set[Tuple[str, str]] = set()
DYNAMIC_TRADFI_BASES: Set[str] = set()

# Префиксы/маркеры для автоматического отсечения контрактов нефти, металлов и индексов в роботах
_ROBOT_COMMODITY_MARKERS: Tuple[str, ...] = (
    "USOIL", "UKOIL", "WTI", "BRENT", "CRUDE", "NATGAS", "NGAS",
    "XAU", "XAG", "XPT", "XPD", "PAXG", "XAUT", "GOLD", "SILVER",
    "COPPER", "PLATINUM", "PALLADIUM", "SPX500", "NAS100", "US500",
    "US100", "US30", "GER40", "UK100", "JP225", "HK50",
)


class UptickSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class RobotSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class UptickEvent:
    exchange: str
    symbol: str
    side: UptickSide
    price: float
    limit_price: float
    mark_price: float
    volume_24h_usd: float
    wall_usd: float
    timestamp_utc: str


@dataclass
class RobotEvent:
    exchange: str
    symbol: str
    side: RobotSide
    price: float
    total_usd: float
    prints_count: int
    avg_print_usd: float
    avg_interval_sec: float
    dominance_pct: float
    volume_24h_usd: float
    timestamp_utc: str
    window_sec: int = 25


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def extract_base_asset(symbol: str) -> str:
    """Извлекает базовый тикер из формата любой биржи (1000PEPEUSDT, WIF-USDT-SWAP, SUI_USDT -> PEPE, WIF, SUI)."""
    s = (symbol or "").upper().strip()
    if not s:
        return ""
    for sep in ("-USDT-SWAP", "-USD-SWAP", "_USDT", "-USDT", "/USDT", "_USDC", "-USDC"):
        if s.endswith(sep):
            s = s[: -len(sep)]
            break
    else:
        for suf in ("USDT", "USDC", "BUSD", "PERP"):
            if s.endswith(suf) and len(s) > len(suf):
                s = s[: -len(suf)]
                break
    s = re.sub(r"^(1000000|100000|10000|1000|100|10|1M|K)(?=[A-Z])", "", s)
    s = re.sub(r"[^A-Z0-9.]", "", s)
    return s


def is_blacklisted_symbol(symbol: str) -> bool:
    """Отсекает только тяжеловесов и стейблкоины (акции и металлы разрешены для Аптиков)."""
    base = extract_base_asset(symbol)
    if not base:
        return True
    if base in HEAVY_COINS_BLACKLIST:
        return True
    clean_dot = base.replace(".", "")
    for suf in ("X", "2L", "2S", "3L", "3S", "5L", "5S"):
        if len(clean_dot) >= len(suf) + 3 and clean_dot.endswith(suf):
            if clean_dot[: -len(suf)] in HEAVY_COINS_BLACKLIST:
                return True
    return False


def is_robot_blacklisted_symbol(symbol: str, exchange: str = "") -> bool:
    """
    Отсекает для Покупаша / Продаваша:
    - все акции, ETF, металлы, нефть, форекс и сырьё (из API всех 6 бирж + ручной список);
    - все тяжёлые крипто-монеты и их производные (SOLX, BTCX, ETHX, 3L/3S и т.д.);
    - все стейблкоины и фиат.
    """
    raw_up = (symbol or "").upper().strip()
    if exchange and (exchange, raw_up) in DYNAMIC_TRADFI_EX_SYMBOLS:
        return True
    base = extract_base_asset(raw_up)
    if not base:
        return True
    if (
        base in HEAVY_COINS_BLACKLIST
        or base in ROBOT_STOCKS_METALS_OIL_BLACKLIST
        or base in DYNAMIC_TRADFI_BASES
    ):
        return True

    clean_dot = base.replace(".", "")
    if (
        clean_dot in HEAVY_COINS_BLACKLIST
        or clean_dot in ROBOT_STOCKS_METALS_OIL_BLACKLIST
        or clean_dot in DYNAMIC_TRADFI_BASES
    ):
        return True

    # Проверка на токенизированные акции / ETF / плечевые токены (*X, *ON, *B, *D, *3L, *3S и т.д.)
    for suf in ("ON", "X", "B", "D", "2L", "2S", "3L", "3S", "5L", "5S"):
        if len(clean_dot) >= len(suf) + 3 and clean_dot.endswith(suf):
            root = clean_dot[: -len(suf)]
            if (
                root in HEAVY_COINS_BLACKLIST
                or root in ROBOT_STOCKS_METALS_OIL_BLACKLIST
                or root in DYNAMIC_TRADFI_BASES
            ):
                return True

    # Проверка на маркеры нефти/металлов/индексов в названии контракта
    for marker in _ROBOT_COMMODITY_MARKERS:
        if base == marker or base.startswith(marker):
            return True
    return False


def _register_tradfi_contract(exchange: str, symbol: str) -> None:
    """Регистрирует контракт акции/ETF/металла/нефти/форекса, обнаруженный в API биржи."""
    sym_up = (symbol or "").upper().strip()
    if not sym_up:
        return
    DYNAMIC_TRADFI_EX_SYMBOLS.add((exchange, sym_up))
    clean_sym = sym_up.replace("-USDT-SWAP", "USDT").replace("_USDT", "USDT")
    DYNAMIC_TRADFI_EX_SYMBOLS.add((exchange, clean_sym))
    base = extract_base_asset(sym_up)
    # Тикеры длиной >= 3 (SVV, SOXL, NVDA, TSLA, XAU, ETN, AMDL и т.д.) блокируем глобально для роботов
    if len(base) >= 3:
        DYNAMIC_TRADFI_BASES.add(base)


async def refresh_tradfi_metadata(session: aiohttp.ClientSession) -> None:
    """
    Автоматически запрашивает метаданные контрактов со всех 6 бирж,
    чтобы гарантированно исключить ВСЕ акции, RWA, ETF, металлы, нефть и форекс
    из Покупаша / Продаваша (включая новые листинги).
    """
    tradfi_kw = ("tradfi", "tradifi", "stock", "equity", "commodity", "metal", "oil", "forex", "etf", "energy", "precious")

    async def _load_binance_style(ex_name: str, url: str) -> int:
        cnt = 0
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return 0
                data = await resp.json()
            for s in data.get("symbols") or []:
                sym = str(s.get("symbol") or "").upper()
                if not sym:
                    continue
                ctype = str(s.get("contractType") or "").lower()
                utype = str(s.get("underlyingType") or "").lower()
                subs = [str(x).lower() for x in (s.get("underlyingSubType") or [])]
                is_tradfi = (
                    any(k in ctype for k in tradfi_kw)
                    or (utype and utype not in ("coin", "crypto", "perpetual"))
                    or any(any(k in sub for k in tradfi_kw) for sub in subs)
                )
                if is_tradfi:
                    _register_tradfi_contract(ex_name, sym)
                    cnt += 1
        except Exception as exc:
            logger.debug("[%s TradFi meta] %s", ex_name, exc)
        return cnt

    async def _load_bitget() -> int:
        cnt = 0
        try:
            url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return 0
                data = await resp.json()
            for c in data.get("data") or []:
                sym = str(c.get("symbol") or "").upper()
                if not sym:
                    continue
                is_rwa = str(c.get("isRwa") or "").strip().upper() in ("YES", "TRUE", "1")
                stype = str(c.get("symbolType") or "").lower()
                if is_rwa or "rwa" in stype or any(k in stype for k in tradfi_kw):
                    _register_tradfi_contract("Bitget", sym)
                    cnt += 1
        except Exception as exc:
            logger.debug("[Bitget TradFi meta] %s", exc)
        return cnt

    async def _load_bybit() -> int:
        cnt = 0
        try:
            cursor = ""
            for _ in range(5):
                url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
                if cursor:
                    url += f"&cursor={cursor}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                res = data.get("result") or {}
                for it in res.get("list") or []:
                    sym = str(it.get("symbol") or "").upper()
                    if not sym:
                        continue
                    stype = str(it.get("symbolType") or "").strip().lower()
                    if stype and (stype in ("stock", "etf", "commodity", "forex", "index", "metal", "tradfi", "equity", "rwa") or any(k in stype for k in tradfi_kw)):
                        _register_tradfi_contract("Bybit", sym)
                        cnt += 1
                cursor = str(res.get("nextPageCursor") or "")
                if not cursor:
                    break
        except Exception as exc:
            logger.debug("[Bybit TradFi meta] %s", exc)
        return cnt

    async def _load_okx() -> int:
        cnt = 0
        try:
            url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return 0
                data = await resp.json()
            for it in data.get("data") or []:
                iid = str(it.get("instId") or "").upper()
                if not iid:
                    continue
                cat = str(it.get("instCategory") or "").strip()
                # На OKX instCategory == "1" — крипта, "3" — акции, "4" — сырьё/металлы/нефть
                if cat and cat != "1":
                    _register_tradfi_contract("OKX", iid)
                    cnt += 1
        except Exception as exc:
            logger.debug("[OKX TradFi meta] %s", exc)
        return cnt

    async def _load_gate() -> int:
        cnt = 0
        try:
            url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return 0
                contracts = await resp.json()
            if not isinstance(contracts, list):
                return 0
            gate_non_crypto = ("stock", "metal", "indic", "index", "forex", "commodit", "etf", "tradfi", "equity", "oil", "energy")
            for c in contracts:
                name = str(c.get("name") or "").upper()
                if not name:
                    continue
                ctype = str(c.get("contract_type") or c.get("classification") or "").strip().lower()
                tags = [str(t).lower() for t in (c.get("tags") or [])]
                if any(k in ctype for k in gate_non_crypto) or any(any(k in t for k in gate_non_crypto) for t in tags):
                    _register_tradfi_contract("Gate", name)
                    cnt += 1
        except Exception as exc:
            logger.debug("[Gate TradFi meta] %s", exc)
        return cnt

    results = await asyncio.gather(
        _load_binance_style("Binance", "https://fapi.binance.com/fapi/v1/exchangeInfo"),
        _load_binance_style("Aster", "https://fapi.asterdex.com/fapi/v1/exchangeInfo"),
        _load_bitget(),
        _load_bybit(),
        _load_okx(),
        _load_gate(),
        return_exceptions=True,
    )
    total_found = sum(r for r in results if isinstance(r, int))
    logger.info(
        "[TradFi Filter] Загружено %s не-крипто контрактов (акции/ETF/металлы/нефть) с 6 бирж (%s уникальных тикеров).",
        total_found,
        len(DYNAMIC_TRADFI_BASES),
    )


async def _tradfi_metadata_loop(session: aiohttp.ClientSession) -> None:
    """Периодически обновляет список акций/металлов/нефти с бирж каждые 30 минут."""
    while True:
        await asyncio.sleep(1800)
        try:
            await refresh_tradfi_metadata(session)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Ошибка обновления TradFi метаданных: %s", exc)



def is_robot_blacklisted_symbol(symbol: str) -> bool:
    """
    Отсекает для Покупаша / Продаваша:
    тяжелые монеты, стейблкоины, акции (включая токенизированные *X/*ON/*B),
    ETF/индексы, металлы и нефть/газ.
    """
    raw_up = (symbol or "").upper().strip()
    base = extract_base_asset(raw_up)
    if not base:
        return True
    if base in HEAVY_COINS_BLACKLIST or base in ROBOT_STOCKS_METALS_OIL_BLACKLIST:
        return True
    # Проверка на токенизированные акции (TSLAX, NVDAX, AAPLON, SNDKB, BRK.B и т.д.)
    clean_dot = base.replace(".", "")
    if clean_dot in ROBOT_STOCKS_METALS_OIL_BLACKLIST:
        return True
    for suf in ("ON", "X", "B", "D"):
        if len(clean_dot) > len(suf) + 2 and clean_dot.endswith(suf):
            root = clean_dot[: -len(suf)]
            if root in ROBOT_STOCKS_METALS_OIL_BLACKLIST:
                return True
    # Проверка на маркеры нефти/металлов/индексов в названии контракта
    for marker in _ROBOT_COMMODITY_MARKERS:
        if base == marker or base.startswith(marker):
            return True
    return False


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


def normalize_robot_config(raw: Any, fallback_min_total: Optional[float] = None) -> Dict[str, Any]:
    """Безопасно парсит и валидирует полную конфигурацию Покупаша / Продаваша."""
    data: Dict[str, Any] = {}
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = {}
    elif isinstance(raw, dict):
        data = raw

    def _flt(key: str, default: float, lo: float, hi: float) -> float:
        try:
            v = float(data.get(key, default))
        except (TypeError, ValueError):
            v = default
        return max(lo, min(hi, v))

    def _int(key: str, default: int, lo: int, hi: int) -> int:
        try:
            v = int(float(data.get(key, default)))
        except (TypeError, ValueError):
            v = default
        return max(lo, min(hi, v))

    min_total_default = (
        float(fallback_min_total)
        if fallback_min_total is not None and float(fallback_min_total) > 0
        else DEFAULT_ROBOT_CONFIG["min_total_usd"]
    )
    window_sec = _flt("window_sec", DEFAULT_ROBOT_CONFIG["window_sec"], 5.0, 120.0)
    min_print_usd = _flt("min_print_usd", DEFAULT_ROBOT_CONFIG["min_print_usd"], 100.0, 10_000_000.0)
    max_print_usd = _flt("max_print_usd", DEFAULT_ROBOT_CONFIG["max_print_usd"], 0.0, 50_000_000.0)
    if max_print_usd > 0 and max_print_usd < min_print_usd:
        max_print_usd = min_print_usd

    min_total_usd = _flt("min_total_usd", min_total_default, 2_000.0, 100_000_000.0)
    min_prints = _int("min_prints", DEFAULT_ROBOT_CONFIG["min_prints"], 3, 100)
    min_interval_sec = _flt("min_interval_sec", DEFAULT_ROBOT_CONFIG["min_interval_sec"], 0.1, 60.0)
    max_interval_sec = _flt("max_interval_sec", DEFAULT_ROBOT_CONFIG["max_interval_sec"], 0.3, 120.0)
    if max_interval_sec < min_interval_sec:
        max_interval_sec = min_interval_sec

    min_dominance = _flt("min_dominance", DEFAULT_ROBOT_CONFIG["min_dominance"], 55.0, 99.5)
    min_vol24h = _flt("min_vol24h", DEFAULT_ROBOT_CONFIG["min_vol24h"], 0.0, 10_000_000_000.0)
    cooldown_sec = _flt("cooldown_sec", DEFAULT_ROBOT_CONFIG["cooldown_sec"], 15.0, 3600.0)

    side_mode = str(data.get("side_mode") or "BOTH").strip().upper()
    if side_mode not in ("BOTH", "BUY", "SELL"):
        side_mode = "BOTH"

    strict_raw = data.get("strict_pattern", True)
    if isinstance(strict_raw, str):
        strict_pattern = strict_raw.strip().lower() not in ("0", "false", "no", "off")
    else:
        strict_pattern = bool(strict_raw)

    raw_ex = data.get("exchanges")
    if isinstance(raw_ex, list):
        exchanges = [e for e in raw_ex if e in ALL_ROBOT_EXCHANGES]
        if not exchanges:
            exchanges = list(ALL_ROBOT_EXCHANGES)
    else:
        exchanges = list(ALL_ROBOT_EXCHANGES)

    return {
        "window_sec": round(window_sec, 1),
        "min_print_usd": round(min_print_usd, 2),
        "max_print_usd": round(max_print_usd, 2),
        "min_total_usd": round(min_total_usd, 2),
        "min_prints": min_prints,
        "min_interval_sec": round(min_interval_sec, 2),
        "max_interval_sec": round(max_interval_sec, 2),
        "min_dominance": round(min_dominance, 1),
        "min_vol24h": round(min_vol24h, 2),
        "cooldown_sec": round(cooldown_sec, 0),
        "side_mode": side_mode,
        "strict_pattern": strict_pattern,
        "exchanges": exchanges,
    }


def robot_matches_user_config(
    exchange: str,
    side_val: str,
    total_usd: float,
    prints_count: int,
    avg_print_usd: float,
    avg_interval_sec: float,
    dominance_pct: float,
    volume_24h_usd: float,
    rcfg: Dict[str, Any],
) -> bool:
    """Проверяет, подходит ли событие робота под настройки пользователя."""
    if exchange not in rcfg["exchanges"]:
        return False
    if rcfg["side_mode"] == "BUY" and side_val != "BUY":
        return False
    if rcfg["side_mode"] == "SELL" and side_val != "SELL":
        return False
    if total_usd < float(rcfg["min_total_usd"]):
        return False
    if prints_count < int(rcfg["min_prints"]):
        return False
    if avg_print_usd < float(rcfg["min_print_usd"]) * 0.85:
        return False
    if float(rcfg["max_print_usd"]) > 0 and avg_print_usd > float(rcfg["max_print_usd"]) * 1.15:
        return False
    if avg_interval_sec < float(rcfg["min_interval_sec"]) * 0.85:
        return False
    if avg_interval_sec > float(rcfg["max_interval_sec"]) * 1.15:
        return False
    if dominance_pct < float(rcfg["min_dominance"]):
        return False
    if volume_24h_usd < float(rcfg["min_vol24h"]):
        return False
    return True


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


# ==================== БАЗА ДАННЫХ (PostgreSQL / SQLite) ====================

_pg_pool: Any = None
_use_pg: bool = False


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
                        robot_config TEXT DEFAULT '{}'
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
                robot_config TEXT DEFAULT '{}'
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

        cur_up = await db.execute("PRAGMA table_info(upticks)")
        up_cols = {row[1] for row in await cur_up.fetchall()}
        if "wall_usd" not in up_cols:
            await db.execute("ALTER TABLE upticks ADD COLUMN wall_usd REAL DEFAULT 0")
        await db.commit()
    logger.info("База данных: SQLite (%s) готова.", SQLITE_PATH)


async def db_execute(query: str, params: Tuple[Any, ...] = ()) -> None:
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            await conn.execute(_to_pg_query(query), *params)
        return
    async with aiosqlite.connect(SQLITE_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def db_fetchone(query: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
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
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            rows = await conn.fetch(_to_pg_query(query), *params)
            return [dict(r) for r in rows]
    async with aiosqlite.connect(SQLITE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


# ==================== ПРОВЕРКА РЕАЛЬНОЙ ПЛОТНОСТИ АПТИКА В СТАКАНЕ ====================

async def fetch_uptick_wall_usd(
    session: aiohttp.ClientSession,
    exchange: str,
    symbol: str,
    side: UptickSide,
    limit_price: float,
    okx_ct_val: float = 1.0,
) -> float:
    """
    Запрашивает реальный стакан на бирже в момент подхода к планке.
    Проверяет, что цена стакана действительно у планки и подсчитывает
    размер плотности аптика в $ в пределах 0.25% от планки.
    Возвращает 0.0, если стакан пустой или аптик ложный.
    """
    if limit_price <= 0:
        return 0.0

    # Для LONG-аптика (верхняя планка) защита/подставление стоит на BID;
    # для SHORT-аптика (нижняя планка) — на ASK.
    levels: List[Tuple[float, float]] = []
    try:
        if exchange == "Binance":
            url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=20"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status != 200:
                    return 0.0
                data = await resp.json()
            raw = data.get("bids" if side == UptickSide.LONG else "asks") or []
            levels = [(_safe_float(p), _safe_float(q) * _safe_float(p)) for p, q in raw]

        elif exchange == "Aster":
            url = f"https://fapi.asterdex.com/fapi/v1/depth?symbol={symbol}&limit=20"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status != 200:
                    return 0.0
                data = await resp.json()
            raw = data.get("bids" if side == UptickSide.LONG else "asks") or []
            levels = [(_safe_float(p), _safe_float(q) * _safe_float(p)) for p, q in raw]

        elif exchange == "Bybit":
            url = f"https://api.bybit.com/v5/market/orderbook?category=linear&symbol={symbol}&limit=25"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status != 200:
                    return 0.0
                data = await resp.json()
            res = data.get("result") or {}
            raw = res.get("b" if side == UptickSide.LONG else "a") or []
            levels = [(_safe_float(p), _safe_float(q) * _safe_float(p)) for p, q in raw]

        elif exchange == "OKX":
            inst_id = symbol if symbol.endswith("-SWAP") else f"{symbol}-USDT-SWAP"
            url = f"https://www.okx.com/api/v5/market/books?instId={inst_id}&sz=20"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status != 200:
                    return 0.0
                data = await resp.json()
            arr = data.get("data") or []
            if not arr:
                return 0.0
            raw = arr[0].get("bids" if side == UptickSide.LONG else "asks") or []
            levels = [
                (_safe_float(row[0]), _safe_float(row[1]) * okx_ct_val * _safe_float(row[0]))
                for row in raw
                if len(row) >= 2
            ]
    except Exception:
        return 0.0

    if not levels:
        return 0.0

    # Проверяем, что лучший уровень стакана действительно находится у планки (<= 0.35%)
    best_px = levels[0][0]
    if best_px <= 0:
        return 0.0
    dist_pct = abs(best_px - limit_price) / limit_price * 100.0
    if dist_pct > 0.35:
        return 0.0

    # Суммируем плотность на верхних уровнях стакана в пределах 0.25% от лучшей цены / планки
    wall_usd = 0.0
    for px, usd in levels[:10]:
        if px <= 0:
            continue
        if abs(px - limit_price) / limit_price * 100.0 <= 0.25 or abs(px - best_px) / best_px * 100.0 <= 0.15:
            wall_usd += usd

    return round(wall_usd, 2)


# ==================== ДВИЖОК АПТИКОВ ====================

class UptickEngine:
    """Управляет детекцией аптиков, проверкой реальной плотности в стакане и антиспамом."""

    def __init__(self, on_uptick: Callable[[UptickEvent], Any]) -> None:
        self.on_uptick = on_uptick
        self._cooldown: Dict[Tuple[str, str, UptickSide], float] = {}
        self._last_book_check: Dict[Tuple[str, str], float] = {}
        # (exchange, symbol) -> (side, ts, vol24h, wall_usd)
        self._active: Dict[Tuple[str, str], Tuple[UptickSide, float, float, float]] = {}

    async def evaluate(
        self,
        session: aiohttp.ClientSession,
        exchange: str,
        symbol: str,
        last_price: float,
        mark_price: float,
        upper_limit: float,
        lower_limit: float,
        volume_24h_usd: float,
        best_bid: float = 0.0,
        best_ask: float = 0.0,
        okx_ct_val: float = 1.0,
    ) -> None:
        if last_price <= 0 or mark_price <= 0 or is_blacklisted_symbol(symbol):
            return

        tol = UPTICK_TOLERANCE_PCT / 100.0
        side: Optional[UptickSide] = None
        limit_price = 0.0
        ref_high = max(last_price, best_ask if best_ask > 0 else last_price)
        ref_low = min(last_price, best_bid if best_bid > 0 else last_price)

        if upper_limit > 0 and ref_high >= upper_limit * (1.0 - tol):
            side = UptickSide.LONG
            limit_price = upper_limit
        elif lower_limit > 0 and ref_low <= lower_limit * (1.0 + tol):
            side = UptickSide.SHORT
            limit_price = lower_limit

        if side is None:
            return

        now = time.time()
        key = (exchange, symbol)
        # Не дёргаем REST-стакан чаще раза в 3 сек для одной и той же монеты
        if now - self._last_book_check.get(key, 0.0) < 3.0:
            return
        self._last_book_check[key] = now

        wall_usd = await fetch_uptick_wall_usd(
            session=session,
            exchange=exchange,
            symbol=symbol,
            side=side,
            limit_price=limit_price,
            okx_ct_val=okx_ct_val,
        )
        if wall_usd < MIN_UPTICK_WALL_USD:
            return

        self._active[key] = (side, now, volume_24h_usd, wall_usd)
        cd_key = (exchange, symbol, side)
        if now - self._cooldown.get(cd_key, 0.0) >= UPTICK_COOLDOWN_SEC:
            self._cooldown[cd_key] = now
            ev = UptickEvent(
                exchange=exchange,
                symbol=symbol,
                side=side,
                price=last_price,
                limit_price=limit_price,
                mark_price=mark_price,
                volume_24h_usd=volume_24h_usd,
                wall_usd=wall_usd,
                timestamp_utc=datetime.now(timezone.utc).strftime("%H:%M:%S"),
            )
            await self.on_uptick(ev)

    def get_active_upticks(self) -> List[Tuple[str, str, UptickSide, float, float]]:
        now = time.time()
        expired = [k for k, (_, ts, _, _) in self._active.items() if now - ts > UPTICK_ACTIVE_TTL_SEC]
        for k in expired:
            self._active.pop(k, None)
        return [
            (ex, sym, side, vol, wall)
            for (ex, sym), (side, _, vol, wall) in self._active.items()
        ]


# ==================== ДВИЖОК ПОКУПАШ / ПРОДАВАШ (6 БИРЖ) ====================

class RobotEngine:
    """
    Детектирует алгоритмических маркет-роботов (Покупаш / Продаваш) по WebSocket-ленте сделок.
    Полностью настраивается: окно анализа, размер 1 удара (мин/макс), общий объём,
    количество ударов, интервал между ударами (мин/макс), перекос дельты, пауза повтора,
    направление (Покупаш/Продаваш), строгий режим ритма и список бирж.
    """

    def __init__(self, on_robot: Callable[[RobotEvent], Any]) -> None:
        self.on_robot = on_robot
        self.config: Dict[str, Any] = dict(DEFAULT_ROBOT_CONFIG)
        # (exchange, symbol) -> deque of [ts, is_buy, usd, price]
        self._prints: Dict[Tuple[str, str], Deque[List[Any]]] = {}
        self._last_eval: Dict[Tuple[str, str], float] = {}
        self._cooldown: Dict[Tuple[str, str, RobotSide], float] = {}
        # (exchange, symbol) -> (RobotSide, ts, total_usd, prints_count, avg_interval, avg_print, dominance, vol24h)
        self._active: Dict[Tuple[str, str], Tuple[RobotSide, float, float, int, float, float, float, float]] = {}

    def update_config(self, cfg: Dict[str, Any]) -> None:
        self.config = normalize_robot_config(cfg)
        logger.info(
            "[RobotEngine] Конфиг обновлён: окно=%ss, удар=$%s..%s, объём>=$%s, ударов>=%s, "
            "инт=%s..%ss, дельта>=%s%%, кд=%ss, режим=%s, строгий=%s",
            self.config["window_sec"],
            self.config["min_print_usd"],
            self.config["max_print_usd"] or "∞",
            self.config["min_total_usd"],
            self.config["min_prints"],
            self.config["min_interval_sec"],
            self.config["max_interval_sec"],
            self.config["min_dominance"],
            self.config["cooldown_sec"],
            self.config["side_mode"],
            self.config["strict_pattern"],
        )

    async def process_trade(
        self,
        exchange: str,
        symbol: str,
        is_buy: bool,
        price: float,
        qty_usd: float,
        ts_sec: float,
        volume_24h_usd: float,
    ) -> None:
        if price <= 0 or qty_usd <= 0 or is_robot_blacklisted_symbol(symbol, exchange):
            return

        cfg = self.config
        if exchange not in cfg["exchanges"]:
            return

        now = time.time()
        ts = ts_sec if abs(now - ts_sec) < 60 else now
        key = (exchange, symbol)
        dq = self._prints.get(key)
        if dq is None:
            dq = deque(maxlen=600)
            self._prints[key] = dq

        # Объединяем части одного рыночного ордера в один принт
        merge_sec = min(0.25, max(0.05, float(cfg["min_interval_sec"]) * 0.45))
        if dq and dq[-1][1] == is_buy and (ts - dq[-1][0]) <= merge_sec:
            dq[-1][2] += qty_usd
            dq[-1][3] = price
        else:
            dq.append([ts, is_buy, qty_usd, price])

        # Держим до 125 сек истории
        max_keep_cutoff = ts - max(125.0, float(cfg["window_sec"]) + 5.0)
        while dq and dq[0][0] < max_keep_cutoff:
            dq.popleft()

        if now - self._last_eval.get(key, 0.0) < 1.0:
            return
        self._last_eval[key] = now

        if volume_24h_usd < float(cfg["min_vol24h"]):
            return

        window_sec = float(cfg["window_sec"])
        win_cutoff = ts - window_sec
        win_prints = [p for p in dq if p[0] >= win_cutoff]
        min_prints = int(cfg["min_prints"])
        if len(win_prints) < min_prints:
            return

        buy_usd = 0.0
        sell_usd = 0.0
        for p in win_prints:
            if p[1]:
                buy_usd += p[2]
            else:
                sell_usd += p[2]

        total_all = buy_usd + sell_usd
        min_total_usd = float(cfg["min_total_usd"])
        if total_all < min_total_usd:
            return

        if buy_usd >= sell_usd:
            side = RobotSide.BUY
            side_is_buy = True
            side_usd = buy_usd
        else:
            side = RobotSide.SELL
            side_is_buy = False
            side_usd = sell_usd

        side_mode = cfg["side_mode"]
        if side_mode == "BUY" and side != RobotSide.BUY:
            return
        if side_mode == "SELL" and side != RobotSide.SELL:
            return

        dominance_pct = (side_usd / total_all) * 100.0
        min_dom = float(cfg["min_dominance"])
        if side_usd < min_total_usd or dominance_pct < min_dom:
            return

        min_lot = float(cfg["min_print_usd"])
        max_lot = float(cfg["max_print_usd"])
        sig_prints = [
            p
            for p in win_prints
            if p[1] == side_is_buy
            and p[2] >= min_lot
            and (max_lot <= 0.0 or p[2] <= max_lot)
        ]
        if len(sig_prints) < min_prints:
            return

        matched_usd = sum(p[2] for p in sig_prints)
        if matched_usd < min_total_usd * 0.75:
            return

        intervals = [
            sig_prints[i][0] - sig_prints[i - 1][0]
            for i in range(1, len(sig_prints))
            if (sig_prints[i][0] - sig_prints[i - 1][0]) >= 0.05
        ]
        span = sig_prints[-1][0] - sig_prints[0][0]
        if not intervals:
            return
        avg_int = sum(intervals) / len(intervals)

        min_int = float(cfg["min_interval_sec"])
        max_int = float(cfg["max_interval_sec"])
        if avg_int < min_int or avg_int > max_int:
            return
        if span < max(1.0, min_int * 2.0):
            return

        sizes = [p[2] for p in sig_prints]
        med_size = statistics.median(sizes)
        if med_size <= 0:
            return

        if cfg["strict_pattern"]:
            similar_cnt = sum(1 for s in sizes if 0.45 * med_size <= s <= 2.2 * med_size)
            uniformity = similar_cnt / len(sig_prints)
            if len(intervals) >= 3 and avg_int > 0:
                std_int = statistics.pstdev(intervals)
                cv_int = std_int / avg_int
            else:
                cv_int = 9.9

            is_lot_robot = uniformity >= 0.55
            is_twap_robot = len(intervals) >= 4 and cv_int <= 0.80
            is_heavy_flow = dominance_pct >= max(82.0, min_dom) and matched_usd >= min_total_usd * 1.5
            if not (is_lot_robot or is_twap_robot or is_heavy_flow):
                return

        avg_print = matched_usd / len(sig_prints)
        self._active[key] = (
            side,
            now,
            matched_usd,
            len(sig_prints),
            avg_int,
            avg_print,
            round(dominance_pct, 1),
            volume_24h_usd,
        )
        cd_key = (exchange, symbol, side)
        cooldown_sec = float(cfg["cooldown_sec"])
        if now - self._cooldown.get(cd_key, 0.0) >= cooldown_sec:
            self._cooldown[cd_key] = now
            ev = RobotEvent(
                exchange=exchange,
                symbol=symbol,
                side=side,
                price=float(sig_prints[-1][3]),
                total_usd=matched_usd,
                prints_count=len(sig_prints),
                avg_print_usd=avg_print,
                avg_interval_sec=round(avg_int, 2),
                dominance_pct=round(dominance_pct, 1),
                volume_24h_usd=volume_24h_usd,
                timestamp_utc=datetime.now(timezone.utc).strftime("%H:%M:%S"),
                window_sec=int(round(window_sec)),
            )
            await self.on_robot(ev)

    def get_active_robots(self) -> List[Tuple[str, str, RobotSide, float, int, float, float, float, float]]:
        now = time.time()
        expired = [k for k, (_, ts, _, _, _, _, _, _) in self._active.items() if now - ts > 45]
        for k in expired:
            self._active.pop(k, None)
        return [
            (ex, sym, side, usd, cnt, avg_int, avg_print, dom, vol)
            for (ex, sym), (side, _, usd, cnt, avg_int, avg_print, dom, vol) in self._active.items()
        ]


# ==================== WEBSOCKET-СКАНЕРЫ АПТИКОВ (4 БИРЖИ) ====================

async def _binance_style_uptick_ws(
    session: aiohttp.ClientSession,
    exchange_name: str,
    rest_base: str,
    ws_url: str,
    uptick_eng: UptickEngine,
) -> None:
    """Универсальный WebSocket-сканер аптиков для Binance Futures и Aster Futures."""
    multipliers: Dict[str, Tuple[float, float]] = {}
    mark_prices: Dict[str, float] = {}
    last_info_ts = 0.0

    async def refresh_exchange_info() -> None:
        nonlocal last_info_ts
        try:
            async with session.get(f"{rest_base}/fapi/v1/exchangeInfo", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            for s in data.get("symbols") or []:
                sym = s.get("symbol")
                if not sym or not sym.endswith("USDT") or s.get("status") != "TRADING":
                    continue
                up_m, dn_m = 1.05, 0.95
                for f in s.get("filters") or []:
                    if f.get("filterType") == "PERCENT_PRICE":
                        u = _safe_float(f.get("multiplierUp"), 1.05)
                        d = _safe_float(f.get("multiplierDown"), 0.95)
                        if u > 1.0:
                            up_m = u
                        if 0.0 < d < 1.0:
                            dn_m = d
                        break
                multipliers[sym] = (up_m, dn_m)
            last_info_ts = time.time()
            logger.info("[%s Uptick WS] Загружены лимиты для %s пар", exchange_name, len(multipliers))
        except Exception as exc:
            logger.warning("[%s Uptick WS] Ошибка exchangeInfo: %s", exchange_name, exc)

    while True:
        try:
            if time.time() - last_info_ts > 900 or not multipliers:
                await refresh_exchange_info()
            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                logger.info("[%s Uptick WS] Подключён", exchange_name)
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    payload = json.loads(msg.data)
                    stream = payload.get("stream") or ""
                    items = payload.get("data")
                    if not isinstance(items, list):
                        continue

                    if "markPrice" in stream:
                        for it in items:
                            sym = it.get("s")
                            mp = _safe_float(it.get("p"))
                            if sym and mp > 0:
                                mark_prices[sym] = mp
                    elif "miniTicker" in stream:
                        if time.time() - last_info_ts > 1800:
                            asyncio.create_task(refresh_exchange_info())
                        for it in items:
                            sym = it.get("s") or ""
                            if not sym.endswith("USDT") or is_blacklisted_symbol(sym):
                                continue
                            mp = mark_prices.get(sym, 0.0)
                            if mp <= 0:
                                continue
                            lp = _safe_float(it.get("c"))
                            vol = _safe_float(it.get("q"))
                            up_m, dn_m = multipliers.get(sym, (1.05, 0.95))
                            upper = mp * up_m
                            lower = mp * dn_m
                            if lp <= 0:
                                continue
                            # Быстрая отсечка до проверки стакана
                            if lp >= upper * (1.0 - UPTICK_TOLERANCE_PCT / 100.0) or lp <= lower * (1.0 + UPTICK_TOLERANCE_PCT / 100.0):
                                asyncio.create_task(
                                    uptick_eng.evaluate(
                                        session=session,
                                        exchange=exchange_name,
                                        symbol=sym,
                                        last_price=lp,
                                        mark_price=mp,
                                        upper_limit=upper,
                                        lower_limit=lower,
                                        volume_24h_usd=vol,
                                    )
                                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[%s Uptick WS] Переподключение через 4с: %s", exchange_name, exc)
            await asyncio.sleep(4)


async def bybit_uptick_ws(session: aiohttp.ClientSession, uptick_eng: UptickEngine) -> None:
    """WebSocket-сканер аптиков для Bybit Linear USDT."""
    ws_url = "wss://stream.bybit.com/v5/public/linear"
    state: Dict[str, Dict[str, float]] = {}
    limits_cache: Dict[str, Tuple[float, float, float]] = {}  # sym -> (upper, lower, ts)

    async def get_bybit_limits(sym: str) -> Tuple[float, float]:
        now = time.time()
        cached = limits_cache.get(sym)
        if cached and now - cached[2] < 30.0:
            return cached[0], cached[1]
        try:
            url = f"https://api.bybit.com/v5/market/price-limit?category=linear&symbol={sym}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    res = data.get("result") or {}
                    up = _safe_float(res.get("buyLmt"))
                    dn = _safe_float(res.get("sellLmt"))
                    if up > 0 and dn > 0:
                        limits_cache[sym] = (up, dn, now)
                        return up, dn
        except Exception:
            pass
        return 0.0, 0.0

    while True:
        try:
            async with session.get(
                "https://api.bybit.com/v5/market/tickers?category=linear",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
            raw_list = (data.get("result") or {}).get("list") or []
            symbols = [
                it["symbol"]
                for it in raw_list
                if str(it.get("symbol") or "").endswith("USDT")
                and not is_blacklisted_symbol(str(it.get("symbol") or ""))
                and _safe_float(it.get("turnover24h")) >= 100_000
            ]
            # Сортируем по обороту и подписываемся на топ-150
            sym_vols = {it["symbol"]: _safe_float(it.get("turnover24h")) for it in raw_list if "symbol" in it}
            symbols.sort(key=lambda s: sym_vols.get(s, 0.0), reverse=True)
            symbols = symbols[:150]

            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                for i in range(0, len(symbols), 10):
                    args = [f"tickers.{s}" for s in symbols[i : i + 10]]
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info("[Bybit Uptick WS] Подписан на %s пар", len(symbols))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        await ws.send_json({"op": "ping"})

                ping_task = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        payload = json.loads(msg.data)
                        if not str(payload.get("topic") or "").startswith("tickers."):
                            continue
                        d = payload.get("data")
                        if not isinstance(d, dict):
                            continue
                        sym = d.get("symbol") or ""
                        if not sym:
                            continue
                        st = state.setdefault(sym, {"lp": 0.0, "mp": 0.0, "vol": 0.0, "bid": 0.0, "ask": 0.0})
                        if "lastPrice" in d:
                            st["lp"] = _safe_float(d["lastPrice"], st["lp"])
                        if "markPrice" in d:
                            st["mp"] = _safe_float(d["markPrice"], st["mp"])
                        if "turnover24h" in d:
                            st["vol"] = _safe_float(d["turnover24h"], st["vol"])
                        if "bid1Price" in d:
                            st["bid"] = _safe_float(d["bid1Price"], st["bid"])
                        if "ask1Price" in d:
                            st["ask"] = _safe_float(d["ask1Price"], st["ask"])

                        lp, mp = st["lp"], st["mp"]
                        if lp <= 0 or mp <= 0:
                            continue
                        dev_pct = abs(lp - mp) / mp * 100.0
                        if dev_pct >= 0.4:
                            up, dn = await get_bybit_limits(sym)
                            if up > 0 and dn > 0:
                                asyncio.create_task(
                                    uptick_eng.evaluate(
                                        session=session,
                                        exchange="Bybit",
                                        symbol=sym,
                                        last_price=lp,
                                        mark_price=mp,
                                        upper_limit=up,
                                        lower_limit=dn,
                                        volume_24h_usd=st["vol"],
                                        best_bid=st["bid"],
                                        best_ask=st["ask"],
                                    )
                                )
                finally:
                    ping_task.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Bybit Uptick WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


async def okx_uptick_ws(session: aiohttp.ClientSession, uptick_eng: UptickEngine) -> None:
    """WebSocket-сканер аптиков для OKX USDT-SWAP."""
    ws_url = "wss://ws.okx.com:8443/ws/v5/public"
    ct_vals: Dict[str, float] = {}
    mark_prices: Dict[str, float] = {}
    limits_cache: Dict[str, Tuple[float, float, float]] = {}

    async def get_okx_limits(inst_id: str) -> Tuple[float, float]:
        now = time.time()
        cached = limits_cache.get(inst_id)
        if cached and now - cached[2] < 30.0:
            return cached[0], cached[1]
        try:
            url = f"https://www.okx.com/api/v5/public/price-limit?instId={inst_id}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    arr = data.get("data") or []
                    if arr:
                        up = _safe_float(arr[0].get("buyLmt"))
                        dn = _safe_float(arr[0].get("sellLmt"))
                        if up > 0 and dn > 0:
                            limits_cache[inst_id] = (up, dn, now)
                            return up, dn
        except Exception:
            pass
        return 0.0, 0.0

    while True:
        try:
            async with session.get(
                "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                inst_data = await resp.json()
            for it in inst_data.get("data") or []:
                iid = str(it.get("instId") or "")
                if iid.endswith("-USDT-SWAP"):
                    ct_vals[iid] = _safe_float(it.get("ctVal"), 1.0)

            async with session.get(
                "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                tk_data = await resp.json()
            items = []
            for it in tk_data.get("data") or []:
                iid = str(it.get("instId") or "")
                if not iid.endswith("-USDT-SWAP") or is_blacklisted_symbol(iid):
                    continue
                vol_usd = _safe_float(it.get("volCcy24h")) * _safe_float(it.get("last"))
                if vol_usd >= 100_000:
                    items.append((iid, vol_usd))
            items.sort(key=lambda x: x[1], reverse=True)
            inst_ids = [x[0] for x in items[:100]]

            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                for i in range(0, len(inst_ids), 25):
                    chunk = inst_ids[i : i + 25]
                    args = []
                    for iid in chunk:
                        args.append({"channel": "tickers", "instId": iid})
                        args.append({"channel": "mark-price", "instId": iid})
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info("[OKX Uptick WS] Подписан на %s пар", len(inst_ids))

                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    if msg.data == "pong":
                        continue
                    payload = json.loads(msg.data)
                    arg = payload.get("arg") or {}
                    ch = arg.get("channel")
                    arr = payload.get("data") or []
                    if not arr:
                        continue
                    d = arr[0]
                    iid = d.get("instId") or arg.get("instId") or ""
                    if not iid:
                        continue
                    if ch == "mark-price":
                        mp = _safe_float(d.get("markPx"))
                        if mp > 0:
                            mark_prices[iid] = mp
                    elif ch == "tickers":
                        lp = _safe_float(d.get("last"))
                        mp = mark_prices.get(iid, 0.0)
                        if lp <= 0 or mp <= 0:
                            continue
                        bid = _safe_float(d.get("bidPx"))
                        ask = _safe_float(d.get("askPx"))
                        vol_usd = _safe_float(d.get("volCcy24h")) * lp
                        dev_pct = abs(lp - mp) / mp * 100.0
                        if dev_pct >= 0.4:
                            up, dn = await get_okx_limits(iid)
                            if up > 0 and dn > 0:
                                sym_clean = iid.replace("-USDT-SWAP", "USDT")
                                asyncio.create_task(
                                    uptick_eng.evaluate(
                                        session=session,
                                        exchange="OKX",
                                        symbol=sym_clean,
                                        last_price=lp,
                                        mark_price=mp,
                                        upper_limit=up,
                                        lower_limit=dn,
                                        volume_24h_usd=vol_usd,
                                        best_bid=bid,
                                        best_ask=ask,
                                        okx_ct_val=ct_vals.get(iid, 1.0),
                                    )
                                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[OKX Uptick WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


# ==================== WEBSOCKET-СКАНЕРЫ ПОКУПАШ / ПРОДАВАШ (6 БИРЖ) ====================

async def _binance_style_robot_ws(
    session: aiohttp.ClientSession,
    exchange_name: str,
    rest_ticker_url: str,
    ws_stream_base: str,
    rob_eng: RobotEngine,
) -> None:
    """WebSocket-поток сделок aggTrade для Binance Futures и Aster Futures."""
    while True:
        try:
            async with session.get(rest_ticker_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                tickers = await resp.json()
            if not isinstance(tickers, list):
                await asyncio.sleep(10)
                continue

            vols: Dict[str, float] = {}
            for t in tickers:
                sym = str(t.get("symbol") or "")
                if not sym.endswith("USDT") or is_robot_blacklisted_symbol(sym, exchange_name):
                    continue
                v = _safe_float(t.get("quoteVolume"))
                if v >= 150_000:
                    vols[sym] = v

            top_syms = sorted(vols.keys(), key=lambda s: vols[s], reverse=True)[:ROBOT_MAX_SYMBOLS]
            if not top_syms:
                await asyncio.sleep(15)
                continue

            streams = "/".join(f"{s.lower()}@aggTrade" for s in top_syms)
            url = f"{ws_stream_base}/stream?streams={streams}"
            started = time.time()

            async with session.ws_connect(url, heartbeat=20, timeout=15) as ws:
                logger.info("[%s Robot WS] Подписан на %s пар", exchange_name, len(top_syms))
                async for msg in ws:
                    if time.time() - started > 1800:
                        break  # Обновляем топ активных монет каждые 30 минут
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    payload = json.loads(msg.data)
                    d = payload.get("data")
                    if not isinstance(d, dict):
                        continue
                    sym = d.get("s") or ""
                    px = _safe_float(d.get("p"))
                    qty = _safe_float(d.get("q"))
                    if px <= 0 or qty <= 0:
                        continue
                    # m == True означает buyer is maker -> агрессор ПРОДАВЕЦ (SELL)
                    is_buy = not bool(d.get("m"))
                    ts_sec = _safe_float(d.get("T"), time.time() * 1000.0) / 1000.0
                    await rob_eng.process_trade(
                        exchange=exchange_name,
                        symbol=sym,
                        is_buy=is_buy,
                        price=px,
                        qty_usd=px * qty,
                        ts_sec=ts_sec,
                        volume_24h_usd=vols.get(sym, 0.0),
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[%s Robot WS] Переподключение через 5с: %s", exchange_name, exc)
            await asyncio.sleep(5)


async def bybit_robot_ws(session: aiohttp.ClientSession, rob_eng: RobotEngine) -> None:
    """WebSocket-поток сделок publicTrade для Bybit Linear."""
    while True:
        try:
            async with session.get(
                "https://api.bybit.com/v5/market/tickers?category=linear",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
            vols: Dict[str, float] = {}
            for it in (data.get("result") or {}).get("list") or []:
                sym = str(it.get("symbol") or "")
                if not sym.endswith("USDT") or is_robot_blacklisted_symbol(sym, "Bybit"):
                    continue
                v = _safe_float(it.get("turnover24h"))
                if v >= 150_000:
                    vols[sym] = v
            top_syms = sorted(vols.keys(), key=lambda s: vols[s], reverse=True)[:ROBOT_MAX_SYMBOLS]
            if not top_syms:
                await asyncio.sleep(15)
                continue

            started = time.time()
            async with session.ws_connect("wss://stream.bybit.com/v5/public/linear", heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top_syms), 10):
                    await ws.send_json({"op": "subscribe", "args": [f"publicTrade.{s}" for s in top_syms[i : i + 10]]})
                logger.info("[Bybit Robot WS] Подписан на %s пар", len(top_syms))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        await ws.send_json({"op": "ping"})

                pt = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if time.time() - started > 1800:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        payload = json.loads(msg.data)
                        if not str(payload.get("topic") or "").startswith("publicTrade."):
                            continue
                        for tr in payload.get("data") or []:
                            sym = tr.get("s") or ""
                            px = _safe_float(tr.get("p"))
                            qty = _safe_float(tr.get("v"))
                            if px <= 0 or qty <= 0:
                                continue
                            is_buy = str(tr.get("S") or "").lower() == "buy"
                            ts_sec = _safe_float(tr.get("T"), time.time() * 1000.0) / 1000.0
                            await rob_eng.process_trade(
                                exchange="Bybit",
                                symbol=sym,
                                is_buy=is_buy,
                                price=px,
                                qty_usd=px * qty,
                                ts_sec=ts_sec,
                                volume_24h_usd=vols.get(sym, 0.0),
                            )
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Bybit Robot WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


async def okx_robot_ws(session: aiohttp.ClientSession, rob_eng: RobotEngine) -> None:
    """WebSocket-поток сделок trades для OKX USDT-SWAP."""
    while True:
        try:
            async with session.get(
                "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                inst_data = await resp.json()
            ct_vals = {
                str(it.get("instId") or ""): _safe_float(it.get("ctVal"), 1.0)
                for it in inst_data.get("data") or []
                if str(it.get("instId") or "").endswith("-USDT-SWAP")
            }

            async with session.get(
                "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                tk_data = await resp.json()
            vols: Dict[str, float] = {}
            for it in tk_data.get("data") or []:
                iid = str(it.get("instId") or "")
                if not iid.endswith("-USDT-SWAP") or is_robot_blacklisted_symbol(iid, "OKX"):
                    continue
                v = _safe_float(it.get("volCcy24h")) * _safe_float(it.get("last"))
                if v >= 150_000:
                    vols[iid] = v
            top_ids = sorted(vols.keys(), key=lambda s: vols[s], reverse=True)[:ROBOT_MAX_SYMBOLS]
            if not top_ids:
                await asyncio.sleep(15)
                continue

            started = time.time()
            async with session.ws_connect("wss://ws.okx.com:8443/ws/v5/public", heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top_ids), 20):
                    args = [{"channel": "trades", "instId": iid} for iid in top_ids[i : i + 20]]
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info("[OKX Robot WS] Подписан на %s пар", len(top_ids))

                async for msg in ws:
                    if time.time() - started > 1800:
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    if msg.data == "pong":
                        continue
                    payload = json.loads(msg.data)
                    if (payload.get("arg") or {}).get("channel") != "trades":
                        continue
                    for tr in payload.get("data") or []:
                        iid = tr.get("instId") or ""
                        px = _safe_float(tr.get("px"))
                        sz = _safe_float(tr.get("sz"))
                        if px <= 0 or sz <= 0:
                            continue
                        ct = ct_vals.get(iid, 1.0)
                        is_buy = str(tr.get("side") or "").lower() == "buy"
                        ts_sec = _safe_float(tr.get("ts"), time.time() * 1000.0) / 1000.0
                        sym_clean = iid.replace("-USDT-SWAP", "USDT")
                        await rob_eng.process_trade(
                            exchange="OKX",
                            symbol=sym_clean,
                            is_buy=is_buy,
                            price=px,
                            qty_usd=px * sz * ct,
                            ts_sec=ts_sec,
                            volume_24h_usd=vols.get(iid, 0.0),
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[OKX Robot WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


async def bitget_robot_ws(session: aiohttp.ClientSession, rob_eng: RobotEngine) -> None:
    """WebSocket-поток сделок trade для Bitget USDT-FUTURES."""
    while True:
        try:
            async with session.get(
                "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
            vols: Dict[str, float] = {}
            for it in data.get("data") or []:
                sym = str(it.get("symbol") or "")
                if not sym.endswith("USDT") or is_robot_blacklisted_symbol(sym, "Bitget"):
                    continue
                v = _safe_float(it.get("usdtVolume"))
                if v >= 150_000:
                    vols[sym] = v
            top_syms = sorted(vols.keys(), key=lambda s: vols[s], reverse=True)[:ROBOT_MAX_SYMBOLS]
            if not top_syms:
                await asyncio.sleep(15)
                continue

            started = time.time()
            async with session.ws_connect("wss://ws.bitget.com/v2/ws/public", heartbeat=25, timeout=15) as ws:
                for i in range(0, len(top_syms), 20):
                    args = [
                        {"instType": "USDT-FUTURES", "channel": "trade", "instId": s}
                        for s in top_syms[i : i + 20]
                    ]
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info("[Bitget Robot WS] Подписан на %s пар", len(top_syms))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(25)
                        await ws.send_str("ping")

                pt = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if time.time() - started > 1800:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        if msg.data == "pong":
                            continue
                        payload = json.loads(msg.data)
                        if payload.get("action") != "update":
                            continue
                        arg = payload.get("arg") or {}
                        sym = arg.get("instId") or ""
                        if not sym:
                            continue
                        for tr in payload.get("data") or []:
                            px = _safe_float(tr.get("price"))
                            sz = _safe_float(tr.get("size"))
                            if px <= 0 or sz <= 0:
                                continue
                            is_buy = str(tr.get("side") or "").lower() == "buy"
                            ts_sec = _safe_float(tr.get("ts"), time.time() * 1000.0) / 1000.0
                            await rob_eng.process_trade(
                                exchange="Bitget",
                                symbol=sym,
                                is_buy=is_buy,
                                price=px,
                                qty_usd=px * sz,
                                ts_sec=ts_sec,
                                volume_24h_usd=vols.get(sym, 0.0),
                            )
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Bitget Robot WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


async def gate_robot_ws(session: aiohttp.ClientSession, rob_eng: RobotEngine) -> None:
    """WebSocket-поток сделок futures.trades для Gate.io USDT Perpetual."""
    while True:
        try:
            async with session.get(
                "https://api.gateio.ws/api/v4/futures/usdt/contracts",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                contracts = await resp.json()
            q_mults = {
                str(c.get("name") or ""): _safe_float(c.get("quanto_multiplier"), 1.0)
                for c in (contracts if isinstance(contracts, list) else [])
                if c.get("name")
            }

            async with session.get(
                "https://api.gateio.ws/api/v4/futures/usdt/tickers",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                tickers = await resp.json()
            vols: Dict[str, float] = {}
            for t in (tickers if isinstance(tickers, list) else []):
                c = str(t.get("contract") or "")
                if not c.endswith("_USDT") or is_robot_blacklisted_symbol(c, "Gate"):
                    continue
                v = _safe_float(t.get("volume_24h_quote"))
                if v >= 150_000:
                    vols[c] = v
            top_contracts = sorted(vols.keys(), key=lambda s: vols[s], reverse=True)[:ROBOT_MAX_SYMBOLS]
            if not top_contracts:
                await asyncio.sleep(15)
                continue

            started = time.time()
            async with session.ws_connect("wss://fx-ws.gateio.ws/v4/ws/usdt", heartbeat=20, timeout=15) as ws:
                await ws.send_json(
                    {
                        "time": int(time.time()),
                        "channel": "futures.trades",
                        "event": "subscribe",
                        "payload": top_contracts,
                    }
                )
                logger.info("[Gate Robot WS] Подписан на %s пар", len(top_contracts))

                async for msg in ws:
                    if time.time() - started > 1800:
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    payload = json.loads(msg.data)
                    if payload.get("channel") != "futures.trades" or payload.get("event") != "update":
                        continue
                    for tr in payload.get("result") or []:
                        c = tr.get("contract") or ""
                        px = _safe_float(tr.get("price"))
                        size_raw = _safe_float(tr.get("size"))
                        if px <= 0 or size_raw == 0:
                            continue
                        is_buy = size_raw > 0
                        mult = q_mults.get(c, 1.0)
                        qty_usd = abs(size_raw) * mult * px
                        ts_sec = _safe_float(tr.get("create_time_ms"), time.time() * 1000.0) / 1000.0
                        sym_clean = c.replace("_USDT", "USDT")
                        await rob_eng.process_trade(
                            exchange="Gate",
                            symbol=sym_clean,
                            is_buy=is_buy,
                            price=px,
                            qty_usd=qty_usd,
                            ts_sec=ts_sec,
                            volume_24h_usd=vols.get(c, 0.0),
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Gate Robot WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


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
    for uid in user_ids:
        try:
            await bot.send_message(uid, text, parse_mode="HTML", reply_markup=reply_markup)
        except Exception as exc:
            logger.debug("Ошибка отправки пользователю %s: %s", uid, exc)
        await asyncio.sleep(0.04)


# Кэш для быстрого ответа /api/data
_api_cache: Dict[str, Any] = {"utc_date": "", "history": [], "robots": [], "ts": 0.0}


async def refresh_api_cache(force: bool = False) -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = time.time()
    if not force and _api_cache["utc_date"] == today_utc and (now - _api_cache["ts"]) < 2.5:
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
        f"Монета: <code>{event.symbol}</code>\n"
        f"{emoji} Сторона: <b>{event.side.value}</b>\n"
        f"🧱 Плотность аптика: <b>${event.wall_usd:,.0f}</b>\n"
        f"Цена: <code>{event.price:g}</code> | Планка: <code>{event.limit_price:g}</code>\n"
        f"Оборот 24ч: <b>${event.volume_24h_usd:,.0f}</b>\n"
        f"Время (UTC): {event.timestamp_utc}"
    )

    users = await db_fetchall("SELECT user_id, min_volume, banned_coins, enable_upticks FROM users")
    base = extract_base_asset(event.symbol)
    target_ids: List[int] = []
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
        "SELECT user_id, min_robot_usd, robot_config, banned_coins, enable_robots FROM users"
    )
    base = extract_base_asset(event.symbol)
    target_ids: List[int] = []
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
    """Отправляет уведомление в Telegram после успешного деплоя / запуска бота."""
    await asyncio.sleep(4)
    try:
        users = await db_fetchall("SELECT user_id FROM users")
        target_ids = [int(u["user_id"]) for u in users if int(u.get("user_id") or 0) > 0]
        if not target_ids:
            return
        commit_sha = (os.getenv("RENDER_GIT_COMMIT") or "").strip()[:7]
        commit_line = f"\nВерсия: <code>{commit_sha}</code>" if commit_sha else ""
        now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        rcfg = robot_engine.config
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
                        robot_config: robotCfg
                    })
                });
                if (res.ok) {
                    const out = await res.json();
                    if (out.robot_config) applyRobotConfigToUI(out.robot_config);
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
        setInterval(loadData, 3000);
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
    await init_db()
    await load_engine_robot_config()

    ws_session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=80, ttl_dns_cache=300),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    # Сразу загружаем со всех 6 бирж актуальные списки акций/ETF/металлов/нефти до запуска роботов
    await refresh_tradfi_metadata(ws_session)

    tasks: List[asyncio.Task[Any]] = [
        asyncio.create_task(_run_telegram_polling()),
        asyncio.create_task(notify_bot_updated()),
        asyncio.create_task(_tradfi_metadata_loop(ws_session)),
        # 4 WebSocket-сканера Аптиков
        asyncio.create_task(
            _binance_style_uptick_ws(
                ws_session,
                "Binance",
                "https://fapi.binance.com",
                "wss://fstream.binance.com/stream?streams=!markPrice@arr@1s/!miniTicker@arr",
                engine,
            )
        ),
        asyncio.create_task(
            _binance_style_uptick_ws(
                ws_session,
                "Aster",
                "https://fapi.asterdex.com",
                "wss://fstream.asterdex.com/stream?streams=!markPrice@arr@1s/!miniTicker@arr",
                engine,
            )
        ),
        asyncio.create_task(bybit_uptick_ws(ws_session, engine)),
        asyncio.create_task(okx_uptick_ws(ws_session, engine)),
        # 6 WebSocket-сканеров Покупаш / Продаваш
        asyncio.create_task(
            _binance_style_robot_ws(
                ws_session,
                "Binance",
                "https://fapi.binance.com/fapi/v1/ticker/24hr",
                "wss://fstream.binance.com",
                robot_engine,
            )
        ),
        asyncio.create_task(
            _binance_style_robot_ws(
                ws_session,
                "Aster",
                "https://fapi.asterdex.com/fapi/v1/ticker/24hr",
                "wss://fstream.asterdex.com",
                robot_engine,
            )
        ),
        asyncio.create_task(bybit_robot_ws(ws_session, robot_engine)),
        asyncio.create_task(okx_robot_ws(ws_session, robot_engine)),
        asyncio.create_task(bitget_robot_ws(ws_session, robot_engine)),
        asyncio.create_task(gate_robot_ws(ws_session, robot_engine)),
    ]

    logger.info("Запущены 4 WS-сканера Аптиков и 6 WS-сканеров Покупаш/Продаваш.")
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
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

    if user_id:
        row = await db_fetchone(
            "SELECT min_volume, min_robot_usd, robot_config, enable_upticks, enable_robots, banned_coins "
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

    if payload.user_id > 0:
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
    return JSONResponse({"status": "ok", "robot_config": rcfg})


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
    return JSONResponse({"status": "ok", "banned_coins": bmap})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
