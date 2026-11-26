"""
Независимый модуль WebSocket-детектора «Покупаш / Продаваш» (6 бирж:
Binance, Bybit, OKX, Bitget, Gate, Aster).
Полностью исключает акции, RWA, ETF, металлы, нефть/газ, форекс,
тяжёлые монеты и стейблкоины (динамически через API всех 6 бирж + ручные списки).
"""

import asyncio
import json
import logging
import re
import statistics
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

import aiohttp

logger = logging.getLogger("uptick_bot.robots")

# ==================== КОНФИГ ПОКУПАШ / ПРОДАВАШ ====================

DEFAULT_MIN_ROBOT_USD = 25_000.0
ROBOT_MAX_SYMBOLS = 40            # Топ активных пар на биржу для WS-ленты сделок
ALL_ROBOT_EXCHANGES = ["Binance", "Bybit", "OKX", "Bitget", "Gate", "Aster"]

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

# Тяжёлые крипто-монеты, обёрнутые токены, стейблкоины и фиат
ROBOT_HEAVY_COINS_BLACKLIST: Set[str] = {
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
    # Обёрнутые и ликвид-стейкинг токены
    "WBTC", "WETH", "STETH", "WSTETH", "BNSOL", "CBETH", "WBETH", "MSOL",
    "JITOSOL", "EZETH", "WEETH", "CBBTC", "TBTC",
    # Стейблкоины и фиат
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDE", "USDS", "PYUSD",
    "EURC", "EURT", "EURI", "AEUR", "USD1", "RLUSD", "USDD", "USDP", "GUSD",
    "FRAX", "LUSD", "CRVUSD", "XUSD", "USDF", "USTC", "USDG", "USDY", "SUSDE",
    "EUR", "GBP", "TRY", "JPY", "AUD", "CHF", "CAD", "USD", "U",
}

# Металлы, нефть/энергетика/сырьё, акции, ETF и фондовые индексы
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

_ROBOT_COMMODITY_MARKERS: Tuple[str, ...] = (
    "USOIL", "UKOIL", "WTI", "BRENT", "CRUDE", "NATGAS", "NGAS",
    "XAU", "XAG", "XPT", "XPD", "PAXG", "XAUT", "GOLD", "SILVER",
    "COPPER", "PLATINUM", "PALLADIUM", "SPX500", "NAS100", "US500",
    "US100", "US30", "GER40", "UK100", "JP225", "HK50",
)


class RobotSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


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


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И ФИЛЬТРЫ ====================

def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _extract_base(symbol: str) -> str:
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


def is_robot_blacklisted_symbol(symbol: str, exchange: str = "") -> bool:
    """
    Отсекает для Покупаша / Продаваша:
    - все акции, RWA, ETF, металлы, нефть, форекс и сырьё (из API всех 6 бирж + ручной список);
    - все тяжёлые крипто-монеты и их производные (SOLX, BTCX, ETHX, 3L/3S и т.д.);
    - все стейблкоины и фиат.
    """
    raw_up = (symbol or "").upper().strip()
    if exchange and (exchange, raw_up) in DYNAMIC_TRADFI_EX_SYMBOLS:
        return True

    base = _extract_base(raw_up)
    if not base:
        return True
    if (
        base in ROBOT_HEAVY_COINS_BLACKLIST
        or base in ROBOT_STOCKS_METALS_OIL_BLACKLIST
        or base in DYNAMIC_TRADFI_BASES
    ):
        return True

    clean_dot = base.replace(".", "")
    if (
        clean_dot in ROBOT_HEAVY_COINS_BLACKLIST
        or clean_dot in ROBOT_STOCKS_METALS_OIL_BLACKLIST
        or clean_dot in DYNAMIC_TRADFI_BASES
    ):
        return True

    # Токенизированные акции / ETF / плечевые токены (*X, *ON, *B, *D, *2L/*3S и т.д.)
    for suf in ("ON", "X", "B", "D", "2L", "2S", "3L", "3S", "5L", "5S"):
        if len(clean_dot) >= len(suf) + 3 and clean_dot.endswith(suf):
            root = clean_dot[: -len(suf)]
            if (
                root in ROBOT_HEAVY_COINS_BLACKLIST
                or root in ROBOT_STOCKS_METALS_OIL_BLACKLIST
                or root in DYNAMIC_TRADFI_BASES
            ):
                return True

    # Маркеры нефти/металлов/индексов в названии контракта
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
    base = _extract_base(sym_up)
    if len(base) >= 3:
        DYNAMIC_TRADFI_BASES.add(base)


async def refresh_tradfi_metadata(session: aiohttp.ClientSession) -> None:
    """
    Автоматически запрашивает метаданные контрактов со всех 6 бирж,
    чтобы исключить ВСЕ акции, RWA, ETF, металлы, нефть и форекс
    из Покупаша / Продаваша (включая новые листинги).
    """
    tradfi_kw = (
        "tradfi", "tradifi", "stock", "equity", "commodity",
        "metal", "oil", "forex", "etf", "energy", "precious",
    )

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
                    if stype and (
                        stype in ("stock", "etf", "commodity", "forex", "index", "metal", "tradfi", "equity", "rwa")
                        or any(k in stype for k in tradfi_kw)
                    ):
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
            gate_non_crypto = (
                "stock", "metal", "indic", "index", "forex",
                "commodit", "etf", "tradfi", "equity", "oil", "energy",
            )
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
        "[TradFi Filter] Загружено %s не-крипто контрактов (акции/ETF/металлы/нефть) с 6 бирж (%s тикеров).",
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


# ==================== ДВИЖОК ПОКУПАШ / ПРОДАВАШ ====================

class RobotEngine:
    """
    Детектирует алгоритмических маркет-роботов (Покупаш / Продаваш) по WebSocket-ленте сделок.
    Полностью настраивается под пользователя (тайминги, размер удара, общий объём, дельта, биржи).
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
                        break
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


def create_robot_tasks(session: aiohttp.ClientSession, rob_eng: RobotEngine) -> List[asyncio.Task[Any]]:
    """Запускает авто-обновление TradFi-фильтра и 6 WebSocket-сканеров Покупаш / Продаваш."""
    return [
        asyncio.create_task(_tradfi_metadata_loop(session)),
        asyncio.create_task(
            _binance_style_robot_ws(
                session,
                "Binance",
                "https://fapi.binance.com/fapi/v1/ticker/24hr",
                "wss://fstream.binance.com",
                rob_eng,
            )
        ),
        asyncio.create_task(
            _binance_style_robot_ws(
                session,
                "Aster",
                "https://fapi.asterdex.com/fapi/v1/ticker/24hr",
                "wss://fstream.asterdex.com",
                rob_eng,
            )
        ),
        asyncio.create_task(bybit_robot_ws(session, rob_eng)),
        asyncio.create_task(okx_robot_ws(session, rob_eng)),
        asyncio.create_task(bitget_robot_ws(session, rob_eng)),
        asyncio.create_task(gate_robot_ws(session, rob_eng)),
    ]