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

ALL_ROBOT_EXCHANGES = ["Binance", "Bybit", "OKX", "Bitget", "Gate", "Aster"]
DEFAULT_MIN_ROBOT_USD = 25_000.0

DEFAULT_ROBOT_CONFIG: Dict[str, Any] = {
    "window_sec": 25.0,
    "min_print_usd": 1000.0,
    "max_print_usd": 0.0,          # 0 = без верхнего лимита
    "min_total_usd": 25000.0,
    "min_prints": 6,
    "min_interval_sec": 0.35,
    "max_interval_sec": 6.0,
    "min_dominance": 78.0,
    "min_vol24h": 300000.0,
    "cooldown_sec": 180.0,
    "side_mode": "BOTH",           # BOTH | BUY | SELL
    "strict_pattern": True,
    "exchanges": list(ALL_ROBOT_EXCHANGES),
}

# Тяжёлые крипто-монеты, обёрнутые токены, стейблкоины и фиат
ROBOT_HEAVY_AND_STABLE: Set[str] = {
    # Тяжеловесы крипто-рынка
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
    # Стейблкоины, фиат и форекс
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDE", "USDS", "PYUSD",
    "EURC", "EURT", "EURI", "AEUR", "USD1", "RLUSD", "USDD", "USDP", "GUSD",
    "FRAX", "LUSD", "CRVUSD", "XUSD", "USDF", "USTC", "USDG", "USDY", "SUSDE",
    "EUR", "GBP", "TRY", "JPY", "AUD", "CHF", "CAD", "USD", "NZD", "CNH",
    "CNY", "HKD", "SGD", "KRW", "MXN", "ZAR", "BRL", "DXY", "U",
}

# Металлы, нефть, газ, сырьё, акции, ETF и индексы (статический резервный список)
ROBOT_TRADFI_STATIC: Set[str] = {
    # Металлы
    "XAU", "XAG", "XPT", "XPD", "PAXG", "XAUT", "GOLD", "SILVER", "COPPER",
    "PLATINUM", "PALLADIUM", "XCU", "XAL", "XNI", "XZN", "SLV", "GLD", "IAU",
    "GDX", "GDXJ", "PPLT", "PALL", "CPER", "HG", "GC", "SI",
    # Нефть, газ и сырьё
    "USOIL", "UKOIL", "WTI", "BRENT", "CRUDE", "OIL", "CL", "USO", "BNO",
    "NATGAS", "NGAS", "UNG", "BOIL", "KOLD", "GAS", "CORN", "WHEAT", "SOYB",
    "SUGAR", "COFFEE", "COCOA", "COTTON", "URANIUM", "URA", "XLE",
    # Индексы и ETF
    "SPY", "QQQ", "DIA", "IWM", "VOO", "VTI", "TQQQ", "SQQQ", "SOXL", "SOXS",
    "UPRO", "SPXU", "UVXY", "VIX", "TLT", "TMF", "HYG", "LQD", "EEM", "FXI",
    "KWEB", "ARKK", "XLF", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "SMH",
    "US500", "SPX", "SPX500", "NAS100", "NDX", "USTEC", "US30", "DJI", "DJ30",
    "US2000", "RUT", "GER40", "DAX", "DE40", "UK100", "FTSE", "JP225", "NIKKEI",
    "HK50", "HSI", "FR40", "EU50", "CN50", "A50", "MAG7", "FAANG",
    # Популярные акции и токенизированные акции
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOG", "GOOGL", "META", "NFLX",
    "AMD", "INTC", "AVGO", "QCOM", "MU", "TSM", "ASML", "ARM", "ORCL", "CRM",
    "ADBE", "CSCO", "IBM", "NOW", "SNOW", "PLTR", "APP", "SMCI", "DELL", "HPE",
    "COIN", "MSTR", "HOOD", "MARA", "RIOT", "CLSK", "CRCL", "SQ", "XYZ", "PYPL",
    "V", "MA", "JPM", "BAC", "GS", "MS", "C", "WFC", "BLK", "SCHW", "BRK",
    "BRKA", "BRKB", "BABA", "JD", "PDD", "BIDU", "NIO", "XPEV", "LI", "BILI",
    "FUTU", "TCEHY", "DIS", "CMCSA", "SPOT", "ROKU", "RBLX", "UBER", "LYFT",
    "ABNB", "DASH", "SHOP", "SE", "MELI", "WMT", "COST", "TGT", "HD", "LOW",
    "NKE", "SBUX", "MCD", "KO", "PEP", "PG", "JNJ", "PFE", "MRNA", "LLY",
    "NVO", "UNH", "ABBV", "MRK", "BMY", "GILD", "AMGN", "XOM", "CVX", "OXY",
    "SLB", "COP", "BA", "LMT", "RTX", "GE", "CAT", "DE", "HON", "UPS", "FDX",
    "F", "GM", "TM", "RIVN", "LCID", "GME", "AMC", "DJT", "RDDT", "SNAP",
    "PINS", "ZM", "DOCU", "OKTA", "CRWD", "PANW", "ZS", "NET", "DDOG", "MDB",
    "U", "TTD", "AFRM", "SOFI", "UPST", "NU", "SVV", "SNDK", "SNDKB", "WDC",
    "STX", "ANET", "VRT", "VST", "CEG", "OKLO", "SMR", "IONQ", "RGTI", "QBTS",
    "RKLB", "ASTS", "LUNR", "HIMS", "TEM", "CVNA", "CAVA", "CMG", "DUOL",
}

# Динамически пополняемые списки TradFi/RWA/акций/металлов/нефти из API 6 бирж
_dynamic_tradfi_pairs: Set[Tuple[str, str]] = set()
_dynamic_tradfi_bases: Set[str] = set()

_TRADFI_KEYWORDS = (
    "STOCK", "EQUITY", "EQUITIES", "RWA", "TRADFI", "TRADIFI", "METAL",
    "GOLD", "SILVER", "OIL", "COMMODIT", "FOREX", "FX", "ETF", "INDEX",
    "INDICES", "SHARE", "ENERGY", "PRECIOUS",
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


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        if val is None or val == "":
            return default
        return int(float(val))
    except (TypeError, ValueError):
        return default


def _extract_base(symbol: str) -> str:
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


def _normalize_pair_key(symbol: str) -> str:
    s = (symbol or "").upper().strip()
    s = s.replace("-USDT-SWAP", "USDT").replace("_USDT", "USDT").replace("-USDT", "USDT")
    return s


def is_robot_blacklisted_symbol(symbol: str, exchange: str = "") -> bool:
    """
    Строгий фильтр для Покупаш / Продаваш:
    отсекает акции, RWA, ETF, индексы, металлы, нефть/газ, форекс,
    тяжёлые монеты и стейблкоины.
    """
    raw_up = (symbol or "").upper().strip()
    if not raw_up:
        return True

    pair_key = _normalize_pair_key(raw_up)
    if exchange and (exchange, pair_key) in _dynamic_tradfi_pairs:
        return True
    if ("", pair_key) in _dynamic_tradfi_pairs:
        return True

    # Префикс r* (токенизированные акции RWA на Bitget, например rSVVUSDT)
    if re.match(r"^R[A-Z]{2,6}(USDT|USDC|_USDT|-USDT)", raw_up) and (
        symbol.startswith("r") or _extract_base(raw_up)[1:] in ROBOT_TRADFI_STATIC
    ):
        return True

    base = _extract_base(raw_up)
    if not base:
        return True

    clean = base.replace(".", "")
    if base in ROBOT_HEAVY_AND_STABLE or clean in ROBOT_HEAVY_AND_STABLE:
        return True
    if base in ROBOT_TRADFI_STATIC or clean in ROBOT_TRADFI_STATIC:
        return True
    if base in _dynamic_tradfi_bases or clean in _dynamic_tradfi_bases:
        return True

    # Явные маркеры металлов, нефти, форекса, индексов
    if any(tok in clean for tok in ("XAU", "XAG", "XPT", "XPD", "OIL", "BRENT", "WTI", "GOLD", "SILVER", "NATGAS", "US500", "NAS100")):
        return True

    # Суффиксы токенизированных акций / плечевых деривативов (*X, *ON, *2L/*3S и т.д.)
    for suf in ("ON", "X", "2L", "2S", "3L", "3S", "5L", "5S"):
        if len(clean) >= len(suf) + 2 and clean.endswith(suf):
            stem = clean[: -len(suf)]
            if stem in ROBOT_TRADFI_STATIC or stem in ROBOT_HEAVY_AND_STABLE or stem in _dynamic_tradfi_bases:
                return True

    return False


def _has_tradfi_keyword(text: Any) -> bool:
    up = str(text or "").upper()
    if not up:
        return False
    return any(k in up for k in _TRADFI_KEYWORDS)


async def refresh_tradfi_metadata(session: aiohttp.ClientSession) -> None:
    """
    Загружает официальные метаданные фьючерсов со всех 6 бирж и автоматически
    пополняет чёрный список роботов всеми акциями, RWA, ETF, металлами, нефтью и форексом.
    """
    new_pairs: Set[Tuple[str, str]] = set()
    new_bases: Set[str] = set()

    def mark_tradfi(ex: str, sym: str) -> None:
        if not sym:
            return
        pk = _normalize_pair_key(sym)
        if pk:
            new_pairs.add((ex, pk))
            new_pairs.add(("", pk))
        b = _extract_base(sym)
        if b and len(b) >= 2:
            new_bases.add(b)
            new_bases.add(b.replace(".", ""))

    async def fetch_bitget() -> None:
        try:
            url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            for it in data.get("data") or []:
                sym = str(it.get("symbol") or "")
                is_rwa = str(it.get("isRwa") or "").upper() == "YES"
                if (
                    is_rwa
                    or _has_tradfi_keyword(it.get("symbolType"))
                    or _has_tradfi_keyword(it.get("category"))
                    or _has_tradfi_keyword(it.get("baseCoin"))
                ):
                    mark_tradfi("Bitget", sym)
        except Exception as exc:
            logger.debug("[TradFi] Bitget metadata skip: %s", exc)

    async def fetch_binance_like(ex_name: str, url: str) -> None:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            for s in data.get("symbols") or []:
                sym = str(s.get("symbol") or "")
                ctype = str(s.get("contractType") or "").upper()
                utype = str(s.get("underlyingType") or "").upper()
                sub = " ".join(str(x) for x in (s.get("underlyingSubType") or []))
                if (
                    _has_tradfi_keyword(ctype)
                    or (utype and utype != "COIN")
                    or _has_tradfi_keyword(sub)
                ):
                    mark_tradfi(ex_name, sym)
        except Exception as exc:
            logger.debug("[TradFi] %s metadata skip: %s", ex_name, exc)

    async def fetch_bybit() -> None:
        try:
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            for it in (data.get("result") or {}).get("list") or []:
                sym = str(it.get("symbol") or "")
                if (
                    _has_tradfi_keyword(it.get("symbolType"))
                    or _has_tradfi_keyword(it.get("contractType"))
                    or _has_tradfi_keyword(it.get("displayName"))
                ):
                    mark_tradfi("Bybit", sym)
        except Exception as exc:
            logger.debug("[TradFi] Bybit metadata skip: %s", exc)

    async def fetch_okx() -> None:
        try:
            url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            for it in data.get("data") or []:
                iid = str(it.get("instId") or "")
                cat = str(it.get("instCategory") or "")
                # На OKX instCategory "1" = Crypto, остальные категории = TradFi/акции/сырьё
                if (cat and cat != "1") or _has_tradfi_keyword(it.get("instFamily")):
                    mark_tradfi("OKX", iid)
        except Exception as exc:
            logger.debug("[TradFi] OKX metadata skip: %s", exc)

    async def fetch_gate() -> None:
        try:
            url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            if isinstance(data, list):
                for it in data:
                    c = str(it.get("name") or "")
                    if (
                        _has_tradfi_keyword(it.get("contract_type"))
                        or _has_tradfi_keyword(it.get("category"))
                        or _has_tradfi_keyword(it.get("tag"))
                    ):
                        mark_tradfi("Gate", c)
        except Exception as exc:
            logger.debug("[TradFi] Gate metadata skip: %s", exc)

    await asyncio.gather(
        fetch_bitget(),
        fetch_binance_like("Binance", "https://fapi.binance.com/fapi/v1/exchangeInfo"),
        fetch_binance_like("Aster", "https://fapi.asterdex.com/fapi/v1/exchangeInfo"),
        fetch_bybit(),
        fetch_okx(),
        fetch_gate(),
        return_exceptions=True,
    )

    if new_pairs or new_bases:
        _dynamic_tradfi_pairs.update(new_pairs)
        _dynamic_tradfi_bases.update(new_bases)
        logger.info(
            "[TradFi] Авто-фильтр обновлён: %s контрактов (%s базовых тикеров) исключено из роботов.",
            len(_dynamic_tradfi_pairs) // 2,
            len(_dynamic_tradfi_bases),
        )


def normalize_robot_config(raw: Any, fallback_min_total: Any = None) -> Dict[str, Any]:
    """Нормализует пользовательские настройки Покупаша / Продаваша в безопасные границы."""
    cfg = dict(DEFAULT_ROBOT_CONFIG)
    data: Dict[str, Any] = {}
    if isinstance(raw, dict):
        data = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = {}

    if fallback_min_total is not None and "min_total_usd" not in data:
        fb = _safe_float(fallback_min_total, DEFAULT_MIN_ROBOT_USD)
        if fb > 0:
            data["min_total_usd"] = fb

    cfg["window_sec"] = round(min(120.0, max(5.0, _safe_float(data.get("window_sec"), cfg["window_sec"]))), 1)
    cfg["min_print_usd"] = round(max(100.0, _safe_float(data.get("min_print_usd"), cfg["min_print_usd"])), 2)
    max_p = max(0.0, _safe_float(data.get("max_print_usd"), cfg["max_print_usd"]))
    if max_p > 0.0 and max_p < cfg["min_print_usd"]:
        max_p = cfg["min_print_usd"]
    cfg["max_print_usd"] = round(max_p, 2)

    cfg["min_total_usd"] = round(max(2000.0, _safe_float(data.get("min_total_usd"), cfg["min_total_usd"])), 2)
    cfg["min_prints"] = max(3, min(100, _safe_int(data.get("min_prints"), cfg["min_prints"])))

    min_i = round(min(60.0, max(0.1, _safe_float(data.get("min_interval_sec"), cfg["min_interval_sec"]))), 2)
    max_i = round(min(120.0, max(0.3, _safe_float(data.get("max_interval_sec"), cfg["max_interval_sec"]))), 2)
    if max_i < min_i:
        max_i = min_i
    cfg["min_interval_sec"] = min_i
    cfg["max_interval_sec"] = max_i

    cfg["min_dominance"] = round(min(99.0, max(55.0, _safe_float(data.get("min_dominance"), cfg["min_dominance"]))), 1)
    cfg["min_vol24h"] = round(max(0.0, _safe_float(data.get("min_vol24h"), cfg["min_vol24h"])), 2)
    cfg["cooldown_sec"] = round(min(3600.0, max(15.0, _safe_float(data.get("cooldown_sec"), cfg["cooldown_sec"]))), 1)

    side_mode = str(data.get("side_mode") or cfg["side_mode"]).strip().upper()
    cfg["side_mode"] = side_mode if side_mode in ("BOTH", "BUY", "SELL") else "BOTH"
    cfg["strict_pattern"] = bool(data.get("strict_pattern", cfg["strict_pattern"]))

    raw_ex = data.get("exchanges")
    if isinstance(raw_ex, list):
        valid_ex = [str(e) for e in raw_ex if str(e) in ALL_ROBOT_EXCHANGES]
        cfg["exchanges"] = valid_ex if valid_ex else list(ALL_ROBOT_EXCHANGES)
    else:
        cfg["exchanges"] = list(ALL_ROBOT_EXCHANGES)

    return cfg


def robot_matches_user_config(
    exchange: str,
    side: str,
    total_usd: float,
    prints_count: int,
    avg_print_usd: float,
    avg_interval_sec: float,
    dominance_pct: float,
    volume_24h_usd: float,
    cfg: Dict[str, Any],
) -> bool:
    """Проверяет, соответствует ли событие Покупаша/Продаваша фильтрам пользователя."""
    if exchange not in cfg.get("exchanges", ALL_ROBOT_EXCHANGES):
        return False
    side_mode = cfg.get("side_mode", "BOTH")
    if side_mode == "BUY" and side != "BUY":
        return False
    if side_mode == "SELL" and side != "SELL":
        return False
    if total_usd < float(cfg.get("min_total_usd", DEFAULT_MIN_ROBOT_USD)):
        return False
    if prints_count < int(cfg.get("min_prints", 6)):
        return False
    if avg_print_usd < float(cfg.get("min_print_usd", 1000.0)):
        return False
    max_p = float(cfg.get("max_print_usd", 0.0))
    if max_p > 0.0 and avg_print_usd > max_p:
        return False
    if avg_interval_sec < float(cfg.get("min_interval_sec", 0.35)):
        return False
    if avg_interval_sec > float(cfg.get("max_interval_sec", 6.0)):
        return False
    if dominance_pct < float(cfg.get("min_dominance", 78.0)):
        return False
    if volume_24h_usd < float(cfg.get("min_vol24h", 300000.0)):
        return False
    return True


# ==================== ДВИЖОК ПОКУПАШ / ПРОДАВАШ ====================

class RobotEngine:
    """
    Детектирует алгоритмических маркет-роботов (Покупаш / Продаваш) по WebSocket-ленте сделок.
    Использует двухэтапное подтверждение, строгий кластерный поиск лота/ритма, защиту от одиночных
    ударов, «пилы» и дублей одной монеты сразу с нескольких бирж.
    """

    def __init__(self, on_robot: Callable[[RobotEvent], Any]) -> None:
        self.on_robot = on_robot
        self.config: Dict[str, Any] = dict(DEFAULT_ROBOT_CONFIG)
        # (exchange, symbol) -> deque of [ts, is_buy, usd, price, qty_base]
        self._prints: Dict[Tuple[str, str], Deque[List[Any]]] = {}
        self._last_eval: Dict[Tuple[str, str], float] = {}
        # Кулдауны: по бирже+монете+стороне, кросс-биржевой по базовой монете и анти-пила по паре
        self._cooldown: Dict[Tuple[str, str, RobotSide], float] = {}
        self._coin_cooldown: Dict[Tuple[str, RobotSide], float] = {}
        self._pair_last_alert: Dict[Tuple[str, str], float] = {}
        # Двухэтапное подтверждение: (exchange, symbol, side) -> (first_seen_ts, last_print_ts)
        self._pending: Dict[Tuple[str, str, RobotSide], Tuple[float, float]] = {}
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

    @staticmethod
    def _base_key(symbol: str) -> str:
        """Быстро выделяет базовый тикер монеты для кросс-биржевого антиспама."""
        return _extract_base(symbol)

    @staticmethod
    def _collapse_bursts(prints: List[List[Any]], min_gap_sec: float = 0.12) -> List[List[Any]]:
        """Схлопывает микро-филлы одного удара в пределах min_gap_sec в один принт."""
        if not prints:
            return []
        merged: List[List[Any]] = [list(prints[0])]
        for p in prints[1:]:
            if (p[0] - merged[-1][0]) < min_gap_sec:
                merged[-1][2] += p[2]
                merged[-1][3] = p[3]
                merged[-1][4] += p[4]
            else:
                merged.append(list(p))
        return merged

    @staticmethod
    def _find_best_lot_cluster(prints: List[List[Any]]) -> List[List[Any]]:
        """
        Находит самую крупную серию ударов с одинаковым размером лота:
        допуск ±16% по USD или ±12% по количеству базовой монеты (фиксированный лот).
        """
        best: List[List[Any]] = []
        best_usd = 0.0
        for anchor in prints:
            a_usd = anchor[2]
            a_qty = anchor[4]
            if a_usd <= 0:
                continue
            cand = [
                p
                for p in prints
                if (0.84 * a_usd <= p[2] <= 1.16 * a_usd)
                or (a_qty > 0 and p[4] > 0 and 0.88 * a_qty <= p[4] <= 1.12 * a_qty)
            ]
            cand_usd = sum(p[2] for p in cand)
            if len(cand) > len(best) or (len(cand) == len(best) and cand_usd > best_usd):
                best = cand
                best_usd = cand_usd
        return best

    @staticmethod
    def _analyze_sequence(
        seq: List[List[Any]],
        now_ts: float,
        min_prints: int,
        min_total_usd: float,
        min_lot: float,
        max_lot: float,
        min_int: float,
        max_int: float,
        strict_span: bool = True,
    ) -> Optional[Tuple[List[List[Any]], float, float, float, float, float]]:
        """
        Проверяет серию принтов по сумме, кол-ву ударов, свежести, отсутствию дыр/одиночных шпилек и ритму.
        Возвращает (seq, total_usd, avg_print_usd, med_int, rhythm_ratio, cv_int) или None.
        """
        if len(seq) < min_prints:
            return None
        total_usd = sum(p[2] for p in seq)
        if total_usd < min_total_usd:
            return None

        # Защита от ложного сигнала, где 1 крупный рыночный ордер делает почти весь объём
        max_single = max(p[2] for p in seq)
        if max_single / total_usd > 0.42:
            return None

        avg_print = total_usd / len(seq)
        if avg_print < min_lot or (max_lot > 0.0 and avg_print > max_lot):
            return None

        # Робот должен быть активен прямо сейчас, а не закончиться 15 секунд назад
        if (now_ts - seq[-1][0]) > max(max_int * 1.6, 5.0):
            return None

        intervals = [seq[i][0] - seq[i - 1][0] for i in range(1, len(seq))]
        if not intervals:
            return None

        span = seq[-1][0] - seq[0][0]
        # Защита от коротких прострелов и каскадов ликвидаций
        min_span = max(5.0 if strict_span else 2.5, min_int * (min_prints - 1) * (0.70 if strict_span else 0.45))
        if span < min_span:
            return None

        # Отсекаем серии с длинной «мёртвой паузой» между случайными всплесками
        if max(intervals) > max(max_int * 2.6, 12.0):
            return None

        med_int = statistics.median(intervals)
        if med_int < min_int or med_int > max_int:
            return None

        mean_int = sum(intervals) / len(intervals)
        if len(intervals) >= 3 and mean_int > 0:
            cv_int = statistics.pstdev(intervals) / mean_int
        else:
            cv_int = 1.0

        # Доля интервалов, попадающих в ровный ритм (±40% от медианного шага)
        rhythm_hits = sum(1 for iv in intervals if 0.60 * med_int <= iv <= 1.45 * med_int)
        rhythm_ratio = rhythm_hits / len(intervals)
        return seq, total_usd, avg_print, med_int, rhythm_ratio, cv_int

    async def process_trade(
        self,
        exchange: str,
        symbol: str,
        is_buy: bool,
        price: float,
        qty_usd: float,
        ts_sec: float,
        volume_24h_usd: float,
        qty_base: float = 0.0,
    ) -> None:
        if price <= 0 or qty_usd <= 0 or is_robot_blacklisted_symbol(symbol, exchange):
            return

        cfg = self.config
        if exchange not in cfg["exchanges"]:
            return

        now = time.time()
        ts = ts_sec if abs(now - ts_sec) < 15.0 else now
        base_qty = qty_base if qty_base > 0 else (qty_usd / price)
        key = (exchange, symbol)
        dq = self._prints.get(key)
        if dq is None:
            dq = deque(maxlen=800)
            self._prints[key] = dq

        # Склеиваем только дробление одного рыночного ордера (<= 80мс), не трогая соседние сделки толпы
        if dq and dq[-1][1] == is_buy and (ts - dq[-1][0]) <= 0.08:
            dq[-1][2] += qty_usd
            dq[-1][3] = price
            dq[-1][4] += base_qty
        else:
            dq.append([ts, is_buy, qty_usd, price, base_qty])

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
        raw_sig = [
            p
            for p in win_prints
            if p[1] == side_is_buy
            and p[2] >= min_lot
            and (max_lot <= 0.0 or p[2] <= max_lot)
        ]
        sig_prints = self._collapse_bursts(raw_sig, min_gap_sec=0.12)
        if len(sig_prints) < min_prints:
            return

        min_int = float(cfg["min_interval_sec"])
        max_int = float(cfg["max_interval_sec"])
        strict_mode = bool(cfg["strict_pattern"])

        detected: Optional[Tuple[List[List[Any]], float, float, float]] = None

        if strict_mode:
            # 1) Приоритет: ищем кластер ударов одинаковым лотом (±16% USD или ±12% монет)
            cluster = self._find_best_lot_cluster(sig_prints)
            res_cluster = self._analyze_sequence(
                cluster, ts, min_prints, min_total_usd, min_lot, max_lot, min_int, max_int, strict_span=True
            )
            if res_cluster is not None:
                c_seq, c_usd, c_avg, c_med_int, c_rhythm, c_cv = res_cluster
                lot_purity = len(c_seq) / len(sig_prints)
                # Робот должен обеспечивать весомую долю объёма стороны (>= 35%), а не тонуть в шуме толпы
                vol_share = c_usd / side_usd if side_usd > 0 else 0.0
                if vol_share >= 0.35:
                    med_usd = statistics.median(p[2] for p in c_seq)
                    med_qty = statistics.median(p[4] for p in c_seq)
                    tight_cnt = sum(
                        1
                        for p in c_seq
                        if (0.90 * med_usd <= p[2] <= 1.10 * med_usd)
                        or (med_qty > 0 and 0.93 * med_qty <= p[4] <= 1.07 * med_qty)
                    )
                    tight_ratio = tight_cnt / len(c_seq)

                    is_exact_lot_bot = tight_ratio >= 0.70 and c_rhythm >= 0.55 and c_cv <= 0.50
                    is_rhythmic_lot_bot = (c_rhythm >= 0.70 or c_cv <= 0.35) and lot_purity >= 0.55
                    is_dense_lot_bot = len(c_seq) >= max(min_prints + 3, 9) and lot_purity >= 0.75 and c_cv <= 0.45

                    if is_exact_lot_bot or is_rhythmic_lot_bot or is_dense_lot_bot:
                        detected = (c_seq, c_usd, c_avg, c_med_int)

            # 2) Резерв для TWAP-роботов со слегка плавающим лотом (±30%), но очень ровным шагом
            if detected is None:
                res_twap = self._analyze_sequence(
                    sig_prints, ts, min_prints, min_total_usd, min_lot, max_lot, min_int, max_int, strict_span=True
                )
                if res_twap is not None:
                    t_seq, t_usd, t_avg, t_med_int, t_rhythm, t_cv = res_twap
                    vol_share = t_usd / side_usd if side_usd > 0 else 0.0
                    med_usd = statistics.median(p[2] for p in t_seq)
                    sim_ratio = sum(1 for p in t_seq if 0.70 * med_usd <= p[2] <= 1.30 * med_usd) / len(t_seq)
                    if (
                        vol_share >= 0.45
                        and len(t_seq) >= max(6, min_prints)
                        and (t_cv <= 0.30 or t_rhythm >= 0.75)
                        and sim_ratio >= 0.65
                    ):
                        detected = (t_seq, t_usd, t_avg, t_med_int)
        else:
            # Мягкий режим (строгий ритм/лоты выключен)
            res_soft = self._analyze_sequence(
                sig_prints, ts, min_prints, min_total_usd, min_lot, max_lot, min_int, max_int, strict_span=False
            )
            if res_soft is not None:
                s_seq, s_usd, s_avg, s_med_int, _, _ = res_soft
                if side_usd > 0 and (s_usd / side_usd) >= 0.30:
                    detected = (s_seq, s_usd, s_avg, s_med_int)

        if detected is None:
            return

        final_prints, matched_usd, avg_print, med_int = detected
        cd_key = (exchange, symbol, side)
        last_print_ts = float(final_prints[-1][0])

        # Двухэтапное подтверждение: ждём минимум 1 новый удар робота после первого обнаружения,
        # чтобы полностью отсечь одноразовые случайные всплески толпы.
        pending_ttl = max(window_sec, max_int * 2.2, 30.0)
        min_confirm_wait = min(3.0, max(1.2, med_int * 0.7))
        pend = self._pending.get(cd_key)
        if pend is None or (now - pend[0]) > pending_ttl:
            self._pending[cd_key] = (now, last_print_ts)
            return
        if last_print_ts <= pend[1] + 0.05 or (now - pend[0]) < min_confirm_wait:
            return
        self._pending.pop(cd_key, None)

        self._active[key] = (
            side,
            now,
            matched_usd,
            len(final_prints),
            med_int,
            avg_print,
            round(dominance_pct, 1),
            volume_24h_usd,
        )

        cooldown_sec = float(cfg["cooldown_sec"])
        base_coin = self._base_key(symbol)
        cross_key = (base_coin, side)
        # Кросс-биржевой антиспам: не дублируем одну и ту же монету одновременно со всех 6 бирж
        cross_cd = min(cooldown_sec * 0.5, 90.0)
        # Анти-пила: если только что был сигнал по монете на бирже, пауза перед сменой стороны
        whipsaw_cd = min(cooldown_sec * 0.4, 60.0)

        if (
            now - self._cooldown.get(cd_key, 0.0) >= cooldown_sec
            and now - self._coin_cooldown.get(cross_key, 0.0) >= cross_cd
            and now - self._pair_last_alert.get(key, 0.0) >= whipsaw_cd
        ):
            self._cooldown[cd_key] = now
            self._coin_cooldown[cross_key] = now
            self._pair_last_alert[key] = now
            ev = RobotEvent(
                exchange=exchange,
                symbol=symbol,
                side=side,
                price=float(final_prints[-1][3]),
                total_usd=matched_usd,
                prints_count=len(final_prints),
                avg_print_usd=avg_print,
                avg_interval_sec=round(med_int, 2),
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
        stale_pend = [k for k, (first_ts, _) in self._pending.items() if now - first_ts > 120]
        for k in stale_pend:
            self._pending.pop(k, None)
        return [
            (ex, sym, side, usd, cnt, avg_int, avg_print, dom, vol)
            for (ex, sym), (side, _, usd, cnt, avg_int, avg_print, dom, vol) in self._active.items()
        ]


# ==================== WEBSOCKET-СКАНЕРЫ ПОКУПАШ / ПРОДАВАШ (6 БИРЖ) ====================

async def _tradfi_refresher_loop(session: aiohttp.ClientSession) -> None:
    """Раз в 30 минут обновляет списки TradFi/RWA/акций/металлов/нефти со всех 6 бирж."""
    while True:
        await asyncio.sleep(1800)
        try:
            await refresh_tradfi_metadata(session)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[TradFi] Ошибка фонового обновления: %s", exc)


async def _binance_style_robot_ws(
    session: aiohttp.ClientSession,
    exchange_name: str,
    rest_base: str,
    ws_base: str,
    rob_eng: RobotEngine,
) -> None:
    """WebSocket-лента сделок aggTrade для Binance Futures и Aster Futures."""
    while True:
        try:
            async with session.get(f"{rest_base}/fapi/v1/ticker/24hr", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    await asyncio.sleep(10)
                    continue
                tickers = await resp.json()

            vols: Dict[str, float] = {}
            for it in tickers if isinstance(tickers, list) else []:
                sym = str(it.get("symbol") or "")
                if not sym.endswith("USDT") or is_robot_blacklisted_symbol(sym, exchange_name):
                    continue
                v = _safe_float(it.get("quoteVolume"))
                if v >= 100_000:
                    vols[sym] = v

            top_syms = sorted(vols.keys(), key=lambda s: vols[s], reverse=True)[:120]
            if not top_syms:
                await asyncio.sleep(15)
                continue

            streams = "/".join(f"{s.lower()}@aggTrade" for s in top_syms)
            ws_url = f"{ws_base}/stream?streams={streams}"
            started = time.time()

            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
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
                    if not sym or px <= 0 or qty <= 0:
                        continue
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
                        qty_base=qty,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[%s Robot WS] Переподключение через 5с: %s", exchange_name, exc)
            await asyncio.sleep(5)


async def bybit_robot_ws(session: aiohttp.ClientSession, rob_eng: RobotEngine) -> None:
    """WebSocket-лента сделок publicTrade для Bybit Linear USDT."""
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
                if v >= 100_000:
                    vols[sym] = v

            top_syms = sorted(vols.keys(), key=lambda s: vols[s], reverse=True)[:100]
            if not top_syms:
                await asyncio.sleep(15)
                continue

            started = time.time()
            async with session.ws_connect("wss://stream.bybit.com/v5/public/linear", heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top_syms), 10):
                    args = [f"publicTrade.{s}" for s in top_syms[i : i + 10]]
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info("[Bybit Robot WS] Подписан на %s пар", len(top_syms))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send_json({"op": "ping"})
                        except Exception:
                            break

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
                            if not sym or px <= 0 or qty <= 0:
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
                                qty_base=qty,
                            )
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Bybit Robot WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


async def okx_robot_ws(session: aiohttp.ClientSession, rob_eng: RobotEngine) -> None:
    """WebSocket-лента сделок trades для OKX USDT-SWAP."""
    while True:
        try:
            ct_vals: Dict[str, float] = {}
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

            vols: Dict[str, float] = {}
            for it in tk_data.get("data") or []:
                iid = str(it.get("instId") or "")
                if not iid.endswith("-USDT-SWAP") or is_robot_blacklisted_symbol(iid, "OKX"):
                    continue
                v = _safe_float(it.get("volCcy24h")) * _safe_float(it.get("last"))
                if v >= 100_000:
                    vols[iid] = v

            top_ids = sorted(vols.keys(), key=lambda s: vols[s], reverse=True)[:80]
            if not top_ids:
                await asyncio.sleep(15)
                continue

            started = time.time()
            async with session.ws_connect("wss://ws.okx.com:8443/ws/v5/public", heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top_ids), 20):
                    args = [{"channel": "trades", "instId": iid} for iid in top_ids[i : i + 20]]
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info("[OKX Robot WS] Подписан на %s пар", len(top_ids))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send_str("ping")
                        except Exception:
                            break

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
                                qty_base=sz * ct,
                            )
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[OKX Robot WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


async def bitget_robot_ws(session: aiohttp.ClientSession, rob_eng: RobotEngine) -> None:
    """WebSocket-лента сделок trade для Bitget USDT-FUTURES."""
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
                if v >= 100_000:
                    vols[sym] = v

            top_syms = sorted(vols.keys(), key=lambda s: vols[s], reverse=True)[:80]
            if not top_syms:
                await asyncio.sleep(15)
                continue

            started = time.time()
            async with session.ws_connect("wss://ws.bitget.com/v2/ws/public", heartbeat=20, timeout=15) as ws:
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
                        try:
                            await ws.send_str("ping")
                        except Exception:
                            break

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
                        arg = payload.get("arg") or {}
                        if arg.get("channel") != "trade" or payload.get("action") != "update":
                            continue
                        sym = str(arg.get("instId") or "")
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
                                qty_base=sz,
                            )
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Bitget Robot WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


async def gate_robot_ws(session: aiohttp.ClientSession, rob_eng: RobotEngine) -> None:
    """WebSocket-лента сделок futures.trades для Gate.io USDT Perpetual."""
    while True:
        try:
            q_mults: Dict[str, float] = {}
            async with session.get(
                "https://api.gateio.ws/api/v4/futures/usdt/contracts",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                c_data = await resp.json()
            if isinstance(c_data, list):
                for it in c_data:
                    name = str(it.get("name") or "")
                    if name:
                        q_mults[name] = _safe_float(it.get("quanto_multiplier"), 1.0)

            async with session.get(
                "https://api.gateio.ws/api/v4/futures/usdt/tickers",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                tk_data = await resp.json()

            vols: Dict[str, float] = {}
            if isinstance(tk_data, list):
                for it in tk_data:
                    c = str(it.get("contract") or "")
                    if not c.endswith("_USDT") or is_robot_blacklisted_symbol(c, "Gate"):
                        continue
                    v = _safe_float(it.get("volume_24h_quote")) or _safe_float(it.get("volume_24h_settle"))
                    if v >= 100_000:
                        vols[c] = v

            top_contracts = sorted(vols.keys(), key=lambda s: vols[s], reverse=True)[:80]
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

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send_json({"time": int(time.time()), "channel": "futures.ping"})
                        except Exception:
                            break

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
                            base_qty = abs(size_raw) * mult
                            qty_usd = base_qty * px
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
                                qty_base=base_qty,
                            )
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Gate Robot WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


def create_robot_tasks(session: aiohttp.ClientSession, rob_eng: RobotEngine) -> List[asyncio.Task[Any]]:
    """Запускает 6 независимых WebSocket-сканеров Покупаш / Продаваш и авто-обновление TradFi-фильтра."""
    return [
        asyncio.create_task(_tradfi_refresher_loop(session)),
        asyncio.create_task(
            _binance_style_robot_ws(
                session,
                "Binance",
                "https://fapi.binance.com",
                "wss://fstream.binance.com",
                rob_eng,
            )
        ),
        asyncio.create_task(
            _binance_style_robot_ws(
                session,
                "Aster",
                "https://fapi.asterdex.com",
                "wss://fstream.asterdex.com",
                rob_eng,
            )
        ),
        asyncio.create_task(bybit_robot_ws(session, rob_eng)),
        asyncio.create_task(okx_robot_ws(session, rob_eng)),
        asyncio.create_task(bitget_robot_ws(session, rob_eng)),
        asyncio.create_task(gate_robot_ws(session, rob_eng)),
    ]