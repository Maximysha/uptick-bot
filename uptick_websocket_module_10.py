"""
Независимый модуль WebSocket-детектора «Аптиков» (Binance, Bybit, OKX, Aster).

Точная механика ценового коридора (планки биржи):
- Аптик фиксируется строго в момент, когда цена упирается в актуальную границу
  ценового коридора биржи (upper_limit / lower_limit; buyLmt / sellLmt) и
  рыночные заявки в эту сторону перестают исполняться:
    * LONG  — best_ask >= upper_limit (в пределах 0.03% / 1.5 тика), а best_bid прижат к границе.
    * SHORT — best_bid <= lower_limit (в пределах 0.03% / 1.5 тика), а best_ask прижат к границе.
- Перед отправкой КАЖДОГО алерта выполняется мгновенная параллельная REST-сверка
  актуальной границы коридора и реальной глубины стакана (топ-10 уровней у планки)
  прямо на бирже — это убирает и ложные срабатывания после вылета, и пустые
  касания без плотности (вроде $85).
- Акции, ETF, индексы, металлы и нефть/газ РАЗРЕШЕНЫ и подписываются в приоритете;
  блокируются только тяжеловесы крипто-рынка и стейблкоины.
"""

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import aiohttp

logger = logging.getLogger("uptick_bot.upticks")

# ==================== КОНФИГ АПТИКОВ ====================

DEFAULT_MIN_VOLUME = 500_000.0
UPTICK_PREFILTER_PCT = 0.08       # Предварительный WS-порог близости к планке перед живой сверкой (%)
UPTICK_TOUCH_PCT = 0.03           # Точный допуск касания границы коридора на живой сверке (%)
UPTICK_MAX_SPREAD_PCT = 0.25      # Макс. отрыв противоположной стороны от планки (отсекает пустой спред)
UPTICK_WALL_BAND_PCT = 0.25       # Зона подсчёта реальной плотности вокруг планки (±%)
try:
    UPTICK_MIN_WALL_USD = max(0.0, float(os.getenv("UPTICK_MIN_WALL_USD") or 1_000.0))
except ValueError:
    UPTICK_MIN_WALL_USD = 1_000.0  # Мин. реальная плотность в стакане у планки ($)
UPTICK_COOLDOWN_SEC = 180.0       # 3 минуты между повторами аптика по монете
UPTICK_ACTIVE_TTL_SEC = 15.0      # Сколько секунд аптик считается активным в Лайве
ALL_UPTICK_EXCHANGES = ["Binance", "Bybit", "OKX", "Aster"]

# Явно разрешённые для Аптиков активы: металлы, нефть/газ/сырьё, акции, ETF и индексы
UPTICK_TRADFI_ALLOWED: Set[str] = {
    # Металлы
    "XAU", "XAG", "XPT", "XPD", "PAXG", "XAUT", "GOLD", "SILVER", "COPPER",
    "PLATINUM", "PALLADIUM", "XCU", "XAL", "XNI", "XZN", "SLV", "GLD", "IAU",
    "GDX", "GDXJ", "PPLT", "PALL", "CPER", "HG", "GC", "SI",
    # Нефть, газ и сырьё
    "CL", "BZ", "USOIL", "UKOIL", "WTI", "BRENT", "CRUDE", "OIL", "USO", "BNO",
    "NG", "NATGAS", "NGAS", "UNG", "BOIL", "KOLD", "GAS", "CORN", "WHEAT",
    "SOYB", "SUGAR", "COFFEE", "COCOA", "COTTON", "URANIUM", "URA", "XLE",
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
    "ANET", "VRT", "VST", "CEG", "OKLO", "SMR", "IONQ", "RGTI", "QBTS",
    "RKLB", "ASTS", "LUNR", "HIMS", "TEM", "CVNA", "CAVA", "CMG", "DUOL", "SPCX",
}

_TRADFI_KEYWORDS = (
    "STOCK", "EQUITY", "EQUITIES", "RWA", "TRADFI", "TRADIFI", "METAL",
    "GOLD", "SILVER", "OIL", "COMMODIT", "ETF", "INDEX", "INDICES",
    "SHARE", "ENERGY", "PRECIOUS", "CRUDE", "GAS", "COPPER",
)

# Динамически пополняемый список TradFi-символов (акции/металлы/нефть) прямо из API бирж
_dynamic_uptick_tradfi: Set[str] = set()

# Черный список ДЛЯ АПТИКОВ: ТОЛЬКО тяжеловесы крипто-рынка, обёрнутые токены и стейблкоины.
# Акции, ETF, металлы и нефть/газ (XAU, XAG, CL, BZ, USOIL, TSLA, NVDA и т.д.) НЕ блокируются.
HEAVY_COINS_BLACKLIST: Set[str] = {
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
    # Обернутые и ликвид-стейкинг токены
    "WBTC", "WETH", "STETH", "WSTETH", "BNSOL", "CBETH", "WBETH", "MSOL",
    "JITOSOL", "EZETH", "WEETH", "CBBTC", "TBTC",
    # Стейблкоины и фиат
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDE", "USDS", "PYUSD",
    "EURC", "EURT", "EURI", "AEUR", "USD1", "RLUSD", "USDD", "USDP", "GUSD",
    "FRAX", "LUSD", "CRVUSD", "XUSD", "USDF", "USTC", "USDG", "USDY", "SUSDE",
    "EUR", "GBP", "TRY", "JPY", "AUD", "CHF", "CAD", "USD",
}


class UptickSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


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


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _has_tradfi_keyword(text: Any) -> bool:
    up = str(text or "").upper()
    if not up:
        return False
    return any(k in up for k in _TRADFI_KEYWORDS)


def extract_base_asset(symbol: str) -> str:
    """Извлекает базовый тикер из формата любой биржи (1000PEPEUSDT, WIF-USDT-SWAP, SUI_USDT -> PEPE, WIF, SUI)."""
    s = (symbol or "").upper().strip()
    if not s:
        return ""
    for sep in ("-USDT-SWAP", "-USDC-SWAP", "-USD-SWAP", "_USDT", "-USDT", "/USDT", "_USDC", "-USDC"):
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


def _register_tradfi_symbol(symbol: str) -> None:
    """Регистрирует TradFi-контракт (акции, ETF, металлы, нефть/газ) как гарантированно разрешённый в Аптиках."""
    raw = (symbol or "").upper().strip()
    if raw:
        _dynamic_uptick_tradfi.add(raw)
        clean_pair = raw.replace("-USDT-SWAP", "USDT").replace("-USDC-SWAP", "USDC")
        _dynamic_uptick_tradfi.add(clean_pair)
    base = extract_base_asset(raw)
    if base:
        _dynamic_uptick_tradfi.add(base)
        _dynamic_uptick_tradfi.add(base.replace(".", ""))


def is_tradfi_uptick_symbol(symbol: str) -> bool:
    """Проверяет, является ли инструмент акцией, ETF, индексом, металлом или нефтью/газом."""
    raw = (symbol or "").upper().strip()
    if not raw:
        return False
    if raw in _dynamic_uptick_tradfi:
        return True
    base = extract_base_asset(raw)
    if not base:
        return False
    clean = base.replace(".", "")
    if base in UPTICK_TRADFI_ALLOWED or clean in UPTICK_TRADFI_ALLOWED:
        return True
    if base in _dynamic_uptick_tradfi or clean in _dynamic_uptick_tradfi:
        return True
    if any(tok in clean for tok in ("XAU", "XAG", "XPT", "XPD", "OIL", "BRENT", "WTI", "GOLD", "SILVER", "NATGAS", "COPPER")):
        return True
    for suf in ("X", "ON"):
        if len(clean) >= len(suf) + 2 and clean.endswith(suf):
            if clean[: -len(suf)] in UPTICK_TRADFI_ALLOWED:
                return True
    return False


def is_blacklisted_symbol(symbol: str) -> bool:
    """
    Фильтр для Аптиков: отсекает ТОЛЬКО тяжеловесов крипто-рынка и стейблкоины.
    Акции, ETF, металлы и нефть/газ всегда разрешены.
    """
    if is_tradfi_uptick_symbol(symbol):
        return False
    base = extract_base_asset(symbol)
    if not base:
        return True
    if base in HEAVY_COINS_BLACKLIST:
        return True
    clean_dot = base.replace(".", "")
    if clean_dot in HEAVY_COINS_BLACKLIST:
        return True
    for suf in ("X", "2L", "2S", "3L", "3S", "5L", "5S"):
        if len(clean_dot) >= len(suf) + 3 and clean_dot.endswith(suf):
            if clean_dot[: -len(suf)] in HEAVY_COINS_BLACKLIST:
                return True
    return False


def _detect_corridor_hit(
    best_bid: float,
    best_ask: float,
    upper_limit: float,
    lower_limit: float,
    tick_size: float = 0.0,
    touch_pct: float = UPTICK_TOUCH_PCT,
    max_spread_pct: float = UPTICK_MAX_SPREAD_PCT,
) -> Optional[Tuple[UptickSide, float, float]]:
    """
    Проверяет строгое условие Аптика (упор цены в границу ценового коридора биржи,
    когда рыночные заявки в одну сторону не могут исполниться):
      - LONG  (верхняя граница): best_ask упёрся в upper_limit или выше него,
        и best_bid прижат к upper_limit (отсекает широкий пустой спред).
      - SHORT (нижняя граница): best_bid упёрся в lower_limit или ниже него,
        и best_ask прижат к lower_limit.
    Возвращает (side, limit_price, ref_price) или None.
    """
    if best_bid <= 0 or best_ask <= 0 or best_bid > best_ask:
        return None

    if upper_limit > 0:
        touch_tol = max(upper_limit * (touch_pct / 100.0), tick_size * 1.5)
        spread_tol = max(upper_limit * (max_spread_pct / 100.0), tick_size * 3.0)
        if best_ask >= (upper_limit - touch_tol) and best_bid >= (upper_limit - spread_tol):
            return UptickSide.LONG, upper_limit, best_bid

    if lower_limit > 0:
        touch_tol = max(lower_limit * (touch_pct / 100.0), tick_size * 1.5)
        spread_tol = max(lower_limit * (max_spread_pct / 100.0), tick_size * 3.0)
        if best_bid <= (lower_limit + touch_tol) and best_ask <= (lower_limit + spread_tol):
            return UptickSide.SHORT, lower_limit, best_ask

    return None


# ==================== ЖИВАЯ СВЕРКА ГРАНИЦЫ КОРИДОРА НА БИРЖЕ ====================

async def _fetch_json(session: aiohttp.ClientSession, url: str, timeout_sec: float = 2.5) -> Optional[Any]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


def _calc_corridor_wall_usd(
    bids: List[Any],
    asks: List[Any],
    limit_price: float,
    ct_val: float = 1.0,
    band_pct: float = UPTICK_WALL_BAND_PCT,
) -> float:
    """
    Считает реальную плотность в стакане вблизи границы ценового коридора (±band_pct от limit_price)
    по топ-10 уровням со стороны BID и ASK и возвращает максимальную из сторон в USD.
    """
    if limit_price <= 0:
        return 0.0
    max_dist = limit_price * (band_pct / 100.0)

    def _sum_side(levels: List[Any]) -> float:
        total = 0.0
        for lvl in levels[:10]:
            if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                continue
            px = _safe_float(lvl[0])
            qty = _safe_float(lvl[1])
            if px <= 0 or qty <= 0:
                continue
            if abs(px - limit_price) <= max_dist:
                total += px * qty * ct_val
        return total

    return round(max(_sum_side(bids), _sum_side(asks)), 2)


async def verify_live_corridor(
    session: aiohttp.ClientSession,
    exchange: str,
    symbol: str,
    rest_base: str = "",
    up_mult: float = 0.0,
    dn_mult: float = 0.0,
    tick_size: float = 0.0,
    okx_inst_id: str = "",
    okx_ct_val: float = 1.0,
    fallback_vol24h: float = 0.0,
) -> Optional[Tuple[UptickSide, float, float, float, float, float]]:
    """
    Выполняет мгновенную параллельную REST-сверку прямо в момент касания:
    берёт АКТУАЛЬНУЮ границу коридора биржи (markPrice*mult или buyLmt/sellLmt),
    АКТУАЛЬНЫЕ best_bid / best_ask и глубину стакана (топ-20 уровней) для проверки
    реальной плотности у планки >= UPTICK_MIN_WALL_USD.
    Возвращает (side, price, limit_price, mark_price, volume_24h_usd, wall_usd) или None.
    """
    live_bid = 0.0
    live_ask = 0.0
    live_bid_usd = 0.0
    live_ask_usd = 0.0
    live_mark = 0.0
    live_upper = 0.0
    live_lower = 0.0
    live_vol = fallback_vol24h
    bids: List[Any] = []
    asks: List[Any] = []
    ct_val_for_wall = 1.0

    if exchange in ("Binance", "Aster"):
        if not rest_base or up_mult <= 1.0 or not (0.0 < dn_mult < 1.0):
            return None
        prem_url = f"{rest_base}/fapi/v1/premiumIndex?symbol={symbol}"
        depth_url = f"{rest_base}/fapi/v1/depth?symbol={symbol}&limit=20"
        prem_data, depth_data = await asyncio.gather(
            _fetch_json(session, prem_url),
            _fetch_json(session, depth_url),
        )
        if not isinstance(prem_data, dict) or not isinstance(depth_data, dict):
            return None
        live_mark = _safe_float(prem_data.get("markPrice"))
        bids = depth_data.get("bids") or []
        asks = depth_data.get("asks") or []
        if not bids or not asks:
            return None
        live_bid = _safe_float(bids[0][0])
        live_ask = _safe_float(asks[0][0])
        live_bid_usd = live_bid * _safe_float(bids[0][1])
        live_ask_usd = live_ask * _safe_float(asks[0][1])
        if live_mark <= 0 or live_bid <= 0 or live_ask <= 0:
            return None
        live_upper = live_mark * up_mult
        live_lower = live_mark * dn_mult

    elif exchange == "Bybit":
        lim_url = f"https://api.bybit.com/v5/market/price-limit?category=linear&symbol={symbol}"
        tk_url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}"
        ob_url = f"https://api.bybit.com/v5/market/orderbook?category=linear&symbol={symbol}&limit=25"
        lim_data, tk_data, ob_data = await asyncio.gather(
            _fetch_json(session, lim_url),
            _fetch_json(session, tk_url),
            _fetch_json(session, ob_url),
        )
        if not isinstance(lim_data, dict) or not isinstance(tk_data, dict):
            return None
        lim_res = lim_data.get("result") or {}
        tk_list = (tk_data.get("result") or {}).get("list") or []
        if not tk_list:
            return None
        tk = tk_list[0]
        live_upper = _safe_float(lim_res.get("buyLmt"))
        live_lower = _safe_float(lim_res.get("sellLmt"))
        live_mark = _safe_float(tk.get("markPrice"))
        live_bid = _safe_float(tk.get("bid1Price"))
        live_ask = _safe_float(tk.get("ask1Price"))
        live_bid_usd = live_bid * _safe_float(tk.get("bid1Size"))
        live_ask_usd = live_ask * _safe_float(tk.get("ask1Size"))
        v24 = _safe_float(tk.get("turnover24h"))
        if v24 > 0:
            live_vol = v24
        if isinstance(ob_data, dict):
            ob_res = ob_data.get("result") or {}
            bids = ob_res.get("b") or []
            asks = ob_res.get("a") or []

    elif exchange == "OKX":
        iid = okx_inst_id
        if not iid:
            if symbol.endswith("-SWAP"):
                iid = symbol
            elif symbol.endswith("USDT"):
                iid = f"{symbol[:-4]}-USDT-SWAP"
            elif symbol.endswith("USDC"):
                iid = f"{symbol[:-4]}-USDC-SWAP"
            else:
                iid = f"{symbol}-USDT-SWAP"
        lim_url = f"https://www.okx.com/api/v5/public/price-limit?instId={iid}"
        tk_url = f"https://www.okx.com/api/v5/market/ticker?instId={iid}"
        ob_url = f"https://www.okx.com/api/v5/market/books?instId={iid}&sz=20"
        lim_data, tk_data, ob_data = await asyncio.gather(
            _fetch_json(session, lim_url),
            _fetch_json(session, tk_url),
            _fetch_json(session, ob_url),
        )
        if not isinstance(lim_data, dict) or not isinstance(tk_data, dict):
            return None
        lim_arr = lim_data.get("data") or []
        tk_arr = tk_data.get("data") or []
        if not lim_arr or not tk_arr:
            return None
        lim = lim_arr[0]
        tk = tk_arr[0]
        live_upper = _safe_float(lim.get("buyLmt"))
        live_lower = _safe_float(lim.get("sellLmt"))
        live_bid = _safe_float(tk.get("bidPx"))
        live_ask = _safe_float(tk.get("askPx"))
        live_last = _safe_float(tk.get("last"))
        live_mark = live_last if live_last > 0 else ((live_bid + live_ask) / 2.0)
        live_bid_usd = live_bid * _safe_float(tk.get("bidSz")) * okx_ct_val
        live_ask_usd = live_ask * _safe_float(tk.get("askSz")) * okx_ct_val
        v24 = _safe_float(tk.get("volCcy24h")) * (live_last if live_last > 0 else live_bid)
        if v24 > 0:
            live_vol = v24
        if isinstance(ob_data, dict):
            ob_arr = ob_data.get("data") or []
            if ob_arr:
                bids = ob_arr[0].get("bids") or []
                asks = ob_arr[0].get("asks") or []
        ct_val_for_wall = okx_ct_val
    else:
        return None

    hit = _detect_corridor_hit(
        best_bid=live_bid,
        best_ask=live_ask,
        upper_limit=live_upper,
        lower_limit=live_lower,
        tick_size=tick_size,
        touch_pct=UPTICK_TOUCH_PCT,
        max_spread_pct=UPTICK_MAX_SPREAD_PCT,
    )
    if hit is None:
        return None

    side, limit_px, ref_px = hit
    wall_usd = _calc_corridor_wall_usd(
        bids=bids,
        asks=asks,
        limit_price=limit_px,
        ct_val=ct_val_for_wall,
        band_pct=UPTICK_WALL_BAND_PCT,
    )
    if wall_usd <= 0:
        wall_usd = round(max(live_bid_usd, live_ask_usd), 2)

    if wall_usd < UPTICK_MIN_WALL_USD:
        return None

    return side, ref_px, limit_px, live_mark, live_vol, wall_usd


# ==================== ДВИЖОК АПТИКОВ ====================

class UptickEngine:
    """
    Движок детекции Аптиков по ценовому коридору биржи с живой сверкой
    актуальной планки и реальной плотности в стакане.
    """

    def __init__(self, on_uptick: Callable[[UptickEvent], Any]) -> None:
        self.on_uptick = on_uptick
        # (exchange, symbol) -> (side, ts, vol24h, wall_usd)
        self._active: Dict[Tuple[str, str], Tuple[UptickSide, float, float, float]] = {}
        # (exchange, symbol, side) -> last_alert_ts
        self._cooldown: Dict[Tuple[str, str, UptickSide], float] = {}
        # (exchange, symbol) -> last_verify_ts (ограничивает частоту REST-сверок)
        self._last_verify: Dict[Tuple[str, str], float] = {}
        self._in_flight: Set[Tuple[str, str]] = set()

    async def evaluate_candidate(
        self,
        session: aiohttp.ClientSession,
        exchange: str,
        symbol: str,
        best_bid: float,
        best_ask: float,
        approx_upper: float,
        approx_lower: float,
        volume_24h_usd: float,
        tick_size: float = 0.0,
        rest_base: str = "",
        up_mult: float = 0.0,
        dn_mult: float = 0.0,
        okx_inst_id: str = "",
        okx_ct_val: float = 1.0,
    ) -> None:
        if is_blacklisted_symbol(symbol):
            return
        if best_bid <= 0 or best_ask <= 0:
            return

        # Предварительная быстрая проверка по WebSocket перед живой сверкой
        pre_hit = _detect_corridor_hit(
            best_bid=best_bid,
            best_ask=best_ask,
            upper_limit=approx_upper,
            lower_limit=approx_lower,
            tick_size=tick_size,
            touch_pct=UPTICK_PREFILTER_PCT,
            max_spread_pct=UPTICK_MAX_SPREAD_PCT * 2.0,
        )
        if pre_hit is None:
            return

        key = (exchange, symbol)
        if key in self._in_flight:
            return

        now = time.time()
        pre_side = pre_hit[0]
        on_cd = (now - self._cooldown.get((exchange, symbol, pre_side), 0.0)) < UPTICK_COOLDOWN_SEC
        min_gap = 5.0 if on_cd else 1.0
        if (now - self._last_verify.get(key, 0.0)) < min_gap:
            return

        self._in_flight.add(key)
        self._last_verify[key] = now
        try:
            verified = await verify_live_corridor(
                session=session,
                exchange=exchange,
                symbol=symbol,
                rest_base=rest_base,
                up_mult=up_mult,
                dn_mult=dn_mult,
                tick_size=tick_size,
                okx_inst_id=okx_inst_id,
                okx_ct_val=okx_ct_val,
                fallback_vol24h=volume_24h_usd,
            )
        finally:
            self._in_flight.discard(key)

        if verified is None:
            return

        side, live_price, live_limit, live_mark, live_vol, wall_usd = verified
        now = time.time()
        self._active[key] = (side, now, live_vol, wall_usd)
        cd_key = (exchange, symbol, side)

        if now - self._cooldown.get(cd_key, 0.0) >= UPTICK_COOLDOWN_SEC:
            self._cooldown[cd_key] = now
            ev = UptickEvent(
                exchange=exchange,
                symbol=symbol,
                side=side,
                price=live_price,
                limit_price=live_limit,
                mark_price=live_mark,
                volume_24h_usd=live_vol,
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


# ==================== WEBSOCKET-СКАНЕРЫ АПТИКОВ (4 БИРЖИ) ====================

async def _binance_style_uptick_ws(
    session: aiohttp.ClientSession,
    exchange_name: str,
    rest_base: str,
    ws_url: str,
    uptick_eng: UptickEngine,
) -> None:
    """
    WebSocket-сканер аптиков для Binance Futures и Aster Futures:
    - Из exchangeInfo загружаются реальные множители PERCENT_PRICE и tickSize.
    - !markPrice@arr@1s обновляет markPrice; !miniTicker@arr страхует все пары и
      автоматически переводит любую монету, подошедшую ближе 2.5% к планке,
      на потиковый @bookTicker.
    - При касании границы коридора на @bookTicker выполняется мгновенная живая
      сверка (/premiumIndex + /depth) — ноль ложных алертов после вылета.
    """
    multipliers: Dict[str, Tuple[float, float]] = {}
    tick_sizes: Dict[str, float] = {}
    tradfi_syms: Set[str] = set()
    mark_prices: Dict[str, float] = {}
    volumes_24h: Dict[str, float] = {}
    hot_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=300)
    last_info_ts = 0.0
    ws_single_url = ws_url.split("/stream?")[0] + "/ws"

    async def refresh_exchange_info() -> None:
        nonlocal last_info_ts
        try:
            async with session.get(f"{rest_base}/fapi/v1/exchangeInfo", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            tradfi_cnt = 0
            for s in data.get("symbols") or []:
                sym = str(s.get("symbol") or "")
                if not sym or s.get("status") != "TRADING":
                    continue
                ctype = str(s.get("contractType") or "").upper()
                utype = str(s.get("underlyingType") or "").upper()
                sub = " ".join(str(x) for x in (s.get("underlyingSubType") or []))
                is_tradfi = (
                    _has_tradfi_keyword(ctype)
                    or (utype and utype != "COIN")
                    or _has_tradfi_keyword(sub)
                    or is_tradfi_uptick_symbol(sym)
                )
                if is_tradfi:
                    _register_tradfi_symbol(sym)
                    tradfi_syms.add(sym)
                    tradfi_cnt += 1
                if not (sym.endswith("USDT") or (is_tradfi and sym.endswith("USDC"))):
                    continue

                up_m, dn_m = 0.0, 0.0
                t_sz = 0.0
                for f in s.get("filters") or []:
                    ft = f.get("filterType")
                    if ft == "PERCENT_PRICE":
                        u = _safe_float(f.get("multiplierUp"))
                        d = _safe_float(f.get("multiplierDown"))
                        if u > 1.0 and 0.0 < d < 1.0:
                            up_m, dn_m = u, d
                    elif ft == "PRICE_FILTER":
                        t_sz = _safe_float(f.get("tickSize"))
                if up_m > 1.0 and 0.0 < dn_m < 1.0:
                    multipliers[sym] = (up_m, dn_m)
                    tick_sizes[sym] = t_sz

            try:
                async with session.get(f"{rest_base}/fapi/v1/ticker/24hr", timeout=aiohttp.ClientTimeout(total=8)) as t_resp:
                    if t_resp.status == 200:
                        t_data = await t_resp.json()
                        if isinstance(t_data, list):
                            for it in t_data:
                                sym = str(it.get("symbol") or "")
                                if sym:
                                    volumes_24h[sym] = _safe_float(it.get("quoteVolume"))
            except Exception:
                pass

            last_info_ts = time.time()
            logger.info(
                "[%s Uptick WS] Загружены коридоры PERCENT_PRICE для %s пар (TradFi: %s)",
                exchange_name,
                len(multipliers),
                tradfi_cnt,
            )
        except Exception as exc:
            logger.warning("[%s Uptick WS] Ошибка exchangeInfo: %s", exchange_name, exc)

    async def fast_book_ticker_loop() -> None:
        req_id = 1
        while True:
            try:
                if not multipliers:
                    await asyncio.sleep(2)
                    continue

                allowed_crypto = [
                    (s, volumes_24h.get(s, 0.0))
                    for s in multipliers
                    if s not in tradfi_syms and not is_blacklisted_symbol(s)
                ]
                allowed_crypto.sort(key=lambda x: x[1], reverse=True)
                init_syms = list(
                    dict.fromkeys(
                        [s for s in tradfi_syms if s in multipliers]
                        + [s for s, _ in allowed_crypto[:120]]
                    )
                )
                if not init_syms:
                    await asyncio.sleep(5)
                    continue

                subscribed: Set[str] = set()
                started = time.time()
                async with session.ws_connect(ws_single_url, heartbeat=20, timeout=15) as bws:
                    for i in range(0, len(init_syms), 40):
                        chunk = init_syms[i : i + 40]
                        params = [f"{s.lower()}@bookTicker" for s in chunk]
                        await bws.send_json({"method": "SUBSCRIBE", "params": params, "id": req_id})
                        req_id += 1
                        subscribed.update(chunk)
                        await asyncio.sleep(0.1)

                    logger.info("[%s Fast BookTicker WS] Подписан на %s пар (реалтайм)", exchange_name, len(subscribed))

                    async def dynamic_sub_worker() -> None:
                        nonlocal req_id
                        while True:
                            sym = await hot_queue.get()
                            if sym in subscribed or sym not in multipliers or len(subscribed) >= 280:
                                continue
                            try:
                                await bws.send_json(
                                    {"method": "SUBSCRIBE", "params": [f"{sym.lower()}@bookTicker"], "id": req_id}
                                )
                                req_id += 1
                                subscribed.add(sym)
                            except Exception:
                                break

                    sub_task = asyncio.create_task(dynamic_sub_worker())
                    pre_tol = UPTICK_PREFILTER_PCT / 100.0
                    try:
                        async for msg in bws:
                            if time.time() - started > 1800:
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                                continue
                            d = json.loads(msg.data)
                            if not isinstance(d, dict) or "b" not in d or "a" not in d:
                                continue
                            sym = d.get("s") or ""
                            mult = multipliers.get(sym)
                            if not sym or mult is None:
                                continue
                            mp = mark_prices.get(sym, 0.0)
                            if mp <= 0:
                                continue
                            bid = _safe_float(d.get("b"))
                            ask = _safe_float(d.get("a"))
                            if bid <= 0 or ask <= 0:
                                continue
                            up_m, dn_m = mult
                            upper = mp * up_m
                            lower = mp * dn_m
                            if ask >= upper * (1.0 - pre_tol) or bid <= lower * (1.0 + pre_tol):
                                asyncio.create_task(
                                    uptick_eng.evaluate_candidate(
                                        session=session,
                                        exchange=exchange_name,
                                        symbol=sym,
                                        best_bid=bid,
                                        best_ask=ask,
                                        approx_upper=upper,
                                        approx_lower=lower,
                                        volume_24h_usd=volumes_24h.get(sym, 0.0),
                                        tick_size=tick_sizes.get(sym, 0.0),
                                        rest_base=rest_base,
                                        up_mult=up_m,
                                        dn_mult=dn_m,
                                    )
                                )
                    finally:
                        sub_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("[%s Fast BookTicker WS] Переподключение через 10с: %s", exchange_name, exc)
                await asyncio.sleep(10)

    book_task = asyncio.create_task(fast_book_ticker_loop())
    try:
        while True:
            try:
                if time.time() - last_info_ts > 900 or not multipliers:
                    await refresh_exchange_info()
                async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                    logger.info("[%s Uptick WS] Подключён", exchange_name)
                    pre_tol = UPTICK_PREFILTER_PCT / 100.0
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
                                sym = str(it.get("s") or "")
                                mult = multipliers.get(sym)
                                if mult is None:
                                    continue
                                vol = _safe_float(it.get("q"))
                                if vol > 0:
                                    volumes_24h[sym] = vol
                                if is_blacklisted_symbol(sym):
                                    continue
                                mp = mark_prices.get(sym, 0.0)
                                if mp <= 0:
                                    continue
                                lp = _safe_float(it.get("c"))
                                if lp <= 0:
                                    continue
                                up_m, dn_m = mult
                                upper = mp * up_m
                                lower = mp * dn_m

                                # Любую «спящую» монету ближе 2.5% к планке сразу переводим на потиковый @bookTicker
                                if lp >= upper * 0.975 or lp <= lower * 1.025:
                                    try:
                                        hot_queue.put_nowait(sym)
                                    except asyncio.QueueFull:
                                        pass

                                # Резервная страховка, если монета ещё не успела подписаться на @bookTicker
                                if lp >= upper * (1.0 - pre_tol) or lp <= lower * (1.0 + pre_tol):
                                    asyncio.create_task(
                                        uptick_eng.evaluate_candidate(
                                            session=session,
                                            exchange=exchange_name,
                                            symbol=sym,
                                            best_bid=lp,
                                            best_ask=lp,
                                            approx_upper=upper,
                                            approx_lower=lower,
                                            volume_24h_usd=vol,
                                            tick_size=tick_sizes.get(sym, 0.0),
                                            rest_base=rest_base,
                                            up_mult=up_m,
                                            dn_mult=dn_m,
                                        )
                                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[%s Uptick WS] Переподключение через 4с: %s", exchange_name, exc)
                await asyncio.sleep(4)
    finally:
        book_task.cancel()


async def bybit_uptick_ws(session: aiohttp.ClientSession, uptick_eng: UptickEngine) -> None:
    """
    WebSocket-сканер аптиков для Bybit Linear:
    - Кэш price-limit используется ТОЛЬКО как ориентир (TTL 5с + мгновенный сброс при сдвиге markPrice >= 0.15%).
    - Перед любым алертом всегда выполняется живая параллельная REST-сверка (price-limit + ticker + orderbook).
    """
    ws_url = "wss://stream.bybit.com/v5/public/linear"
    state: Dict[str, Dict[str, float]] = {}
    tick_sizes: Dict[str, float] = {}
    # sym -> (buyLmt, sellLmt, ts, mark_at_fetch)
    limits_cache: Dict[str, Tuple[float, float, float, float]] = {}
    in_flight_limits: Set[str] = set()

    async def refresh_and_check(sym: str) -> None:
        try:
            st = state.get(sym)
            if not st or st["mp"] <= 0:
                return
            lim_data = await _fetch_json(
                session,
                f"https://api.bybit.com/v5/market/price-limit?category=linear&symbol={sym}",
            )
            if not isinstance(lim_data, dict):
                return
            res = lim_data.get("result") or {}
            up = _safe_float(res.get("buyLmt"))
            dn = _safe_float(res.get("sellLmt"))
            if up <= 0 or dn <= 0:
                return
            limits_cache[sym] = (up, dn, time.time(), st["mp"])
            bid, ask = st["bid"], st["ask"]
            if bid > 0 and ask > 0:
                await uptick_eng.evaluate_candidate(
                    session=session,
                    exchange="Bybit",
                    symbol=sym,
                    best_bid=bid,
                    best_ask=ask,
                    approx_upper=up,
                    approx_lower=dn,
                    volume_24h_usd=st["vol"],
                    tick_size=tick_sizes.get(sym, 0.0),
                )
        finally:
            in_flight_limits.discard(sym)

    while True:
        try:
            tradfi_bybit: Set[str] = set()
            inst_data = await _fetch_json(
                session,
                "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000",
                timeout_sec=10.0,
            )
            if isinstance(inst_data, dict):
                for it in (inst_data.get("result") or {}).get("list") or []:
                    sym = str(it.get("symbol") or "")
                    if not sym or str(it.get("status") or "") != "Trading":
                        continue
                    t_sz = _safe_float((it.get("priceFilter") or {}).get("tickSize"))
                    if t_sz > 0:
                        tick_sizes[sym] = t_sz
                    if (
                        _has_tradfi_keyword(it.get("symbolType"))
                        or _has_tradfi_keyword(it.get("contractType"))
                        or _has_tradfi_keyword(it.get("displayName"))
                        or is_tradfi_uptick_symbol(sym)
                    ):
                        _register_tradfi_symbol(sym)
                        tradfi_bybit.add(sym)

            tk_data = await _fetch_json(
                session,
                "https://api.bybit.com/v5/market/tickers?category=linear",
                timeout_sec=10.0,
            )
            raw_list = ((tk_data or {}).get("result") or {}).get("list") or []

            tradfi_list: List[str] = []
            crypto_list: List[Tuple[str, float]] = []
            for it in raw_list:
                sym = str(it.get("symbol") or "")
                if not sym:
                    continue
                is_tf = sym in tradfi_bybit or is_tradfi_uptick_symbol(sym)
                if is_tf and (sym.endswith("USDT") or sym.endswith("USDC")):
                    tradfi_list.append(sym)
                    continue
                if sym.endswith("USDT") and not is_blacklisted_symbol(sym):
                    vol = _safe_float(it.get("turnover24h"))
                    if vol >= 100_000:
                        crypto_list.append((sym, vol))

            crypto_list.sort(key=lambda x: x[1], reverse=True)
            symbols = list(dict.fromkeys(tradfi_list + [x[0] for x in crypto_list[:150]]))
            if not symbols:
                await asyncio.sleep(15)
                continue

            started = time.time()
            pre_tol = UPTICK_PREFILTER_PCT / 100.0
            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                for i in range(0, len(symbols), 10):
                    args = [f"tickers.{s}" for s in symbols[i : i + 10]]
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info(
                    "[Bybit Uptick WS] Подписан на %s пар (TradFi: %s)",
                    len(symbols),
                    len(tradfi_list),
                )

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send_json({"op": "ping"})
                        except Exception:
                            break

                ping_task = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if time.time() - started > 1800:
                            break
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
                        st = state.setdefault(
                            sym,
                            {"mp": 0.0, "vol": 0.0, "bid": 0.0, "ask": 0.0},
                        )
                        if "markPrice" in d:
                            st["mp"] = _safe_float(d["markPrice"], st["mp"])
                        if "turnover24h" in d:
                            st["vol"] = _safe_float(d["turnover24h"], st["vol"])
                        if "bid1Price" in d:
                            st["bid"] = _safe_float(d["bid1Price"], st["bid"])
                        if "ask1Price" in d:
                            st["ask"] = _safe_float(d["ask1Price"], st["ask"])

                        mp, bid, ask = st["mp"], st["bid"], st["ask"]
                        if mp <= 0 or bid <= 0 or ask <= 0:
                            continue

                        dev_pct = max(abs(ask - mp), abs(bid - mp)) / mp * 100.0
                        min_dev = 0.25 if sym in tradfi_bybit else 0.60
                        if dev_pct < min_dev:
                            continue

                        now = time.time()
                        cached = limits_cache.get(sym)
                        # Считаем кэш актуальным не дольше 5с и только если markPrice не ушёл дальше 0.15%
                        cache_valid = (
                            cached is not None
                            and (now - cached[2]) < 5.0
                            and cached[3] > 0
                            and (abs(mp - cached[3]) / cached[3] * 100.0) < 0.15
                        )
                        if cache_valid:
                            up, dn = cached[0], cached[1]
                            if ask >= up * (1.0 - pre_tol) or bid <= dn * (1.0 + pre_tol):
                                asyncio.create_task(
                                    uptick_eng.evaluate_candidate(
                                        session=session,
                                        exchange="Bybit",
                                        symbol=sym,
                                        best_bid=bid,
                                        best_ask=ask,
                                        approx_upper=up,
                                        approx_lower=dn,
                                        volume_24h_usd=st["vol"],
                                        tick_size=tick_sizes.get(sym, 0.0),
                                    )
                                )
                        elif sym not in in_flight_limits:
                            in_flight_limits.add(sym)
                            asyncio.create_task(refresh_and_check(sym))
                finally:
                    ping_task.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Bybit Uptick WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


async def okx_uptick_ws(session: aiohttp.ClientSession, uptick_eng: UptickEngine) -> None:
    """
    WebSocket-сканер аптиков для OKX SWAP:
    - Подписывается на официальный WS-канал price-limit (buyLmt/sellLmt) + tickers + mark-price.
    - Перед любым алертом всегда делает живую параллельную REST-сверку (price-limit + ticker + books),
      так как OKX WS price-limit пушит обновления с шагом до 5с.
    """
    ws_url = "wss://ws.okx.com:8443/ws/v5/public"
    ct_vals: Dict[str, float] = {}
    tick_sizes: Dict[str, float] = {}
    mark_prices: Dict[str, float] = {}
    ws_limits: Dict[str, Tuple[float, float]] = {}

    while True:
        try:
            tradfi_okx: Set[str] = set()
            inst_data = await _fetch_json(
                session,
                "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
                timeout_sec=10.0,
            )
            for it in (inst_data or {}).get("data") or []:
                iid = str(it.get("instId") or "")
                if not (iid.endswith("-USDT-SWAP") or iid.endswith("-USDC-SWAP")):
                    continue
                ct_vals[iid] = _safe_float(it.get("ctVal"), 1.0)
                tick_sizes[iid] = _safe_float(it.get("tickSz"), 0.0)
                cat = str(it.get("instCategory") or "")
                if (cat and cat != "1") or _has_tradfi_keyword(it.get("instFamily")) or is_tradfi_uptick_symbol(iid):
                    _register_tradfi_symbol(iid)
                    tradfi_okx.add(iid)

            tk_data = await _fetch_json(
                session,
                "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
                timeout_sec=10.0,
            )

            tradfi_ids: List[str] = []
            crypto_items: List[Tuple[str, float]] = []
            for it in (tk_data or {}).get("data") or []:
                iid = str(it.get("instId") or "")
                if not (iid.endswith("-USDT-SWAP") or iid.endswith("-USDC-SWAP")):
                    continue
                if iid in tradfi_okx or is_tradfi_uptick_symbol(iid):
                    tradfi_ids.append(iid)
                    continue
                if not iid.endswith("-USDT-SWAP") or is_blacklisted_symbol(iid):
                    continue
                vol_usd = _safe_float(it.get("volCcy24h")) * _safe_float(it.get("last"))
                if vol_usd >= 100_000:
                    crypto_items.append((iid, vol_usd))

            crypto_items.sort(key=lambda x: x[1], reverse=True)
            inst_ids = list(dict.fromkeys(tradfi_ids + [x[0] for x in crypto_items[:100]]))
            if not inst_ids:
                await asyncio.sleep(15)
                continue

            started = time.time()
            pre_tol = UPTICK_PREFILTER_PCT / 100.0
            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                for i in range(0, len(inst_ids), 15):
                    chunk = inst_ids[i : i + 15]
                    args = []
                    for iid in chunk:
                        args.append({"channel": "tickers", "instId": iid})
                        args.append({"channel": "mark-price", "instId": iid})
                        args.append({"channel": "price-limit", "instId": iid})
                    await ws.send_json({"op": "subscribe", "args": args})
                    await asyncio.sleep(0.15)
                logger.info(
                    "[OKX Uptick WS] Подписан на %s пар + price-limit (TradFi: %s)",
                    len(inst_ids),
                    len(tradfi_ids),
                )

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send_str("ping")
                        except Exception:
                            break

                ping_task = asyncio.create_task(pinger())
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
                        ch = arg.get("channel")
                        arr = payload.get("data") or []
                        if not arr:
                            continue
                        d = arr[0]
                        iid = d.get("instId") or arg.get("instId") or ""
                        if not iid:
                            continue

                        if ch == "price-limit":
                            up = _safe_float(d.get("buyLmt"))
                            dn = _safe_float(d.get("sellLmt"))
                            if up > 0 and dn > 0:
                                ws_limits[iid] = (up, dn)
                        elif ch == "mark-price":
                            mp = _safe_float(d.get("markPx"))
                            if mp > 0:
                                mark_prices[iid] = mp
                        elif ch == "tickers":
                            lim = ws_limits.get(iid)
                            if lim is None:
                                continue
                            bid = _safe_float(d.get("bidPx"))
                            ask = _safe_float(d.get("askPx"))
                            if bid <= 0 or ask <= 0:
                                continue
                            up, dn = lim
                            if ask >= up * (1.0 - pre_tol) or bid <= dn * (1.0 + pre_tol):
                                lp = _safe_float(d.get("last"), bid)
                                vol_usd = _safe_float(d.get("volCcy24h")) * lp
                                sym_clean = iid.replace("-USDT-SWAP", "USDT").replace("-USDC-SWAP", "USDC")
                                asyncio.create_task(
                                    uptick_eng.evaluate_candidate(
                                        session=session,
                                        exchange="OKX",
                                        symbol=sym_clean,
                                        best_bid=bid,
                                        best_ask=ask,
                                        approx_upper=up,
                                        approx_lower=dn,
                                        volume_24h_usd=vol_usd,
                                        tick_size=tick_sizes.get(iid, 0.0),
                                        okx_inst_id=iid,
                                        okx_ct_val=ct_vals.get(iid, 1.0),
                                    )
                                )
                finally:
                    ping_task.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[OKX Uptick WS] Переподключение через 5с: %s", exc)
            await asyncio.sleep(5)


def create_uptick_tasks(session: aiohttp.ClientSession, uptick_eng: UptickEngine) -> List[asyncio.Task[Any]]:
    """Запускает 4 независимых WebSocket-сканера Аптиков с живой сверкой ценового коридора."""
    return [
        asyncio.create_task(
            _binance_style_uptick_ws(
                session,
                "Binance",
                "https://fapi.binance.com",
                "wss://fstream.binance.com/stream?streams=!markPrice@arr@1s/!miniTicker@arr",
                uptick_eng,
            )
        ),
        asyncio.create_task(
            _binance_style_uptick_ws(
                session,
                "Aster",
                "https://fapi.asterdex.com",
                "wss://fstream.asterdex.com/stream?streams=!markPrice@arr@1s/!miniTicker@arr",
                uptick_eng,
            )
        ),
        asyncio.create_task(bybit_uptick_ws(session, uptick_eng)),
        asyncio.create_task(okx_uptick_ws(session, uptick_eng)),
    ]