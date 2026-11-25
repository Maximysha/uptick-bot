"""
Мультибиржевой детектор «Аптиков» + Сканер «Плотностей» (Спот и Фьючерсы) + Telegram Bot + Mini App + Tiger.Trade API.
Аптики (4 биржи): Binance, Bybit, OKX, Aster.
Плотности (7 бирж): Binance, Bybit, OKX, KuCoin, Aster, Gate, Bitget.

Зависимости:
    pip install aiohttp aiosqlite aiogram fastapi uvicorn asyncpg
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
    import asyncpg  # Для бесплатного PostgreSQL (Neon / Supabase)
except ImportError:
    asyncpg = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("UptickDensityBot")


# ==============================================================================
# ГЛОБАЛЬНЫЙ ЧЕРНЫЙ СПИСОК (ТЯЖЕЛЫЕ МОНЕТЫ, СТЕЙБЛКОИНЫ, МЕТАЛЛЫ, НЕФТЬ, АКЦИИ)
# ==============================================================================

STOCK_AND_ETF_TICKERS: Set[str] = {
    # Крупные акции США, полупроводники, криптоклипы, популярные RWA/xStocks
    "TSLA", "NVDA", "AAPL", "MSFT", "AMZN", "GOOG", "GOOGL", "META", "NFLX",
    "AMD", "INTC", "MSTR", "COIN", "HOOD", "PLTR", "GME", "AMC", "BABA", "PYPL",
    "UBER", "DIS", "BA", "JPM", "GS", "V", "MA", "KO", "PEP", "MCD", "WMT",
    "COST", "ARM", "SMCI", "CRCL", "RKLB", "SOFI", "ORCL", "CRM", "ADBE", "AVGO",
    "TSM", "SKHY", "SPCX", "SNDK", "MU", "QCOM", "TXN", "ASML", "AMAT", "LRCX",
    "KLAC", "MRVL", "ANET", "PANW", "CRWD", "SNOW", "DDOG", "NET", "SHOP", "SQ",
    "SPOT", "ABNB", "DASH", "LYFT", "RIVN", "LCID", "NIO", "XPEV", "LI", "BYD",
    "GM", "NKE", "SBUX", "TGT", "HD", "LOW", "PFE", "JNJ", "LLY", "NVO", "UNH",
    "ABBV", "MRK", "XOM", "CVX", "OXY", "SLB", "COP", "BP", "SHEL", "BAC", "WFC",
    "MS", "BLK", "SCHW", "BRK", "BRKB", "JD", "PDD", "BIDU", "TCEHY", "MELI",
    "MARA", "RIOT", "CLSK", "IREN", "HUT", "BITF", "CORZ", "WULF", "CIFR",
    "APP", "RDDT", "DUOL", "AXON", "VST", "CEG", "GEV", "OKLO", "SMR", "IONQ",
    "RGTI", "QBTS", "QUBT", "SOUN", "BBAI", "ASTS", "LUNR", "ACHR", "JOBY",
    "HIMS", "TEM", "ALAB", "NBIS", "CRWV", "DJT", "IBM", "CSCO", "DELL", "HPE",
    "WDC", "STX", "NOW", "INTU", "WDAY", "TEAM", "ZS", "FTNT", "OKTA", "MDB",
    "RBLX", "TTWO", "SONY", "ZM", "DOCU", "TWLO", "ROKU", "SNAP", "PINS",
    "BKNG", "EXPE", "DAL", "UAL", "AAL", "CCL", "RCL", "CAT", "DE", "GE", "HON",
    "LMT", "RTX", "NOC", "UPS", "FDX", "MMM", "PG", "PM", "LULU", "CMG", "DKNG",
    "BX", "KKR", "APO", "AXP", "COF", "AFRM", "UPST", "ISRG", "AMGN", "GILD",
    "MRNA", "VRTX", "REGN", "TMO", "DHR", "ABT", "MDT", "SYK", "BSX", "VZ",
    "TMUS", "CMCSA", "WBD", "TTD", "IBKR", "CME", "ICE", "NDAQ", "SPGI", "MCO",
    "SBET", "BMNR", "GLXY", "BULL", "FIG", "KLAR",
    # ETF и индексы
    "SPY", "QQQ", "SPX", "NDX", "DJI", "DIA", "IWM", "VIX", "VOO", "VTI",
    "SOXL", "SOXS", "TQQQ", "SQQQ", "UVXY", "ARKK", "TLT", "HYG", "LQD",
    "EEM", "FXI", "KWEB", "IBIT", "FBTC", "GBTC", "GLD", "SLV", "GDX", "GDXJ",
    "USO", "UNG", "COPX", "XLE", "XLF", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU",
    "SMH", "SOXX", "MAGS", "SPXU", "UPRO", "SPXL", "LABU", "LABD", "NUGT", "DUST",
    "CONL", "MSTX", "MSTU", "NVDL", "NVDS", "TSLL", "TSLQ", "NVDX",
    "NAS100", "US100", "US500", "US30", "SP500", "GER40", "DAX", "UK100", "JP225", "HK50",
}

COMMODITY_AND_METAL_TICKERS: Set[str] = {
    # Драгоценные и промышленные металлы
    "XAU", "XAG", "XPT", "XPD", "PAXG", "XAUT", "GOLD", "SILVER", "PLATINUM",
    "PALLADIUM", "COPPER", "ALUMINUM", "NICKEL", "ZINC", "IRON", "STEEL",
    "KAU", "KAG", "PMGT", "DGX", "XAUM", "XAGX", "XAUX", "HG", "GC", "SI", "PL", "PA",
    # Нефть, газ и сырье
    "CL", "BZ", "WTI", "BRENT", "OIL", "USOIL", "UKOIL", "CRUDE", "CRUDEOIL",
    "WTIOIL", "BRENTOIL", "NG", "NATGAS", "NATURALGAS", "GAS", "RBOB", "HO",
    "CORN", "WHEAT", "SOY", "SOYBEAN", "COFFEE", "SUGAR", "COTTON", "COCOA",
    "UKO", "USO", "BNO", "DBO",
}

HEAVY_COINS_BLACKLIST: Set[str] = {
    # Тяжелые криптовалюты
    "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "TRX", "TON", "AVAX",
    "LINK", "DOT", "MATIC", "POL", "LTC", "BCH", "UNI", "ATOM", "ETC", "XLM",
    "XMR", "ZEC", "NEAR", "APT", "ARB", "OP", "SUI", "FIL", "ICP", "HBAR",
    "VET", "ALGO", "AAVE", "MKR", "INJ", "TIA", "SEI", "SHIB", "PEPE",
    # Стейблкоины и фиат / форекс
    "USDT", "USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDP", "BUSD", "PYUSD",
    "EUR", "EURC", "AEUR", "GBP", "JPY", "TRY", "BRL", "USD1", "RLUSD",
    "AUD", "CAD", "CHF", "NZD", "CNH", "CNY", "HKD", "SGD", "MXN", "ZAR",
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD",
} | STOCK_AND_ETF_TICKERS | COMMODITY_AND_METAL_TICKERS


def extract_base_asset(symbol: str) -> str:
    """Извлекает базовый тикер из формата биржи (BTCUSDT, BTC-USDT-SWAP, 1000PEPEUSDT и т.д.)."""
    s = symbol.upper().strip()
    for suffix in ("-USDT-SWAP", "_USDT", "-USDT", "USDTM", "USDT", "PERP"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    s = re.sub(r"^(1000000|100000|10000|1000|100|1M|K)", "", s)
    return s.strip("-_")


def is_blacklisted_symbol(symbol: str) -> bool:
    """Возвращает True, если тикер относится к тяжелым монетам, металлам, нефти или акциям."""
    base = extract_base_asset(symbol)
    if not base:
        return True
    if base in HEAVY_COINS_BLACKLIST:
        return True
    # Токенизированные акции с суффиксом X (TSLAX, NVDAX, SNDKX) или ON (TSLAON, NVDAON)
    if base.endswith("X") and base[:-1] in STOCK_AND_ETF_TICKERS:
        return True
    if base.endswith("ON") and len(base) > 3 and base[:-2] in STOCK_AND_ETF_TICKERS:
        return True
    if base.startswith("X") and base[1:] in STOCK_AND_ETF_TICKERS:
        return True
    # Металлы, нефть и сырье по ключевым подстрокам
    if any(
        k in base
        for k in (
            "XAU", "XAG", "XPT", "XPD", "GOLD", "SILVER", "PAXG", "XAUT",
            "BRENT", "WTI", "USOIL", "UKOIL", "CRUDE", "NATGAS", "STOCK",
            "COPPER", "PLATIN", "PALLAD",
        )
    ):
        return True
    return False


# ==============================================================================
# МОДЕЛИ И ДВИЖОК АПТИКОВ
# ==============================================================================

class UptickSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class UptickEvent:
    exchange: str
    symbol: str
    side: UptickSide
    price: float
    mark_price: float
    limit_price: float
    volume_24h_usd: float
    distance_pct: float
    timestamp_utc: str
    utc_date: str


class UptickEngine:
    """Проверяет касание ценовой планки с защитой от пустого стакана и гистерезисом."""

    def __init__(
        self,
        min_volume_usd: float = 100_000.0,
        trigger_tolerance: float = 0.0008,
        release_tolerance: float = 0.0060,
        book_confirm_tol: float = 0.0080,
        min_cooldown_sec: float = 180.0,
        on_uptick: Optional[Callable[[UptickEvent], Awaitable[None]]] = None,
    ) -> None:
        self.min_volume_usd = min_volume_usd
        self.trigger_tolerance = trigger_tolerance
        self.release_tolerance = release_tolerance
        self.book_confirm_tol = book_confirm_tol
        self.min_cooldown_sec = min_cooldown_sec
        self.on_uptick = on_uptick

        self._active: Dict[Tuple[str, str], UptickSide] = {}
        self._last_alert_ts: Dict[Tuple[str, str, UptickSide], float] = {}

    def get_active_upticks(self) -> List[Tuple[str, str, UptickSide]]:
        return [(ex, sym, side) for (ex, sym), side in self._active.items()]

    async def evaluate(
        self,
        exchange: str,
        symbol: str,
        last_price: float,
        mark_price: float,
        upper_limit: float,
        lower_limit: float,
        volume_24h_usd: float,
        best_bid: Optional[float] = None,
        best_ask: Optional[float] = None,
    ) -> None:
        if is_blacklisted_symbol(symbol):
            return
        if volume_24h_usd < self.min_volume_usd:
            return
        if last_price <= 0 or mark_price <= 0 or upper_limit <= 0 or lower_limit <= 0:
            return
        if upper_limit <= lower_limit:
            return

        key = (exchange, symbol)
        now = time.time()

        dist_up = (upper_limit - last_price) / upper_limit
        dist_dn = (last_price - lower_limit) / lower_limit

        bid_ok_up = True if not best_bid or best_bid <= 0 else ((upper_limit - best_bid) / upper_limit <= self.book_confirm_tol)
        ask_ok_dn = True if not best_ask or best_ask <= 0 else ((best_ask - lower_limit) / lower_limit <= self.book_confirm_tol)

        detected: Optional[UptickSide] = None
        limit_price = 0.0
        dist_pct = 0.0

        if dist_up <= self.trigger_tolerance and bid_ok_up:
            detected = UptickSide.LONG
            limit_price = upper_limit
            dist_pct = round(max(0.0, dist_up) * 100.0, 3)
        elif dist_dn <= self.trigger_tolerance and ask_ok_dn:
            detected = UptickSide.SHORT
            limit_price = lower_limit
            dist_pct = round(max(0.0, dist_dn) * 100.0, 3)

        prev_side = self._active.get(key)

        if detected is not None:
            self._active[key] = detected
            cd_key = (exchange, symbol, detected)
            last_ts = self._last_alert_ts.get(cd_key, 0.0)
            if prev_side != detected and (now - last_ts) >= self.min_cooldown_sec:
                self._last_alert_ts[cd_key] = now
                dt = datetime.now(timezone.utc)
                event = UptickEvent(
                    exchange=exchange,
                    symbol=symbol,
                    side=detected,
                    price=last_price,
                    mark_price=mark_price,
                    limit_price=limit_price,
                    volume_24h_usd=volume_24h_usd,
                    distance_pct=dist_pct,
                    timestamp_utc=dt.strftime("%H:%M:%S"),
                    utc_date=dt.strftime("%Y-%m-%d"),
                )
                if self.on_uptick:
                    await self.on_uptick(event)
        elif prev_side is not None:
            if prev_side == UptickSide.LONG and dist_up > self.release_tolerance:
                self._active.pop(key, None)
            elif prev_side == UptickSide.SHORT and dist_dn > self.release_tolerance:
                self._active.pop(key, None)


# ==============================================================================
# МОДЕЛИ И ДВИЖОК ПЛОТНОСТЕЙ (LONG / SHORT + ИНДИВИДУАЛЬНЫЕ ПОРОГИ ПО МОНЕТАМ)
# ==============================================================================

class MarketType(Enum):
    SPOT = "SPOT"
    FUTURES = "FUTURES"


class DensitySide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class DensityEvent:
    exchange: str
    market_type: MarketType
    symbol: str
    side: DensitySide
    price: float
    current_price: float
    size_usd: float
    distance_pct: float
    volume_24h_usd: float
    timestamp_utc: str
    utc_date: str


class DensityEngine:
    """Фильтрует и отслеживает крупные лимитные плотности в пределах N% от цены."""

    def __init__(
        self,
        base_min_spot_usd: float = 250_000.0,
        base_min_futures_usd: float = 1_000_000.0,
        max_distance_pct: float = 5.0,
        cooldown_sec: float = 600.0,
        active_ttl_sec: float = 90.0,
        on_density: Optional[Callable[[DensityEvent], Awaitable[None]]] = None,
    ) -> None:
        self.base_min_spot_usd = base_min_spot_usd
        self.base_min_futures_usd = base_min_futures_usd
        self.max_distance_pct = max_distance_pct
        self.cooldown_sec = cooldown_sec
        self.active_ttl_sec = active_ttl_sec
        self.on_density = on_density

        self.custom_min_spot: Dict[str, float] = {}
        self.custom_min_futures: Dict[str, float] = {}
        self.watched_bases: Set[str] = set()

        self._active: Dict[Tuple[str, str, str, str], Tuple[DensityEvent, float]] = {}
        self._last_alert_ts: Dict[Tuple[str, str, str, str], float] = {}

    def update_custom_floors(
        self,
        spot_floors: Dict[str, float],
        futures_floors: Dict[str, float],
    ) -> None:
        self.custom_min_spot = spot_floors
        self.custom_min_futures = futures_floors
        self.watched_bases = set(spot_floors.keys()) | set(futures_floors.keys())

    def get_effective_min_usd(self, symbol: str, market_type: MarketType) -> float:
        base = extract_base_asset(symbol)
        if market_type == MarketType.SPOT:
            custom = self.custom_min_spot.get(base)
            return min(self.base_min_spot_usd, custom) if custom is not None else self.base_min_spot_usd
        custom = self.custom_min_futures.get(base)
        return min(self.base_min_futures_usd, custom) if custom is not None else self.base_min_futures_usd

    def get_active_densities(self) -> List[DensityEvent]:
        now = time.time()
        expired = [k for k, (_, ts) in self._active.items() if now - ts > self.active_ttl_sec]
        for k in expired:
            self._active.pop(k, None)
        items = [ev for ev, _ in self._active.values()]
        items.sort(key=lambda x: x.size_usd, reverse=True)
        return items

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
        if is_blacklisted_symbol(symbol) or current_price <= 0:
            return

        min_usd = self.get_effective_min_usd(symbol, market_type)
        now = time.time()

        # BID -> LONG, ASK -> SHORT
        for side, levels in ((DensitySide.LONG, bids), (DensitySide.SHORT, asks)):
            best_hit: Optional[Tuple[float, float, float]] = None
            for px, qty in levels:
                if px <= 0 or qty <= 0:
                    continue
                dist_pct = abs(px - current_price) / current_price * 100.0
                if dist_pct > self.max_distance_pct:
                    continue
                size_usd = px * qty * contract_multiplier
                if size_usd >= min_usd:
                    if best_hit is None or size_usd > best_hit[0]:
                        best_hit = (size_usd, px, dist_pct)

            if best_hit is None:
                continue

            size_usd, level_px, dist_pct = best_hit
            dt = datetime.now(timezone.utc)
            event = DensityEvent(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                side=side,
                price=level_px,
                current_price=current_price,
                size_usd=size_usd,
                distance_pct=round(dist_pct, 2),
                volume_24h_usd=volume_24h_usd,
                timestamp_utc=dt.strftime("%H:%M:%S"),
                utc_date=dt.strftime("%Y-%m-%d"),
            )

            key = (exchange, market_type.value, symbol, side.value)
            self._active[key] = (event, now)

            last_ts = self._last_alert_ts.get(key, 0.0)
            if now - last_ts >= self.cooldown_sec:
                self._last_alert_ts[key] = now
                if self.on_density:
                    await self.on_density(event)


# ==============================================================================
# СКАНЕРЫ АПТИКОВ (4 БИРЖИ: BINANCE, BYBIT, OKX, ASTER)
# ==============================================================================

class BaseExchangeScanner:
    name: str = "Base"
    poll_interval: float = 3.5

    def __init__(self, engine_ref: UptickEngine, session: aiohttp.ClientSession) -> None:
        self.engine = engine_ref
        self.session = session

    async def _get_json(self, url: str, timeout: float = 8.0) -> Optional[object]:
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception:
            pass
        return None

    async def poll_once(self) -> None:
        raise NotImplementedError

    async def run_forever(self) -> None:
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"[{self.name} Uptick] Ошибка цикла: {exc}")
            await asyncio.sleep(self.poll_interval)


class BinanceLikeFuturesScanner(BaseExchangeScanner):
    base_url: str = ""
    api_prefix: str = "/fapi/v1"

    def __init__(self, engine_ref: UptickEngine, session: aiohttp.ClientSession) -> None:
        super().__init__(engine_ref, session)
        self._multipliers: Dict[str, Tuple[float, float]] = {}
        self._last_info_ts: float = 0.0

    async def _refresh_exchange_info(self) -> None:
        now = time.time()
        if self._multipliers and now - self._last_info_ts < 1800:
            return
        data = await self._get_json(f"{self.base_url}{self.api_prefix}/exchangeInfo", timeout=12.0)
        if not isinstance(data, dict):
            return
        parsed: Dict[str, Tuple[float, float]] = {}
        for sym_obj in data.get("symbols", []):
            if sym_obj.get("status") != "TRADING" or sym_obj.get("quoteAsset") != "USDT":
                continue
            sym = sym_obj.get("symbol", "")
            for flt in sym_obj.get("filters", []):
                if flt.get("filterType") == "PERCENT_PRICE":
                    try:
                        m_up = float(flt.get("multiplierUp", 0))
                        m_dn = float(flt.get("multiplierDown", 0))
                        if m_up > 1.0 and 0 < m_dn < 1.0:
                            parsed[sym] = (m_up, m_dn)
                    except (TypeError, ValueError):
                        pass
        if parsed:
            self._multipliers = parsed
            self._last_info_ts = now

    async def poll_once(self) -> None:
        await self._refresh_exchange_info()
        if not self._multipliers:
            return

        prem_raw, tick_raw, book_raw = await asyncio.gather(
            self._get_json(f"{self.base_url}{self.api_prefix}/premiumIndex"),
            self._get_json(f"{self.base_url}{self.api_prefix}/ticker/24hr"),
            self._get_json(f"{self.base_url}{self.api_prefix}/ticker/bookTicker"),
        )
        if not isinstance(prem_raw, list) or not isinstance(tick_raw, list):
            return

        marks = {
            item["symbol"]: float(item["markPrice"])
            for item in prem_raw
            if item.get("symbol") and item.get("markPrice")
        }
        books = {}
        if isinstance(book_raw, list):
            for b in book_raw:
                s = b.get("symbol")
                if s:
                    try:
                        books[s] = (float(b.get("bidPrice") or 0), float(b.get("askPrice") or 0))
                    except (TypeError, ValueError):
                        pass

        for t in tick_raw:
            sym = t.get("symbol", "")
            mult = self._multipliers.get(sym)
            mark = marks.get(sym, 0.0)
            if not mult or mark <= 0 or is_blacklisted_symbol(sym):
                continue
            try:
                last_px = float(t.get("lastPrice") or 0)
                vol_usd = float(t.get("quoteVolume") or 0)
            except (TypeError, ValueError):
                continue
            if vol_usd < self.engine.min_volume_usd or last_px <= 0:
                continue

            upper = mark * mult[0]
            lower = mark * mult[1]
            bid_px, ask_px = books.get(sym, (None, None))
            await self.engine.evaluate(
                exchange=self.name,
                symbol=sym,
                last_price=last_px,
                mark_price=mark,
                upper_limit=upper,
                lower_limit=lower,
                volume_24h_usd=vol_usd,
                best_bid=bid_px,
                best_ask=ask_px,
            )


class BinanceFuturesScanner(BinanceLikeFuturesScanner):
    name = "Binance"
    base_url = "https://fapi.binance.com"
    api_prefix = "/fapi/v1"
    poll_interval = 3.0


class AsterFuturesScanner(BinanceLikeFuturesScanner):
    name = "Aster"
    base_url = "https://fapi.asterdex.com"
    api_prefix = "/fapi/v3"
    poll_interval = 3.5


class BybitFuturesScanner(BaseExchangeScanner):
    name = "Bybit"
    poll_interval = 3.5

    async def poll_once(self) -> None:
        data = await self._get_json("https://api.bybit.com/v5/market/tickers?category=linear")
        if not isinstance(data, dict):
            return
        items = data.get("result", {}).get("list", [])
        candidates = []
        for t in items:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or is_blacklisted_symbol(sym):
                continue
            try:
                last_px = float(t.get("lastPrice") or 0)
                mark_px = float(t.get("markPrice") or 0)
                vol_usd = float(t.get("turnover24h") or 0)
                bid_px = float(t.get("bid1Price") or 0)
                ask_px = float(t.get("ask1Price") or 0)
            except (TypeError, ValueError):
                continue
            if vol_usd < self.engine.min_volume_usd or last_px <= 0 or mark_px <= 0:
                continue
            dev = abs(last_px - mark_px) / mark_px
            if dev >= 0.015:
                candidates.append((sym, last_px, mark_px, vol_usd, bid_px, ask_px))

        for sym, last_px, mark_px, vol_usd, bid_px, ask_px in candidates[:12]:
            lim_data = await self._get_json(
                f"https://api.bybit.com/v5/market/price-limit?category=linear&symbol={sym}"
            )
            if not isinstance(lim_data, dict):
                continue
            res = lim_data.get("result", {})
            try:
                buy_lmt = float(res.get("buyLmt") or 0)
                sell_lmt = float(res.get("sellLmt") or 0)
            except (TypeError, ValueError):
                continue
            if buy_lmt > 0 and sell_lmt > 0:
                await self.engine.evaluate(
                    exchange=self.name,
                    symbol=sym,
                    last_price=last_px,
                    mark_price=mark_px,
                    upper_limit=buy_lmt,
                    lower_limit=sell_lmt,
                    volume_24h_usd=vol_usd,
                    best_bid=bid_px,
                    best_ask=ask_px,
                )
            await asyncio.sleep(0.08)


class OKXSwapScanner(BaseExchangeScanner):
    name = "OKX"
    poll_interval = 3.5

    async def poll_once(self) -> None:
        tick_raw, mark_raw = await asyncio.gather(
            self._get_json("https://www.okx.com/api/v5/market/tickers?instType=SWAP"),
            self._get_json("https://www.okx.com/api/v5/public/mark-price?instType=SWAP"),
        )
        if not isinstance(tick_raw, dict) or not isinstance(mark_raw, dict):
            return

        marks = {
            m["instId"]: float(m["markPx"])
            for m in mark_raw.get("data", [])
            if m.get("instId") and m.get("markPx")
        }

        candidates = []
        for t in tick_raw.get("data", []):
            inst_id = t.get("instId", "")
            if not inst_id.endswith("-USDT-SWAP"):
                continue
            clean_sym = inst_id.replace("-USDT-SWAP", "USDT")
            if is_blacklisted_symbol(clean_sym):
                continue
            mark_px = marks.get(inst_id, 0.0)
            try:
                last_px = float(t.get("last") or 0)
                vol_ccy = float(t.get("volCcy24h") or 0)
                vol_usd = vol_ccy * last_px
                bid_px = float(t.get("bidPx") or 0)
                ask_px = float(t.get("askPx") or 0)
            except (TypeError, ValueError):
                continue
            if vol_usd < self.engine.min_volume_usd or last_px <= 0 or mark_px <= 0:
                continue
            dev = abs(last_px - mark_px) / mark_px
            if dev >= 0.015:
                candidates.append((inst_id, clean_sym, last_px, mark_px, vol_usd, bid_px, ask_px))

        for inst_id, clean_sym, last_px, mark_px, vol_usd, bid_px, ask_px in candidates[:12]:
            lim_raw = await self._get_json(
                f"https://www.okx.com/api/v5/public/price-limit?instId={inst_id}"
            )
            if not isinstance(lim_raw, dict):
                continue
            arr = lim_raw.get("data", [])
            if not arr:
                continue
            try:
                buy_lmt = float(arr[0].get("buyLmt") or 0)
                sell_lmt = float(arr[0].get("sellLmt") or 0)
            except (TypeError, ValueError):
                continue
            if buy_lmt > 0 and sell_lmt > 0:
                await self.engine.evaluate(
                    exchange=self.name,
                    symbol=clean_sym,
                    last_price=last_px,
                    mark_price=mark_px,
                    upper_limit=buy_lmt,
                    lower_limit=sell_lmt,
                    volume_24h_usd=vol_usd,
                    best_bid=bid_px,
                    best_ask=ask_px,
                )
            await asyncio.sleep(0.08)


# ==============================================================================
# СКАНЕРЫ ПЛОТНОСТЕЙ (7 БИРЖ: BINANCE, BYBIT, OKX, KUCOIN, ASTER, GATE, BITGET)
# ==============================================================================

class BaseDensityScanner:
    name: str = "Base"
    cycle_pause: float = 8.0
    max_pairs_per_market: int = 35
    min_pair_volume_usd: float = 1_000_000.0
    req_delay: float = 0.16

    def __init__(self, d_engine: DensityEngine, session: aiohttp.ClientSession) -> None:
        self.engine = d_engine
        self.session = session

    async def _get_json(self, url: str, timeout: float = 8.0) -> Optional[object]:
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception:
            pass
        return None

    @staticmethod
    def _parse_levels(raw_levels: list) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        for lvl in raw_levels or []:
            try:
                out.append((float(lvl[0]), float(lvl[1])))
            except (TypeError, ValueError, IndexError):
                pass
        return out

    def _select_pairs(self, candidates: list, sym_index: int) -> list:
        watched = self.engine.watched_bases
        priority = []
        regular = []
        for item in candidates:
            base = extract_base_asset(item[sym_index])
            if base in watched:
                priority.append(item)
            else:
                regular.append(item)
        regular.sort(key=lambda x: x[0], reverse=True)
        return priority + regular[: self.max_pairs_per_market]

    async def scan_once(self) -> None:
        raise NotImplementedError

    async def run_forever(self) -> None:
        await asyncio.sleep(2.0)
        while True:
            try:
                await self.scan_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"[{self.name} Density] Ошибка цикла: {exc}")
            await asyncio.sleep(self.cycle_pause)


class BinanceDensityScanner(BaseDensityScanner):
    name = "Binance"

    async def _scan_market(self, mkt: MarketType, ticker_url: str, depth_url_tpl: str) -> None:
        data = await self._get_json(ticker_url)
        if not isinstance(data, list):
            return
        pairs = []
        for t in data:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or is_blacklisted_symbol(sym):
                continue
            try:
                px = float(t.get("lastPrice") or 0)
                vol = float(t.get("quoteVolume") or 0)
            except (TypeError, ValueError):
                continue
            is_watched = extract_base_asset(sym) in self.engine.watched_bases
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, sym, px))
        for vol, sym, px in self._select_pairs(pairs, sym_index=1):
            ob = await self._get_json(depth_url_tpl.format(sym=sym))
            if isinstance(ob, dict):
                await self.engine.process_orderbook(
                    exchange=self.name,
                    market_type=mkt,
                    symbol=sym,
                    current_price=px,
                    volume_24h_usd=vol,
                    bids=self._parse_levels(ob.get("bids")),
                    asks=self._parse_levels(ob.get("asks")),
                )
            await asyncio.sleep(self.req_delay)

    async def scan_once(self) -> None:
        await self._scan_market(
            MarketType.SPOT,
            "https://api.binance.com/api/v3/ticker/24hr",
            "https://api.binance.com/api/v3/depth?symbol={sym}&limit=100",
        )
        await self._scan_market(
            MarketType.FUTURES,
            "https://fapi.binance.com/fapi/v1/ticker/24hr",
            "https://fapi.binance.com/fapi/v1/depth?symbol={sym}&limit=100",
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
            if not sym.endswith("USDT") or is_blacklisted_symbol(sym):
                continue
            try:
                px = float(t.get("lastPrice") or 0)
                vol = float(t.get("turnover24h") or 0)
            except (TypeError, ValueError):
                continue
            is_watched = extract_base_asset(sym) in self.engine.watched_bases
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, sym, px))
        for vol, sym, px in self._select_pairs(pairs, sym_index=1):
            ob = await self._get_json(
                f"https://api.bybit.com/v5/market/orderbook?category={category}&symbol={sym}&limit=50"
            )
            if isinstance(ob, dict):
                res = ob.get("result", {})
                await self.engine.process_orderbook(
                    exchange=self.name,
                    market_type=mkt,
                    symbol=sym,
                    current_price=px,
                    volume_24h_usd=vol,
                    bids=self._parse_levels(res.get("b")),
                    asks=self._parse_levels(res.get("a")),
                )
            await asyncio.sleep(self.req_delay)

    async def scan_once(self) -> None:
        await self._scan_category(MarketType.SPOT, "spot")
        await self._scan_category(MarketType.FUTURES, "linear")


class OKXDensityScanner(BaseDensityScanner):
    name = "OKX"

    def __init__(self, d_engine: DensityEngine, session: aiohttp.ClientSession) -> None:
        super().__init__(d_engine, session)
        self._ct_vals: Dict[str, float] = {}
        self._last_inst_ts: float = 0.0

    async def _refresh_ct_vals(self) -> None:
        now = time.time()
        if self._ct_vals and now - self._last_inst_ts < 1800:
            return
        raw = await self._get_json("https://www.okx.com/api/v5/public/instruments?instType=SWAP")
        if not isinstance(raw, dict):
            return
        parsed = {}
        for item in raw.get("data", []):
            inst_id = item.get("instId", "")
            if inst_id.endswith("-USDT-SWAP"):
                try:
                    parsed[inst_id] = float(item.get("ctVal") or 1.0)
                except (TypeError, ValueError):
                    pass
        if parsed:
            self._ct_vals = parsed
            self._last_inst_ts = now

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
            if is_blacklisted_symbol(clean_sym):
                continue
            try:
                px = float(t.get("last") or 0)
                vol_usd = float(t.get("volCcy24h") or 0) * (px if inst_type == "SWAP" else 1.0)
            except (TypeError, ValueError):
                continue
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if px > 0 and (vol_usd >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol_usd, inst_id, clean_sym, px))
        for vol_usd, inst_id, clean_sym, px in self._select_pairs(pairs, sym_index=2):
            ob = await self._get_json(f"https://www.okx.com/api/v5/market/books?instId={inst_id}&sz=50")
            if isinstance(ob, dict) and ob.get("data"):
                book = ob["data"][0]
                mult = self._ct_vals.get(inst_id, 1.0) if mkt == MarketType.FUTURES else 1.0
                await self.engine.process_orderbook(
                    exchange=self.name,
                    market_type=mkt,
                    symbol=clean_sym,
                    current_price=px,
                    volume_24h_usd=vol_usd,
                    bids=self._parse_levels(book.get("bids")),
                    asks=self._parse_levels(book.get("asks")),
                    contract_multiplier=mult,
                )
            await asyncio.sleep(self.req_delay)

    async def scan_once(self) -> None:
        await self._refresh_ct_vals()
        await self._scan_type(MarketType.SPOT, "SPOT")
        await self._scan_type(MarketType.FUTURES, "SWAP")


class KuCoinDensityScanner(BaseDensityScanner):
    name = "KuCoin"
    max_pairs_per_market = 28

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
            if is_blacklisted_symbol(clean_sym):
                continue
            try:
                px = float(t.get("last") or 0)
                vol = float(t.get("volValue") or 0)
            except (TypeError, ValueError):
                continue
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, sym_raw, clean_sym, px))
        for vol, sym_raw, clean_sym, px in self._select_pairs(pairs, sym_index=2):
            ob = await self._get_json(
                f"https://api.kucoin.com/api/v1/market/orderbook/level2_100?symbol={sym_raw}"
            )
            if isinstance(ob, dict) and isinstance(ob.get("data"), dict):
                book = ob["data"]
                await self.engine.process_orderbook(
                    exchange=self.name,
                    market_type=MarketType.SPOT,
                    symbol=clean_sym,
                    current_price=px,
                    volume_24h_usd=vol,
                    bids=self._parse_levels(book.get("bids")),
                    asks=self._parse_levels(book.get("asks")),
                )
            await asyncio.sleep(self.req_delay)

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
            if is_blacklisted_symbol(clean_sym):
                continue
            try:
                px = float(c.get("markPrice") or c.get("lastTradePrice") or 0)
                vol = float(c.get("turnoverOf24h") or 0)
                mult = float(c.get("multiplier") or 1.0)
            except (TypeError, ValueError):
                continue
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, sym_raw, clean_sym, px, abs(mult)))
        for vol, sym_raw, clean_sym, px, mult in self._select_pairs(pairs, sym_index=2):
            ob = await self._get_json(
                f"https://api-futures.kucoin.com/api/v1/level2/depth100?symbol={sym_raw}"
            )
            if isinstance(ob, dict) and isinstance(ob.get("data"), dict):
                book = ob["data"]
                await self.engine.process_orderbook(
                    exchange=self.name,
                    market_type=MarketType.FUTURES,
                    symbol=clean_sym,
                    current_price=px,
                    volume_24h_usd=vol,
                    bids=self._parse_levels(book.get("bids")),
                    asks=self._parse_levels(book.get("asks")),
                    contract_multiplier=mult,
                )
            await asyncio.sleep(self.req_delay)

    async def scan_once(self) -> None:
        await self._scan_spot()
        await self._scan_futures()


class AsterDensityScanner(BaseDensityScanner):
    name = "Aster"
    max_pairs_per_market = 30

    async def scan_once(self) -> None:
        data = await self._get_json("https://fapi.asterdex.com/fapi/v3/ticker/24hr")
        if not isinstance(data, list):
            return
        pairs = []
        for t in data:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or is_blacklisted_symbol(sym):
                continue
            try:
                px = float(t.get("lastPrice") or 0)
                vol = float(t.get("quoteVolume") or 0)
            except (TypeError, ValueError):
                continue
            is_watched = extract_base_asset(sym) in self.engine.watched_bases
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, sym, px))
        for vol, sym, px in self._select_pairs(pairs, sym_index=1):
            ob = await self._get_json(
                f"https://fapi.asterdex.com/fapi/v3/depth?symbol={sym}&limit=100"
            )
            if isinstance(ob, dict):
                await self.engine.process_orderbook(
                    exchange=self.name,
                    market_type=MarketType.FUTURES,
                    symbol=sym,
                    current_price=px,
                    volume_24h_usd=vol,
                    bids=self._parse_levels(ob.get("bids")),
                    asks=self._parse_levels(ob.get("asks")),
                )
            await asyncio.sleep(self.req_delay)


class GateDensityScanner(BaseDensityScanner):
    name = "Gate"
    max_pairs_per_market = 28

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
            if is_blacklisted_symbol(clean_sym):
                continue
            try:
                px = float(t.get("last") or 0)
                vol = float(t.get("quote_volume") or 0)
            except (TypeError, ValueError):
                continue
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, pair_raw, clean_sym, px))
        for vol, pair_raw, clean_sym, px in self._select_pairs(pairs, sym_index=2):
            ob = await self._get_json(
                f"https://api.gateio.ws/api/v4/spot/order_book?currency_pair={pair_raw}&limit=50"
            )
            if isinstance(ob, dict):
                await self.engine.process_orderbook(
                    exchange=self.name,
                    market_type=MarketType.SPOT,
                    symbol=clean_sym,
                    current_price=px,
                    volume_24h_usd=vol,
                    bids=self._parse_levels(ob.get("bids")),
                    asks=self._parse_levels(ob.get("asks")),
                )
            await asyncio.sleep(self.req_delay)

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
            if is_blacklisted_symbol(clean_sym):
                continue
            try:
                px = float(c.get("last_price") or c.get("mark_price") or 0)
                mult = float(c.get("quanto_multiplier") or 1.0)
                vol = float(c.get("trade_size") or 0) * px * mult
            except (TypeError, ValueError):
                continue
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, name_raw, clean_sym, px, mult))
        for vol, name_raw, clean_sym, px, mult in self._select_pairs(pairs, sym_index=2):
            ob = await self._get_json(
                f"https://api.gateio.ws/api/v4/futures/usdt/order_book?contract={name_raw}&limit=50"
            )
            if isinstance(ob, dict):
                bids = [(float(b["p"]), float(b["s"])) for b in ob.get("bids", []) if "p" in b and "s" in b]
                asks = [(float(a["p"]), float(a["s"])) for a in ob.get("asks", []) if "p" in a and "s" in a]
                await self.engine.process_orderbook(
                    exchange=self.name,
                    market_type=MarketType.FUTURES,
                    symbol=clean_sym,
                    current_price=px,
                    volume_24h_usd=vol,
                    bids=bids,
                    asks=asks,
                    contract_multiplier=mult,
                )
            await asyncio.sleep(self.req_delay)

    async def scan_once(self) -> None:
        await self._scan_spot()
        await self._scan_futures()


class BitgetDensityScanner(BaseDensityScanner):
    name = "Bitget"
    max_pairs_per_market = 28

    async def _scan_spot(self) -> None:
        raw = await self._get_json("https://api.bitget.com/api/v2/spot/market/tickers")
        if not isinstance(raw, dict):
            return
        pairs = []
        for t in raw.get("data", []):
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or is_blacklisted_symbol(sym):
                continue
            try:
                px = float(t.get("lastPr") or 0)
                vol = float(t.get("usdtVolume") or 0)
            except (TypeError, ValueError):
                continue
            is_watched = extract_base_asset(sym) in self.engine.watched_bases
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, sym, px))
        for vol, sym, px in self._select_pairs(pairs, sym_index=1):
            ob = await self._get_json(
                f"https://api.bitget.com/api/v2/spot/market/orderbook?symbol={sym}&type=step0&limit=50"
            )
            if isinstance(ob, dict) and isinstance(ob.get("data"), dict):
                book = ob["data"]
                await self.engine.process_orderbook(
                    exchange=self.name,
                    market_type=MarketType.SPOT,
                    symbol=sym,
                    current_price=px,
                    volume_24h_usd=vol,
                    bids=self._parse_levels(book.get("bids")),
                    asks=self._parse_levels(book.get("asks")),
                )
            await asyncio.sleep(self.req_delay)

    async def _scan_futures(self) -> None:
        raw = await self._get_json(
            "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
        )
        if not isinstance(raw, dict):
            return
        pairs = []
        for t in raw.get("data", []):
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or is_blacklisted_symbol(sym):
                continue
            try:
                px = float(t.get("lastPr") or 0)
                vol = float(t.get("usdtVolume") or 0)
            except (TypeError, ValueError):
                continue
            is_watched = extract_base_asset(sym) in self.engine.watched_bases
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, sym, px))
        for vol, sym, px in self._select_pairs(pairs, sym_index=1):
            ob = await self._get_json(
                f"https://api.bitget.com/api/v2/mix/market/orderbook?productType=USDT-FUTURES&symbol={sym}&limit=50"
            )
            if isinstance(ob, dict) and isinstance(ob.get("data"), dict):
                book = ob["data"]
                await self.engine.process_orderbook(
                    exchange=self.name,
                    market_type=MarketType.FUTURES,
                    symbol=sym,
                    current_price=px,
                    volume_24h_usd=vol,
                    bids=self._parse_levels(book.get("bids")),
                    asks=self._parse_levels(book.get("asks")),
                )
            await asyncio.sleep(self.req_delay)

    async def scan_once(self) -> None:
        await self._scan_spot()
        await self._scan_futures()


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

_api_cache: Dict[str, object] = {
    "ts": 0.0,
    "utc_date": "",
    "history": [],
    "densities": [],
}
API_CACHE_TTL_SEC = 2.5


def _to_pg_sql(sql: str) -> str:
    parts = sql.split("?")
    if len(parts) == 1:
        return sql
    return "".join(f"{p}${i}" for i, p in enumerate(parts[:-1], 1)) + parts[-1]


async def db_execute(sql: str, params: tuple = ()) -> None:
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            await conn.execute(_to_pg_sql(sql), *params)
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(sql, params)
            await db.commit()


async def db_fetchall(sql: str, params: tuple = ()) -> List[dict]:
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
    rows = await db_fetchall(sql, params)
    return rows[0] if rows else None


def parse_coin_rules(raw: Optional[str]) -> Dict[str, Dict[str, float]]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        clean: Dict[str, Dict[str, float]] = {}
        for k, v in data.items():
            base = extract_base_asset(str(k))
            if not base or not isinstance(v, dict):
                continue
            rule: Dict[str, float] = {}
            if v.get("spot") is not None and float(v["spot"]) > 0:
                rule["spot"] = float(v["spot"])
            if v.get("futures") is not None and float(v["futures"]) > 0:
                rule["futures"] = float(v["futures"])
            if rule:
                clean[base] = rule
        return clean
    except Exception:
        return {}


def parse_banned_coins(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if not isinstance(data, list):
            return []
        bases = {extract_base_asset(str(item)) for item in data}
        return sorted(b for b in bases if b)
    except Exception:
        return []


def get_user_density_threshold(
    symbol: str,
    market_type_str: str,
    default_spot: float,
    default_futures: float,
    coin_rules: Dict[str, Dict[str, float]],
) -> float:
    base = extract_base_asset(symbol)
    rule = coin_rules.get(base)
    if market_type_str == "SPOT":
        if rule and "spot" in rule:
            return rule["spot"]
        return default_spot
    if rule and "futures" in rule:
        return rule["futures"]
    return default_futures


async def sync_density_engine_custom_floors() -> None:
    try:
        rows = await db_fetchall("SELECT coin_rules FROM users")
    except Exception:
        return

    spot_floors: Dict[str, float] = {}
    futures_floors: Dict[str, float] = {}

    for row in rows:
        rules = parse_coin_rules(row.get("coin_rules"))
        for base, cfg in rules.items():
            if "spot" in cfg:
                val = max(10_000.0, float(cfg["spot"]))
                if base not in spot_floors or val < spot_floors[base]:
                    spot_floors[base] = val
            if "futures" in cfg:
                val = max(25_000.0, float(cfg["futures"]))
                if base not in futures_floors or val < futures_floors[base]:
                    futures_floors[base] = val

    density_engine.update_custom_floors(spot_floors, futures_floors)


async def init_db() -> None:
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
            min_density_futures REAL DEFAULT 1000000,
            coin_rules TEXT DEFAULT '{{}}',
            banned_coins TEXT DEFAULT '[]'
        )
        """
    )

    for col_sql in (
        "ALTER TABLE users ADD COLUMN min_density_spot REAL DEFAULT 250000",
        "ALTER TABLE users ADD COLUMN min_density_futures REAL DEFAULT 1000000",
        "ALTER TABLE users ADD COLUMN coin_rules TEXT DEFAULT '{}'",
        "ALTER TABLE users ADD COLUMN banned_coins TEXT DEFAULT '[]'",
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

    await sync_density_engine_custom_floors()


async def close_db() -> None:
    global _pg_pool
    if _pg_pool is not None:
        await _pg_pool.close()
        _pg_pool = None


def _invalidate_api_cache() -> None:
    _api_cache["ts"] = 0.0


def _build_main_keyboard() -> Optional[InlineKeyboardMarkup]:
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔴 Открыть Лайв (Аптики & Плотности)", web_app=WebAppInfo(url=WEBAPP_URL))]
        ]
    )


async def _broadcast_messages(user_ids: List[int], text: str, kb: Optional[InlineKeyboardMarkup]) -> None:
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
        users = await db_fetchall("SELECT user_id, min_volume, banned_coins FROM users")
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

    base = extract_base_asset(event.symbol)
    target_ids: List[int] = []
    for u in users:
        banned = set(parse_banned_coins(u.get("banned_coins")))
        if base in banned:
            continue
        if event.volume_24h_usd >= float(u.get("min_volume") or DEFAULT_MIN_VOLUME):
            target_ids.append(int(u["user_id"]))

    await _broadcast_messages(target_ids, text, _build_main_keyboard())


async def handle_density_alert(event: DensityEvent) -> None:
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
            "SELECT user_id, min_density_spot, min_density_futures, coin_rules, banned_coins FROM users"
        )
    except Exception as exc:
        logger.error(f"Ошибка записи плотности в БД: {exc}")
        return

    if not bot:
        return

    side_emoji = "🟢" if event.side == DensitySide.LONG else "🔴"
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

    base = extract_base_asset(event.symbol)
    target_ids: List[int] = []
    for u in users:
        banned = set(parse_banned_coins(u.get("banned_coins")))
        if base in banned:
            continue
        u_spot = float(u.get("min_density_spot") or DEFAULT_MIN_DENSITY_SPOT)
        u_fut = float(u.get("min_density_futures") or DEFAULT_MIN_DENSITY_FUTURES)
        u_rules = parse_coin_rules(u.get("coin_rules"))
        threshold = get_user_density_threshold(
            event.symbol, event.market_type.value, u_spot, u_fut, u_rules
        )
        if event.size_usd >= threshold:
            target_ids.append(int(u["user_id"]))

    await _broadcast_messages(target_ids, text, _build_main_keyboard())


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
        INSERT INTO users (user_id, min_volume, min_density_spot, min_density_futures, coin_rules, banned_coins)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO NOTHING
        """,
        (
            message.from_user.id,
            DEFAULT_MIN_VOLUME,
            DEFAULT_MIN_DENSITY_SPOT,
            DEFAULT_MIN_DENSITY_FUTURES,
            "{}",
            "[]",
        ),
    )

    kb = _build_main_keyboard()
    hint = (
        "Нажмите кнопку ниже, чтобы открыть окно «Лайв» (со звуком LONG/SHORT, фильтрами и Tiger.Trade)."
        if kb
        else "⚠️ Переменная WEBAPP_URL еще не задана."
    )
    await message.answer(
        "👋 <b>Аптик + Сканер плотностей запущен!</b>\n\n"
        "⚡ <b>Аптики (4):</b> Binance, Bybit, OKX, Aster.\n"
        "🧱 <b>Плотности (7):</b> Binance, Bybit, OKX, KuCoin, Aster, Gate, Bitget.\n"
        "🐯 <b>Tiger.Trade:</b> открытие по клику или автооткрытие в Mini App.\n"
        "🚫 Исключены: тяжелые монеты, акции, ETF, нефть и металлы.\n\n"
        "<b>Команды:</b>\n"
        "• <code>/vol 500000</code> — мин. оборот 24ч ($) для аптиков\n"
        "• <code>/dspot 250000</code> — общий порог плотности на Споте ($)\n"
        "• <code>/dfut 1000000</code> — общий порог плотности на Фьючерсах ($)\n"
        "• <code>/coin WIF 100000 500000</code> — свой порог плотности для монеты\n"
        "• <code>/ban MOODENG</code> — убрать монету из всех алертов\n"
        "• <code>/unban MOODENG</code> — вернуть монету обратно\n"
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
        await message.answer(f"✅ Общий порог плотности на Споте: <b>${val:,.0f}</b>", parse_mode="HTML")
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
        await message.answer(f"✅ Общий порог плотности на Фьючерсах: <b>${val:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/dfut 1000000</code> (минимум $1,000,000)", parse_mode="HTML")


@dp.message(Command("coin"))
async def cmd_coin(message: types.Message) -> None:
    try:
        parts = (message.text or "").split()
        if len(parts) < 3:
            raise ValueError()
        base = extract_base_asset(parts[1])
        if not base or is_blacklisted_symbol(base):
            await message.answer("❌ Эта монета в глобальном черном списке или указана неверно.")
            return
        spot_val = max(10_000.0, float(parts[2].replace(",", "").replace("$", "")))
        fut_val = (
            max(25_000.0, float(parts[3].replace(",", "").replace("$", "")))
            if len(parts) >= 4
            else max(25_000.0, spot_val)
        )

        row = await db_fetchone("SELECT coin_rules FROM users WHERE user_id = ?", (message.from_user.id,))
        rules = parse_coin_rules(row.get("coin_rules") if row else None)
        rules[base] = {"spot": spot_val, "futures": fut_val}

        await db_execute(
            "INSERT INTO users (user_id, coin_rules) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET coin_rules = excluded.coin_rules",
            (message.from_user.id, json.dumps(rules)),
        )
        await sync_density_engine_custom_floors()
        await message.answer(
            f"✅ Для монеты <b>{base}</b> заданы плотности:\n"
            f"• Спот от: <b>${spot_val:,.0f}</b>\n"
            f"• Фьючерсы от: <b>${fut_val:,.0f}</b>",
            parse_mode="HTML",
        )
    except Exception:
        await message.answer(
            "Использование:\n"
            "• <code>/coin WIF 100000 500000</code> (Спот $100K, Фьючерс $500K)\n"
            "• <code>/delcoin WIF</code> (удалить настройку)",
            parse_mode="HTML",
        )


@dp.message(Command("delcoin"))
async def cmd_delcoin(message: types.Message) -> None:
    try:
        parts = (message.text or "").split()
        base = extract_base_asset(parts[1])
        row = await db_fetchone("SELECT coin_rules FROM users WHERE user_id = ?", (message.from_user.id,))
        rules = parse_coin_rules(row.get("coin_rules") if row else None)
        if base in rules:
            rules.pop(base)
            await db_execute(
                "UPDATE users SET coin_rules = ? WHERE user_id = ?",
                (json.dumps(rules), message.from_user.id),
            )
            await sync_density_engine_custom_floors()
            await message.answer(f"🗑 Индивидуальный порог для <b>{base}</b> удален.", parse_mode="HTML")
        else:
            await message.answer(f"Для <b>{base}</b> нет индивидуальной настройки.", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/delcoin WIF</code>", parse_mode="HTML")


@dp.message(Command("ban"))
async def cmd_ban(message: types.Message) -> None:
    try:
        base = extract_base_asset((message.text or "").split()[1])
        if not base:
            raise ValueError()
        row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (message.from_user.id,))
        banned = set(parse_banned_coins(row.get("banned_coins") if row else None))
        banned.add(base)
        await db_execute(
            "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
            (message.from_user.id, json.dumps(sorted(banned))),
        )
        await message.answer(f"🚫 Монета <b>{base}</b> скрыта (больше не будет приходить).", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/ban MOODENG</code>", parse_mode="HTML")


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message) -> None:
    try:
        base = extract_base_asset((message.text or "").split()[1])
        row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (message.from_user.id,))
        banned = set(parse_banned_coins(row.get("banned_coins") if row else None))
        if base in banned:
            banned.discard(base)
            await db_execute(
                "UPDATE users SET banned_coins = ? WHERE user_id = ?",
                (json.dumps(sorted(banned)), message.from_user.id),
            )
            await message.answer(f"✅ Монета <b>{base}</b> возвращена в алерты.", parse_mode="HTML")
        else:
            await message.answer(f"Монета <b>{base}</b> не была скрыта.", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/unban MOODENG</code>", parse_mode="HTML")


@dp.message(Command("status"))
async def cmd_status(message: types.Message) -> None:
    active_u = engine.get_active_upticks()
    active_d = density_engine.get_active_densities()

    lines = []
    if active_u:
        lines.append("⚡ <b>Активные аптики сейчас:</b>")
        for ex, sym, side in active_u:
            emoji = "🟢" if side == UptickSide.LONG else "🔴"
            lines.append(f"{emoji} <b>{sym}</b> ({ex}) — <b>{side.value}</b>")
    else:
        lines.append("⚡ Активных аптиков прямо сейчас нет.")

    lines.append("")
    if active_d:
        lines.append(f"🧱 <b>Активные плотности в стакане ({len(active_d)}):</b>")
        for d in active_d[:12]:
            emoji = "🟢" if d.side == DensitySide.LONG else "🔴"
            lines.append(
                f"{emoji} <b>{d.symbol}</b> ({d.exchange} {d.market_type.value}) — "
                f"<b>{d.side.value}</b> | <b>${d.size_usd:,.0f}</b> @ {d.price} ({d.distance_pct}%)"
            )
    else:
        lines.append("🧱 Крупных плотностей прямо сейчас не найдено.")

    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=_build_main_keyboard())


# ==============================================================================
# FASTAPI + TELEGRAM MINI APP (ОКНО «ЛАЙВ», УДАЛЕНИЕ МОНЕТ, LONG/SHORT, TIGER)
# ==============================================================================

WEBAPP_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Лайв: Аптики & Плотности</title>
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
            width: 8px; height: 8px; border-radius: 50%; background: var(--short);
            box-shadow: 0 0 8px var(--short); display: inline-block;
            animation: pulse 1.6s infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.45; transform: scale(0.85); }
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
            border-radius: 14px; padding: 11px 12px; margin-bottom: 9px;
            display: flex; justify-content: space-between; align-items: center; gap: 10px;
            cursor: pointer; transition: transform 0.12s, border-color 0.2s;
        }
        .card:hover { border-color: rgba(245, 183, 0, 0.45); }
        .card:active { transform: scale(0.985); }
        .card-uptick { border-left: 3px solid var(--gold); }
        .card-density { border-left: 3px solid var(--accent); }
        .sym-row { display: flex; align-items: center; gap: 6px; margin-bottom: 4px; flex-wrap: wrap; }
        .symbol { font-size: 15px; font-weight: 700; }
        .kind-tag {
            font-size: 10px; font-weight: 800; padding: 2px 6px; border-radius: 6px;
        }
        .kind-uptick { background: rgba(245, 183, 0, 0.18); color: var(--gold); }
        .kind-density { background: rgba(42, 171, 238, 0.18); color: #5ec6ff; }
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
        .right-col { text-align: right; flex-shrink: 0; display: flex; flex-direction: column; align-items: flex-end; gap: 3px; }
        .right-top { display: flex; align-items: center; gap: 6px; }
        .ban-mini-btn {
            border: 1px solid rgba(234, 57, 67, 0.35);
            background: rgba(234, 57, 67, 0.12);
            color: var(--short);
            border-radius: 6px;
            padding: 2px 6px;
            font-size: 11px;
            font-weight: 700;
            cursor: pointer;
            line-height: 1.3;
        }
        .side-badge {
            display: inline-block; font-size: 11px; font-weight: 700;
            padding: 4px 8px; border-radius: 7px;
        }
        .side-long { background: rgba(22, 199, 132, 0.16); color: var(--long); }
        .side-short { background: rgba(234, 57, 67, 0.16); color: var(--short); }
        .size-big { font-size: 14px; font-weight: 800; color: var(--text); }
        .time-str { font-size: 11px; color: var(--hint); font-variant-numeric: tabular-nums; }
        .section-title {
            font-size: 12px; font-weight: 700; color: var(--hint);
            margin: 10px 0 7px; text-transform: uppercase; letter-spacing: 0.4px;
            display: flex; justify-content: space-between; align-items: center;
        }
        .live-split {
            display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 10px;
        }
        @media (max-width: 520px) {
            .live-split { grid-template-columns: 1fr; }
        }
        .live-box {
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 10px;
        }
        .live-box-head {
            font-size: 12px; font-weight: 700; color: var(--text);
            margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center;
        }
        .live-count-pill {
            font-size: 11px; font-weight: 700; padding: 2px 7px; border-radius: 10px;
            background: rgba(255, 255, 255, 0.08); color: var(--hint);
        }
        .empty {
            text-align: center; padding: 22px 12px; color: var(--hint);
            background: var(--card); border-radius: 12px; font-size: 12px;
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
        input[type="number"], input[type="text"], select {
            width: 100%; padding: 9px 11px; border-radius: 10px; border: 1px solid var(--border);
            background: var(--bg); color: var(--text); font-size: 14px; outline: none;
        }
        .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin: 10px 0; }
        .save-btn {
            background: var(--accent); color: var(--accent-text); border: none;
            padding: 11px 16px; border-radius: 10px; font-weight: 700; font-size: 14px;
            cursor: pointer; width: 100%; margin-top: 4px;
        }
        .danger-btn {
            background: rgba(234, 57, 67, 0.16); color: var(--short);
            border: 1px solid rgba(234, 57, 67, 0.4);
            padding: 9px 14px; border-radius: 10px; font-weight: 700; font-size: 13px;
            cursor: pointer; flex-shrink: 0;
        }
        .presets { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; margin-bottom: 12px; }
        .preset-btn {
            background: var(--bg); color: var(--text); border: 1px solid var(--border);
            padding: 7px 4px; border-radius: 8px; font-size: 11px; font-weight: 600; cursor: pointer;
        }
        .coin-rule-item {
            display: flex; justify-content: space-between; align-items: center; gap: 8px;
            background: var(--bg); border: 1px solid var(--border);
            border-radius: 10px; padding: 9px 11px; margin-top: 8px;
        }
        .coin-rule-name { font-size: 14px; font-weight: 700; }
        .coin-rule-vals { font-size: 12px; color: var(--hint); margin-top: 2px; }
        .del-coin-btn {
            border: 1px solid rgba(234, 57, 67, 0.4); background: rgba(234, 57, 67, 0.14);
            color: var(--short); border-radius: 8px; padding: 6px 10px;
            font-size: 12px; font-weight: 700; cursor: pointer; flex-shrink: 0;
        }
        .banned-tags {
            display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px;
        }
        .banned-chip {
            display: inline-flex; align-items: center; gap: 6px;
            background: rgba(234, 57, 67, 0.12); border: 1px solid rgba(234, 57, 67, 0.35);
            color: var(--text); padding: 5px 9px; border-radius: 16px;
            font-size: 12px; font-weight: 700;
        }
        .banned-chip button {
            border: none; background: transparent; color: var(--short);
            font-weight: 800; cursor: pointer; padding: 0; font-size: 13px;
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
        <div class="title"><span class="live-dot"></span> Лайв Скринер</div>
        <div class="header-right">
            <button class="tiger-pill disconnected" id="tiger-status-btn" onclick="onTigerPillClick()" title="Статус Tiger.Trade">
                <span class="tiger-dot"></span> 🐯 Tiger
            </button>
            <button class="sound-btn on" id="sound-toggle-btn" onclick="toggleSound()">🔊</button>
            <div class="utc-badge" id="utc-clock">UTC --:--:--</div>
        </div>
    </div>

    <div class="tabs">
        <button class="tab-btn active" onclick="switchTab('live', this)">🔴 Лайв (<span id="live-count">0</span>)</button>
        <button class="tab-btn" onclick="switchTab('upticks', this)">Аптики</button>
        <button class="tab-btn" onclick="switchTab('densities', this)">Плотности</button>
        <button class="tab-btn" onclick="switchTab('settings', this)">Настройки</button>
    </div>

    <!-- ГЛАВНОЕ ОКНО ЛАЙВ (АПТИКИ + ПЛОТНОСТИ ОДНОВРЕМЕННО) -->
    <div id="panel-live" class="panel active">
        <div class="live-split">
            <div class="live-box">
                <div class="live-box-head">
                    <span>⚡ Аптики сейчас</span>
                    <span class="live-count-pill" id="live-u-count">0</span>
                </div>
                <div id="live-upticks-list"><div class="empty">Активных планок нет</div></div>
            </div>
            <div class="live-box">
                <div class="live-box-head">
                    <span>🧱 Плотности в стакане</span>
                    <span class="live-count-pill" id="live-d-count">0</span>
                </div>
                <div id="live-densities-list"><div class="empty">Крупных плотностей нет</div></div>
            </div>
        </div>

        <div class="section-title">
            <span>📡 Общая лента Лайв (Аптики + Плотности)</span>
        </div>
        <div class="filters" id="live-feed-filters">
            <button class="chip active" onclick="setLiveFeedMode('ALL', this)">Все вместе</button>
            <button class="chip" onclick="setLiveFeedMode('UPTICK', this)">⚡ Только Аптики</button>
            <button class="chip" onclick="setLiveFeedMode('DENSITY', this)">🧱 Только Плотности</button>
        </div>
        <div id="live-feed-list"><div class="empty">Загрузка живой ленты...</div></div>
    </div>

    <!-- ВКЛАДКА АПТИКИ -->
    <div id="panel-upticks" class="panel">
        <div class="filters" id="uptick-ex-filters">
            <button class="chip active" onclick="setUptickEx('ALL', this)">Все (4)</button>
            <button class="chip" data-ex="Binance" onclick="setUptickEx('Binance', this)"></button>
            <button class="chip" data-ex="Bybit" onclick="setUptickEx('Bybit', this)"></button>
            <button class="chip" data-ex="OKX" onclick="setUptickEx('OKX', this)"></button>
            <button class="chip" data-ex="Aster" onclick="setUptickEx('Aster', this)"></button>
        </div>
        <div id="history-list"><div class="empty">Загрузка данных...</div></div>
    </div>

    <!-- ВКЛАДКА ПЛОТНОСТИ -->
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

    <!-- ВКЛАДКА НАСТРОЙКИ -->
    <div id="panel-settings" class="panel">
        <!-- Скрытые монеты (личный черный список) -->
        <div class="setting-box">
            <label class="setting-label">🚫 Убрать монеты (личный черный список)</label>
            <div class="meta" style="margin-bottom:10px;">
                Добавьте тикер, чтобы скрыть его из всех аптиков, плотностей и Telegram-уведомлений (также можно нажать ✕ прямо на карточке).
            </div>
            <div class="input-row">
                <input type="text" id="ban-coin-input" placeholder="Например: MOODENG, TRUMP, DOGS">
                <button class="danger-btn" onclick="banCoinFromInput()">🚫 Убрать</button>
            </div>
            <div class="banned-tags" id="banned-coins-list"></div>
        </div>

        <!-- Индивидуальный порог плотности по монете -->
        <div class="setting-box">
            <label class="setting-label">🎯 Свой порог плотности для конкретной монеты</label>
            <div class="meta" style="margin-bottom:10px;">
                Выберите монету и задайте от какой суммы ($) видеть плотности именно по ней.
            </div>

            <div class="input-row">
                <input type="text" id="custom-coin-input" list="coin-suggestions" placeholder="Тикер монеты (напр. WIF, BONK, VIRTUAL)">
                <datalist id="coin-suggestions">
                    <option value="WIF"></option>
                    <option value="BONK"></option>
                    <option value="FLOKI"></option>
                    <option value="VIRTUAL"></option>
                    <option value="FARTCOIN"></option>
                    <option value="PENGU"></option>
                    <option value="TRUMP"></option>
                    <option value="HYPE"></option>
                    <option value="ENA"></option>
                    <option value="ONDO"></option>
                    <option value="POPCAT"></option>
                    <option value="MOODENG"></option>
                </datalist>
            </div>

            <div class="two-col" style="margin-top:0;">
                <div>
                    <label class="setting-label" style="font-size:12px;">Спот от ($)</label>
                    <input type="number" id="custom-coin-spot" min="10000" step="10000" placeholder="100000">
                </div>
                <div>
                    <label class="setting-label" style="font-size:12px;">Фьючерс от ($)</label>
                    <input type="number" id="custom-coin-fut" min="25000" step="25000" placeholder="300000">
                </div>
            </div>

            <div class="presets">
                <button class="preset-btn" onclick="setCoinPreset(50000, 150000)">$50K / $150K</button>
                <button class="preset-btn" onclick="setCoinPreset(100000, 300000)">$100K / $300K</button>
                <button class="preset-btn" onclick="setCoinPreset(250000, 500000)">$250K / $500K</button>
                <button class="preset-btn" onclick="setCoinPreset(500000, 2000000)">$500K / $2M</button>
            </div>

            <button class="save-btn" onclick="addOrUpdateCoinRule()">➕ Сохранить порог для монеты</button>
            <div id="coin-rules-list" style="margin-top:8px;"></div>
        </div>

        <!-- Tiger.Trade -->
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
                <button class="secondary-btn" onclick="testTigerOpen()">▶ Тест: WIFUSDT</button>
            </div>

            <p class="meta" style="margin:10px 0 0;">
                В терминале Tiger.Trade откройте: <b>Настройки → Локальный сервер сигналов → Включить сервер сигналов → Сохранить</b>.
            </p>
        </div>

        <!-- Звук -->
        <div class="setting-box">
            <div class="setting-row">
                <div>
                    <div class="setting-label" style="margin-bottom:2px;">Голосовые оповещения (LONG / SHORT)</div>
                    <div class="meta">Озвучивает биржу, монету и сторону (лонг/шорт)</div>
                </div>
                <button class="sound-btn on" id="sound-settings-btn" onclick="toggleSound()">🔊 Вкл</button>
            </div>
            <div class="btn-grid">
                <button class="secondary-btn" onclick="testUptickSound()">▶ Тест: Аптик</button>
                <button class="secondary-btn" onclick="testDensitySound()">▶ Тест: Плотность</button>
            </div>
        </div>

        <!-- Общие фильтры -->
        <div class="setting-box">
            <label class="setting-label">🧱 Общий порог плотности на СПОТЕ ($)</label>
            <div class="input-row">
                <input type="number" id="min-dspot-input" min="250000" step="50000" placeholder="250000">
            </div>
            <div class="presets">
                <button class="preset-btn" onclick="setField('min-dspot-input', 250000)">$250K</button>
                <button class="preset-btn" onclick="setField('min-dspot-input', 500000)">$500K</button>
                <button class="preset-btn" onclick="setField('min-dspot-input', 1000000)">$1M</button>
                <button class="preset-btn" onclick="setField('min-dspot-input', 2000000)">$2M</button>
            </div>

            <label class="setting-label">🧱 Общий порог плотности на ФЬЮЧЕРСАХ ($)</label>
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

            <button class="save-btn" onclick="saveSettings()">💾 Сохранить общие фильтры</button>

            <p class="meta" style="margin-top:12px;">
                Аптики (4): Binance, Bybit, OKX, Aster.<br>
                Плотности (7): Binance, Bybit, OKX, KuCoin, Aster, Gate, Bitget.<br>
                Акции, ETF, нефть, металлы и тяжелые монеты исключены автоматически.
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
        let liveFeedMode = 'ALL';

        let cachedHistory = [];
        let cachedDensities = [];
        let userCoinRules = {};
        let userBannedCoins = [];
        let settingsInitialized = false;

        let isFirstLoad = true;
        let lastSeenUptickId = 0;
        let lastSeenDensityId = 0;

        // ===== TIGER.TRADE =====
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
            openInTiger('Binance', 'FUTURES', 'WIFUSDT', false);
        }

        updateTigerUI();
        connectTiger();

        // ===== ЗВУК И ГОЛОС (LONG / SHORT) =====
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

        function isLongSide(sideStr) {
            const s = (sideStr || '').toUpperCase();
            return s.includes('LONG') || s.includes('BID');
        }

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

        function speakDensity(exchange, marketType, symbol, isLong) {
            if (!soundEnabled) return;
            playChime(isLong ? [523, 659, 784] : [784, 659, 523]);
            try { tg.HapticFeedback?.notificationOccurred('success'); } catch (e) {}
            const exName = EXCHANGE_PRONOUNCE[exchange] || exchange;
            const mktName = marketType === 'SPOT' ? 'спот' : 'фьючерс';
            const cleanSym = (symbol || '').replace(/USDT$/i, '');
            const sideText = isLong ? 'лонг' : 'шорт';
            speakText(`Плотность на бирже ${exName}, ${mktName}. Монета ${cleanSym}. ${sideText}`);
        }

        function testUptickSound() {
            unlockAudio();
            if (!soundEnabled) toggleSound();
            speakUptick('Aster', 'WIFUSDT', true);
            showToast('🔊 Тест: Аптик LONG');
        }

        function testDensitySound() {
            unlockAudio();
            if (!soundEnabled) toggleSound();
            speakDensity('Binance', 'SPOT', 'WIFUSDT', false);
            showToast('🔊 Тест: Плотность SHORT');
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

        function setLiveFeedMode(mode, btn) {
            liveFeedMode = mode;
            document.querySelectorAll('#live-feed-filters .chip').forEach(c => c.classList.remove('active'));
            btn.classList.add('active');
            renderLiveFeed();
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

        // ===== ЛИЧНЫЙ ЧЕРНЫЙ СПИСОК МОНЕТ =====
        function renderBannedCoins() {
            const box = document.getElementById('banned-coins-list');
            if (!userBannedCoins.length) {
                box.innerHTML = '<div class="meta">Скрытых вручную монет пока нет.</div>';
                return;
            }
            box.innerHTML = userBannedCoins.map(sym => `
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
                    body: JSON.stringify({ user_id: userId, symbol: clean })
                });
                const data = await res.json();
                if (res.ok && data.status === 'ok') {
                    userBannedCoins = data.banned_coins || [];
                    renderBannedCoins();
                    showToast(`🚫 Монета ${data.base} скрыта`);
                    loadData();
                } else {
                    showToast(data.detail || 'Ошибка скрытия монеты');
                }
            } catch {
                showToast('Нет связи с сервером');
            }
        }

        function banCoinFromInput() {
            const inp = document.getElementById('ban-coin-input');
            const val = (inp.value || '').trim();
            if (!val) { showToast('Введите тикер монеты'); return; }
            inp.value = '';
            banCoin(val, null);
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
                    userBannedCoins = data.banned_coins || [];
                    renderBannedCoins();
                    showToast(`✅ Монета ${symbol} возвращена`);
                    loadData();
                }
            } catch {
                showToast('Ошибка');
            }
        }

        // ===== ИНДИВИДУАЛЬНЫЕ ПОРОГИ ПО МОНЕТАМ =====
        function setCoinPreset(spotVal, futVal) {
            document.getElementById('custom-coin-spot').value = spotVal;
            document.getElementById('custom-coin-fut').value = futVal;
        }

        function renderCoinRules() {
            const box = document.getElementById('coin-rules-list');
            const keys = Object.keys(userCoinRules || {}).sort();
            if (!keys.length) {
                box.innerHTML = '<div class="meta" style="text-align:center; padding:6px 0;">Индивидуальных правил по монетам пока нет.</div>';
                return;
            }
            box.innerHTML = keys.map(sym => {
                const r = userCoinRules[sym] || {};
                const spotTxt = r.spot ? `Спот: <b>${formatVol(r.spot)}</b>` : 'Спот: общий';
                const futTxt = r.futures ? `Фьючерс: <b>${formatVol(r.futures)}</b>` : 'Фьючерс: общий';
                return `
                    <div class="coin-rule-item">
                        <div onclick="fillCoinForm('${sym}')" style="cursor:pointer; flex:1;">
                            <div class="coin-rule-name">${sym}</div>
                            <div class="coin-rule-vals">${spotTxt} • ${futTxt}</div>
                        </div>
                        <button class="del-coin-btn" onclick="deleteCoinRule('${sym}')">✕</button>
                    </div>
                `;
            }).join('');
        }

        function fillCoinForm(sym) {
            const r = userCoinRules[sym] || {};
            document.getElementById('custom-coin-input').value = sym;
            document.getElementById('custom-coin-spot').value = r.spot || '';
            document.getElementById('custom-coin-fut').value = r.futures || '';
        }

        async function addOrUpdateCoinRule() {
            if (!userId) { showToast('Откройте приложение через Telegram'); return; }
            const sym = (document.getElementById('custom-coin-input').value || '').trim().toUpperCase();
            const spotRaw = document.getElementById('custom-coin-spot').value;
            const futRaw = document.getElementById('custom-coin-fut').value;

            if (!sym) { showToast('Введите тикер монеты (например WIF)'); return; }
            const spotVal = spotRaw ? Number(spotRaw) : null;
            const futVal = futRaw ? Number(futRaw) : null;
            if (!spotVal && !futVal) { showToast('Укажите порог для Спота и/или Фьючерса'); return; }

            try {
                const res = await fetch('/api/coin-rule', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        user_id: userId,
                        symbol: sym,
                        min_spot: spotVal,
                        min_futures: futVal
                    })
                });
                const data = await res.json();
                if (res.ok && data.status === 'ok') {
                    userCoinRules = data.coin_rules || {};
                    renderCoinRules();
                    document.getElementById('custom-coin-input').value = '';
                    showToast(`✅ Порог для ${data.base} сохранен`);
                    loadData();
                } else {
                    showToast(data.detail || 'Ошибка сохранения монеты');
                }
            } catch {
                showToast('Нет связи с сервером');
            }
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
            } catch {
                showToast('Ошибка удаления');
            }
        }

        // ===== РЕНДЕР КАРТОЧЕК =====
        function renderUptickCard(item, showKindTag = false) {
            const isLong = isLongSide(item.side);
            return `
                <div class="card card-uptick" onclick="openInTiger('${item.exchange}', 'FUTURES', '${item.symbol}', false)">
                    <div>
                        <div class="sym-row">
                            ${getExchangeLogoHtml(item.exchange)}
                            <span class="symbol">${item.symbol}</span>
                            ${showKindTag ? '<span class="kind-tag kind-uptick">⚡ АПТИК</span>' : ''}
                        </div>
                        <div class="meta">
                            Цена: ${item.price} • Планка: ${Number(item.limit_price).toPrecision(5)}<br>
                            Оборот 24ч: <b>${formatVol(item.volume)}</b>
                        </div>
                    </div>
                    <div class="right-col">
                        <div class="right-top">
                            <span class="side-badge ${isLong ? 'side-long' : 'side-short'}">
                                ${isLong ? '▲ LONG' : '▼ SHORT'}
                            </span>
                            <button class="ban-mini-btn" onclick="banCoin('${item.symbol}', event)" title="Убрать монету">✕</button>
                        </div>
                        <div class="time-str">${item.timestamp_utc}</div>
                    </div>
                </div>
            `;
        }

        function renderDensityCard(item, showKindTag = false) {
            const isLong = isLongSide(item.side);
            const isSpot = item.market_type === 'SPOT';
            return `
                <div class="card card-density" onclick="openInTiger('${item.exchange}', '${item.market_type}', '${item.symbol}', false)">
                    <div>
                        <div class="sym-row">
                            ${getExchangeLogoHtml(item.exchange)}
                            <span class="symbol">${item.symbol}</span>
                            ${showKindTag ? '<span class="kind-tag kind-density">🧱 ПЛОТН.</span>' : ''}
                            <span class="ex-tag ${isSpot ? 'mkt-spot' : 'mkt-fut'}">${isSpot ? 'SPOT' : 'FUT'}</span>
                        </div>
                        <div class="meta">
                            Уровень: <b>${item.price}</b> (до цены: ${item.distance_pct}%)<br>
                            Оборот 24ч: ${formatVol(item.volume)}
                        </div>
                    </div>
                    <div class="right-col">
                        <div class="right-top">
                            <span class="size-big">${formatVol(item.size_usd)}</span>
                            <button class="ban-mini-btn" onclick="banCoin('${item.symbol}', event)" title="Убрать монету">✕</button>
                        </div>
                        <span class="side-badge ${isLong ? 'side-long' : 'side-short'}">
                            ${isLong ? '▲ LONG' : '▼ SHORT'}
                        </span>
                        <div class="time-str">${item.timestamp_utc}</div>
                    </div>
                </div>
            `;
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
            list.innerHTML = filtered.map(item => renderUptickCard(item, false)).join('');
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
            list.innerHTML = filtered.map(item => renderDensityCard(item, false)).join('');
        }

        function renderLiveFeed() {
            const list = document.getElementById('live-feed-list');
            let merged = [];
            if (liveFeedMode === 'ALL' || liveFeedMode === 'UPTICK') {
                merged = merged.concat(cachedHistory.map(i => ({ ...i, _kind: 'UPTICK' })));
            }
            if (liveFeedMode === 'ALL' || liveFeedMode === 'DENSITY') {
                merged = merged.concat(cachedDensities.map(i => ({ ...i, _kind: 'DENSITY' })));
            }
            merged.sort((a, b) => (b.timestamp_utc || '').localeCompare(a.timestamp_utc || ''));

            if (!merged.length) {
                list.innerHTML = '<div class="empty">Событий за сегодня пока нет — лента обновляется автоматически.</div>';
                return;
            }
            list.innerHTML = merged.slice(0, 120).map(item =>
                item._kind === 'UPTICK' ? renderUptickCard(item, true) : renderDensityCard(item, true)
            ).join('');
        }

        function renderLive(activeUpticks, activeDensities) {
            document.getElementById('live-count').textContent = activeUpticks.length + activeDensities.length;
            document.getElementById('live-u-count').textContent = activeUpticks.length;
            document.getElementById('live-d-count').textContent = activeDensities.length;

            const uList = document.getElementById('live-upticks-list');
            if (!activeUpticks.length) {
                uList.innerHTML = '<div class="empty">Активных планок нет</div>';
            } else {
                uList.innerHTML = activeUpticks.map(item => {
                    const isLong = isLongSide(item.side);
                    return `
                        <div class="card card-uptick" style="margin-bottom:6px; padding:9px 10px;" onclick="openInTiger('${item.exchange}', 'FUTURES', '${item.symbol}', false)">
                            <div class="sym-row" style="margin:0;">
                                ${getExchangeLogoHtml(item.exchange)}
                                <span class="symbol" style="font-size:14px;">${item.symbol}</span>
                            </div>
                            <div class="right-top">
                                <span class="side-badge ${isLong ? 'side-long' : 'side-short'}">
                                    ${isLong ? '▲ LONG' : '▼ SHORT'}
                                </span>
                                <button class="ban-mini-btn" onclick="banCoin('${item.symbol}', event)" title="Убрать">✕</button>
                            </div>
                        </div>
                    `;
                }).join('');
            }

            const dList = document.getElementById('live-densities-list');
            if (!activeDensities.length) {
                dList.innerHTML = '<div class="empty">Крупных плотностей нет</div>';
            } else {
                dList.innerHTML = activeDensities.slice(0, 12).map(item => {
                    const isLong = isLongSide(item.side);
                    const isSpot = item.market_type === 'SPOT';
                    return `
                        <div class="card card-density" style="margin-bottom:6px; padding:9px 10px;" onclick="openInTiger('${item.exchange}', '${item.market_type}', '${item.symbol}', false)">
                            <div>
                                <div class="sym-row" style="margin-bottom:2px;">
                                    ${getExchangeLogoHtml(item.exchange)}
                                    <span class="symbol" style="font-size:14px;">${item.symbol}</span>
                                    <span class="ex-tag ${isSpot ? 'mkt-spot' : 'mkt-fut'}">${isSpot ? 'SPOT' : 'FUT'}</span>
                                </div>
                                <div class="meta" style="font-size:11px;">${item.price} (${item.distance_pct}%)</div>
                            </div>
                            <div class="right-col">
                                <div class="right-top">
                                    <span class="size-big" style="font-size:13px;">${formatVol(item.size_usd)}</span>
                                    <button class="ban-mini-btn" onclick="banCoin('${item.symbol}', event)" title="Убрать">✕</button>
                                </div>
                                <span class="side-badge ${isLong ? 'side-long' : 'side-short'}" style="padding:2px 6px; font-size:10px;">
                                    ${isLong ? '▲ LONG' : '▼ SHORT'}
                                </span>
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
                    const newUpticks = history
                        .filter(i => (Number(i.id) || 0) > lastSeenUptickId)
                        .sort((a, b) => (Number(a.id) || 0) - (Number(b.id) || 0));
                    if (newUpticks.length) {
                        lastSeenUptickId = Math.max(...newUpticks.map(i => Number(i.id) || 0));
                        newUpticks.forEach(item => {
                            const isLong = isLongSide(item.side);
                            speakUptick(item.exchange, item.symbol, isLong);
                            showToast(`🚨 АПТИК ${isLong ? 'LONG' : 'SHORT'}: ${item.symbol} (${item.exchange})`);
                            if (tigerAutoUptick) {
                                openInTiger(item.exchange, 'FUTURES', item.symbol, true);
                            }
                        });
                    }

                    const newDensities = densities
                        .filter(i => (Number(i.id) || 0) > lastSeenDensityId)
                        .sort((a, b) => (Number(a.id) || 0) - (Number(b.id) || 0));
                    if (newDensities.length) {
                        lastSeenDensityId = Math.max(...newDensities.map(i => Number(i.id) || 0));
                        newDensities.slice(0, 2).forEach(item => {
                            const isLong = isLongSide(item.side);
                            speakDensity(item.exchange, item.market_type, item.symbol, isLong);
                            showToast(`🧱 ПЛОТНОСТЬ ${isLong ? 'LONG' : 'SHORT'}: ${item.symbol} ${formatVol(item.size_usd)} (${item.exchange})`);
                            if (tigerAutoDensity) {
                                openInTiger(item.exchange, item.market_type, item.symbol, true);
                            }
                        });
                    }
                }

                cachedHistory = history;
                cachedDensities = densities;
                renderLive(data.active || [], data.active_densities || []);
                renderLiveFeed();
                renderHistory();
                renderDensities();

                if (!settingsInitialized) {
                    if (data.user_min_volume) document.getElementById('min-vol-input').value = Math.round(data.user_min_volume);
                    if (data.user_min_density_spot) document.getElementById('min-dspot-input').value = Math.round(data.user_min_density_spot);
                    if (data.user_min_density_futures) document.getElementById('min-dfut-input').value = Math.round(data.user_min_density_futures);
                    userCoinRules = data.user_coin_rules || {};
                    userBannedCoins = data.user_banned_coins || [];
                    renderCoinRules();
                    renderBannedCoins();
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
                    showToast('✅ Общие фильтры сохранены');
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


class CoinRulePayload(BaseModel):
    user_id: int
    symbol: str
    min_spot: Optional[float] = None
    min_futures: Optional[float] = None


class SymbolPayload(BaseModel):
    user_id: int
    symbol: str


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
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
    user_coin_rules: Dict[str, Dict[str, float]] = {}
    user_banned_list: List[str] = []

    try:
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
                user_banned_list = parse_banned_coins(row.get("banned_coins"))

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
                "SELECT * FROM densities WHERE utc_date = ? ORDER BY id DESC LIMIT 250",
                (today_utc,),
            )
            _api_cache["utc_date"] = today_utc
            _api_cache["ts"] = now_ts
    except Exception as exc:
        logger.error(f"Ошибка чтения /api/data: {exc}")

    banned_set = set(user_banned_list)

    history_rows = [
        h
        for h in _api_cache["history"]
        if not is_blacklisted_symbol(str(h.get("symbol") or ""))
        and extract_base_asset(str(h.get("symbol") or "")) not in banned_set
    ]

    density_rows = []
    for d in _api_cache["densities"]:
        sym = str(d.get("symbol") or "")
        if is_blacklisted_symbol(sym) or extract_base_asset(sym) in banned_set:
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
        if not is_blacklisted_symbol(sym) and extract_base_asset(sym) not in banned_set
    ]

    active_densities = []
    for d in density_engine.get_active_densities():
        if is_blacklisted_symbol(d.symbol) or extract_base_asset(d.symbol) in banned_set:
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
            "user_banned_coins": user_banned_list,
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


@app.post("/api/coin-rule")
async def api_set_coin_rule(payload: CoinRulePayload) -> JSONResponse:
    base = extract_base_asset(payload.symbol)
    if not base or is_blacklisted_symbol(base):
        return JSONResponse(
            {"status": "error", "detail": "Эта монета в черном списке или указана неверно"},
            status_code=400,
        )

    rule: Dict[str, float] = {}
    if payload.min_spot is not None and payload.min_spot > 0:
        rule["spot"] = max(10_000.0, float(payload.min_spot))
    if payload.min_futures is not None and payload.min_futures > 0:
        rule["futures"] = max(25_000.0, float(payload.min_futures))
    if not rule:
        return JSONResponse(
            {"status": "error", "detail": "Укажите порог для Спота и/или Фьючерса"},
            status_code=400,
        )

    row = await db_fetchone("SELECT coin_rules FROM users WHERE user_id = ?", (payload.user_id,))
    rules = parse_coin_rules(row.get("coin_rules") if row else None)
    rules[base] = rule

    await db_execute(
        "INSERT INTO users (user_id, coin_rules) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET coin_rules = excluded.coin_rules",
        (payload.user_id, json.dumps(rules)),
    )
    await sync_density_engine_custom_floors()
    return JSONResponse({"status": "ok", "base": base, "coin_rules": rules})


@app.post("/api/coin-rule/delete")
async def api_delete_coin_rule(payload: SymbolPayload) -> JSONResponse:
    base = extract_base_asset(payload.symbol)
    row = await db_fetchone("SELECT coin_rules FROM users WHERE user_id = ?", (payload.user_id,))
    rules = parse_coin_rules(row.get("coin_rules") if row else None)
    rules.pop(base, None)

    await db_execute(
        "INSERT INTO users (user_id, coin_rules) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET coin_rules = excluded.coin_rules",
        (payload.user_id, json.dumps(rules)),
    )
    await sync_density_engine_custom_floors()
    return JSONResponse({"status": "ok", "coin_rules": rules})


@app.post("/api/ban-coin")
async def api_ban_coin(payload: SymbolPayload) -> JSONResponse:
    base = extract_base_asset(payload.symbol)
    if not base:
        return JSONResponse({"status": "error", "detail": "Некорректный тикер"}, status_code=400)

    row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (payload.user_id,))
    banned = set(parse_banned_coins(row.get("banned_coins") if row else None))
    banned.add(base)
    banned_sorted = sorted(banned)

    await db_execute(
        "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
        (payload.user_id, json.dumps(banned_sorted)),
    )
    return JSONResponse({"status": "ok", "base": base, "banned_coins": banned_sorted})


@app.post("/api/unban-coin")
async def api_unban_coin(payload: SymbolPayload) -> JSONResponse:
    base = extract_base_asset(payload.symbol)
    row = await db_fetchone("SELECT banned_coins FROM users WHERE user_id = ?", (payload.user_id,))
    banned = set(parse_banned_coins(row.get("banned_coins") if row else None))
    banned.discard(base)
    banned_sorted = sorted(banned)

    await db_execute(
        "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
        (payload.user_id, json.dumps(banned_sorted)),
    )
    return JSONResponse({"status": "ok", "banned_coins": banned_sorted})


@app.get("/webapp", response_class=HTMLResponse)
async def webapp_page() -> HTMLResponse:
    return HTMLResponse(content=WEBAPP_HTML)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
