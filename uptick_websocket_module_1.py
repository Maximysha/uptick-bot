"""
Независимый модуль WebSocket-детектора «Аптиков» (Binance, Bybit, OKX, Aster).
Включает проверку реальной плотности в стакане на планке.
Акции, ETF и металлы РАЗРЕШЕНЫ; блокируются только тяжеловесы крипто-рынка и стейблкоины.
"""

import asyncio
import json
import logging
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
UPTICK_TOLERANCE_PCT = 0.20       # Допуск близости цены к планке (%)
UPTICK_COOLDOWN_SEC = 180.0       # 3 минуты между повторами аптика по монете
UPTICK_ACTIVE_TTL_SEC = 15.0      # Сколько секунд аптик считается активным в Лайве
MIN_UPTICK_WALL_USD = 5_000.0     # Мин. реальная плотность в стакане на планке ($)
ALL_UPTICK_EXCHANGES = ["Binance", "Bybit", "OKX", "Aster"]

# Черный список ДЛЯ АПТИКОВ: ТОЛЬКО тяжеловесы, обернутые токены и стейблкоины.
# Акции, ETF и металлы (XAU, XAG, TSLA, NVDA и т.д.) НЕ блокируются и участвуют в аптиках.
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
    # Стейблкоины, фиат и технические тикеры
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDE", "USDS", "PYUSD",
    "EURC", "EURT", "EURI", "AEUR", "USD1", "RLUSD", "USDD", "USDP", "GUSD",
    "FRAX", "LUSD", "CRVUSD", "XUSD", "USDF", "USTC", "USDG", "USDY", "SUSDE",
    "EUR", "GBP", "TRY", "JPY", "AUD", "CHF", "CAD", "USD", "U",
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


def create_uptick_tasks(session: aiohttp.ClientSession, uptick_eng: UptickEngine) -> List[asyncio.Task[Any]]:
    """Запускает 4 независимых WebSocket-сканера Аптиков."""
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