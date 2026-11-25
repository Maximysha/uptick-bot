"""
Мультибиржевой детектор «Аптиков» (планок / границ ценового коридора) + Telegram Bot + Mini App.
Биржи (6): Binance, Bybit, OKX, Bitget, Gate, KuCoin.

Возможности:
- Асинхронный мониторинг 6 криптобирж с расчетом официальных ценовых коридоров.
- Фильтрация по суточному обороту (от $100,000, настраивается индивидуально для каждого пользователя).
- Защита от спама через гистерезис (машина состояний) и кулдаун.
- Мгновенные уведомления в Telegram-бот.
- Встроенное Telegram Mini App с вкладками («Сегодня», «Сейчас на планке», «Настройки»)
  и модульной структурой для быстрого добавления новых функций.
- Автоматический сброс истории «Аптик сегодня» в 00:00 UTC.

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
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

import aiohttp
import aiosqlite
import uvicorn
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("UptickDetector")


class UptickSide(str, Enum):
    """Сторона планки (в какую сторону заблокировано исполнение рыночных заявок)."""
    LONG = "LONG (Верхняя планка)"
    SHORT = "SHORT (Нижняя планка)"


@dataclass
class UptickEvent:
    """Структура события зафиксированного аптика."""
    exchange: str
    symbol: str
    side: UptickSide
    price: float
    mark_price: float
    limit_price: float
    volume_24h_usd: float
    distance_pct: float  # Расстояние до планки в процентах (0.0% = ровно на планке)
    timestamp_utc: str
    utc_date: str


@dataclass
class SymbolState:
    """Состояние монеты для алгоритма гистерезиса (защита от дребезга/спама)."""
    in_uptick: bool = False
    side: Optional[UptickSide] = None
    last_trigger_ts: float = 0.0


class UptickEngine:
    """
    Ядро алгоритма детекции аптиков.
    Отвечает за:
    - проверку условий входа/выхода из планки (гистерезис);
    - фильтрацию по обороту ($100,000+);
    - хранение истории «Аптик сегодня (UTC)» и её сброс в 00:00 UTC;
    - вызов колбэка для отправки уведомлений (например, в Telegram).
    """

    def __init__(
        self,
        min_volume_usd: float = 100_000.0,
        trigger_tolerance: float = 0.0025,   # 0.25% до границы планки для срабатывания
        release_tolerance: float = 0.0100,   # 1.00% отхода от границы для сброса статуса «Аптик»
        min_cooldown_sec: float = 180.0,     # Минимальный кулдаун (3 мин) между повторными сигналами
        on_uptick: Optional[Callable[[UptickEvent], Awaitable[None]]] = None,
    ):
        self.min_volume_usd = min_volume_usd
        self.trigger_tolerance = trigger_tolerance
        self.release_tolerance = release_tolerance
        self.min_cooldown_sec = min_cooldown_sec
        self.on_uptick = on_uptick

        # Ключ: (exchange, symbol) -> SymbolState
        self.states: Dict[Tuple[str, str], SymbolState] = {}

        # История «Аптик сегодня (UTC)»
        self.current_utc_date: str = self._get_utc_date()
        self.today_history: List[UptickEvent] = []

    @staticmethod
    def _get_utc_date() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    @staticmethod
    def _get_utc_time() -> str:
        return datetime.now(timezone.utc).strftime("%H:%M:%S")

    def _check_utc_day_rollover(self) -> None:
        """Сбрасывает историю «Аптик сегодня» при наступлении новых суток (00:00 UTC)."""
        today = self._get_utc_date()
        if today != self.current_utc_date:
            logger.info(f"Наступили новые сутки UTC ({today}). Очистка истории «Аптик сегодня».")
            self.current_utc_date = today
            self.today_history.clear()

    def get_active_upticks(self) -> List[Tuple[str, str, UptickSide]]:
        """Возвращает список монет, на которых прямо сейчас активна плашка «Аптик»."""
        return [
            (ex, sym, st.side)
            for (ex, sym), st in self.states.items()
            if st.in_uptick and st.side is not None
        ]

    async def evaluate(
        self,
        exchange: str,
        symbol: str,
        price: float,
        mark_price: float,
        up_limit: float,
        down_limit: float,
        volume_24h_usd: float,
        best_ask: Optional[float] = None,
        best_bid: Optional[float] = None,
    ) -> None:
        """
        Основная математика проверки одной монеты на касание ценового коридора.

        Мы проверяем не только Last Price, но и лучшие цены в стакане (Best Ask / Best Bid),
        так как именно они определяют, может ли исполниться рыночный ордер.
        """
        self._check_utc_day_rollover()

        # 1. Валидация входных данных и фильтр по суточному обороту
        if (
            volume_24h_usd < self.min_volume_usd
            or price <= 0
            or mark_price <= 0
            or up_limit <= 0
            or down_limit <= 0
        ):
            return

        # Для верхней планки берем максимум из Last Price и Best Ask (если доступен)
        check_up_price = max(price, best_ask) if (best_ask and best_ask > 0) else price
        # Для нижней планки берем минимум из Last Price и Best Bid (если доступен)
        check_down_price = min(price, best_bid) if (best_bid and best_bid > 0) else price

        # Пороги входа в аптик
        up_trigger = up_limit * (1.0 - self.trigger_tolerance)
        down_trigger = down_limit * (1.0 + self.trigger_tolerance)

        # Пороги выхода из аптика (гистерезис)
        up_release = up_limit * (1.0 - self.release_tolerance)
        down_release = down_limit * (1.0 + self.release_tolerance)

        key = (exchange, symbol)
        state = self.states.setdefault(key, SymbolState())
        now_ts = time.time()

        # 2. Проверяем, вышла ли монета из предыдущего аптика
        if state.in_uptick:
            if state.side == UptickSide.LONG and check_up_price < up_release:
                state.in_uptick = False
                state.side = None
                logger.debug(f"[{exchange}] {symbol}: снятие верхней планки (цена {check_up_price})")
            elif state.side == UptickSide.SHORT and check_down_price > down_release:
                state.in_uptick = False
                state.side = None
                logger.debug(f"[{exchange}] {symbol}: снятие нижней планки (цена {check_down_price})")
            else:
                # Монета все еще на планке — не дублируем сигнал
                return

        # 3. Проверяем вход в новый аптик
        detected_side: Optional[UptickSide] = None
        limit_hit: float = 0.0
        dist_pct: float = 0.0

        if check_up_price >= up_trigger:
            detected_side = UptickSide.LONG
            limit_hit = up_limit
            dist_pct = max(0.0, (up_limit - check_up_price) / up_limit * 100.0)
        elif check_down_price <= down_trigger:
            detected_side = UptickSide.SHORT
            limit_hit = down_limit
            dist_pct = max(0.0, (check_down_price - down_limit) / down_limit * 100.0)

        if detected_side is not None:
            # Дополнительная защита кулдауном на случай резких прострелов туда-обратно
            if now_ts - state.last_trigger_ts < self.min_cooldown_sec:
                return

            state.in_uptick = True
            state.side = detected_side
            state.last_trigger_ts = now_ts

            event = UptickEvent(
                exchange=exchange,
                symbol=symbol,
                side=detected_side,
                price=price,
                mark_price=mark_price,
                limit_price=limit_hit,
                volume_24h_usd=volume_24h_usd,
                distance_pct=round(dist_pct, 3),
                timestamp_utc=self._get_utc_time(),
                utc_date=self.current_utc_date,
            )

            self.today_history.insert(0, event)
            # Ограничиваем память последними 1000 событиями за сутки
            if len(self.today_history) > 1000:
                self.today_history.pop()

            if self.on_uptick:
                try:
                    await self.on_uptick(event)
                except Exception as exc:
                    logger.error(f"Ошибка в обработчике уведомления on_uptick: {exc}")


# ==============================================================================
# АДАПТЕРЫ БИРЖ (Binance, Bybit, OKX, Bitget, Gate, KuCoin)
# ==============================================================================

class BaseExchangeScanner:
    """Базовый класс для асинхронного сканера биржи."""

    name: str = "Base"
    poll_interval: float = 3.0       # Интервал опроса тикеров (сек)
    meta_refresh_sec: float = 1800.0  # Обновление правил/лимитов контрактов раз в 30 минут

    def __init__(self, engine: UptickEngine, session: aiohttp.ClientSession):
        self.engine = engine
        self.session = session
        self.limits_meta: Dict[str, Tuple[float, float]] = {}  # symbol -> (up_mult, down_mult)
        self._last_meta_ts: float = 0.0

    async def _get_json(self, url: str, params: Optional[dict] = None) -> dict:
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} для {url}")
            return await resp.json()

    async def refresh_metadata(self) -> None:
        """Переопределяется в наследниках: загрузка коэффициентов ценового коридора."""
        pass

    async def scan_once(self) -> None:
        """Переопределяется в наследниках: один цикл проверки всех монет биржи."""
        raise NotImplementedError

    async def run_forever(self) -> None:
        """Бесконечный цикл работы сканера с автовосстановлением при сетевых сбоях."""
        logger.info(f"[{self.name}] Запуск сканера...")
        while True:
            try:
                if time.time() - self._last_meta_ts > self.meta_refresh_sec or not self.limits_meta:
                    await self.refresh_metadata()
                    self._last_meta_ts = time.time()

                await self.scan_once()
            except asyncio.CancelledError:
                logger.info(f"[{self.name}] Сканер остановлен.")
                break
            except Exception as exc:
                logger.warning(f"[{self.name}] Ошибка цикла: {exc}")
                await asyncio.sleep(5.0)

            await asyncio.sleep(self.poll_interval)


class BinanceFuturesScanner(BaseExchangeScanner):
    """
    Binance USD-M Futures.
    Коридор задается фильтром PERCENT_PRICE (multiplierUp / multiplierDown от Mark Price).
    """
    name = "Binance"
    poll_interval = 2.5

    async def refresh_metadata(self) -> None:
        data = await self._get_json("https://fapi.binance.com/fapi/v1/exchangeInfo")
        new_meta: Dict[str, Tuple[float, float]] = {}
        for s in data.get("symbols", []):
            if s.get("status") != "TRADING":
                continue
            for f in s.get("filters", []):
                if f.get("filterType") == "PERCENT_PRICE":
                    up = float(f.get("multiplierUp", 1.05))
                    down = float(f.get("multiplierDown", 0.95))
                    new_meta[s["symbol"]] = (up, down)
                    break
        self.limits_meta = new_meta
        logger.info(f"[{self.name}] Загружены лимиты для {len(self.limits_meta)} контрактов.")

    async def scan_once(self) -> None:
        tickers, premiums, books = await asyncio.gather(
            self._get_json("https://fapi.binance.com/fapi/v1/ticker/24hr"),
            self._get_json("https://fapi.binance.com/fapi/v1/premiumIndex"),
            self._get_json("https://fapi.binance.com/fapi/v1/ticker/bookTicker"),
        )
        marks = {p["symbol"]: float(p["markPrice"]) for p in premiums if p.get("markPrice")}
        book_map = {
            b["symbol"]: (float(b["bidPrice"]), float(b["askPrice"]))
            for b in books if b.get("bidPrice") and b.get("askPrice")
        }

        for t in tickers:
            sym = t["symbol"]
            if sym not in self.limits_meta or sym not in marks:
                continue
            up_mult, down_mult = self.limits_meta[sym]
            mark = marks[sym]
            last = float(t["lastPrice"])
            vol = float(t["quoteVolume"])
            bid, ask = book_map.get(sym, (None, None))

            await self.engine.evaluate(
                exchange=self.name,
                symbol=sym,
                price=last,
                mark_price=mark,
                up_limit=mark * up_mult,
                down_limit=mark * down_mult,
                volume_24h_usd=vol,
                best_bid=bid,
                best_ask=ask,
            )


class BybitFuturesScanner(BaseExchangeScanner):
    """
    Bybit Linear Futures (USDT Perpetual).
    Использует коэффициенты ценового лимита из instruments-info относительно Mark Price.
    """
    name = "Bybit"
    poll_interval = 3.0

    async def refresh_metadata(self) -> None:
        data = await self._get_json(
            "https://api.bybit.com/v5/market/instruments-info",
            params={"category": "linear", "limit": 1000},
        )
        new_meta: Dict[str, Tuple[float, float]] = {}
        for item in data.get("result", {}).get("list", []):
            if item.get("status") != "Trading":
                continue
            sym = item["symbol"]
            ratio_x = float(item.get("priceLimitRatioX") or 0.05)
            ratio_y = float(item.get("priceLimitRatioY") or ratio_x)
            new_meta[sym] = (1.0 + ratio_x, 1.0 - ratio_y)
        self.limits_meta = new_meta
        logger.info(f"[{self.name}] Загружены лимиты для {len(self.limits_meta)} контрактов.")

    async def scan_once(self) -> None:
        data = await self._get_json(
            "https://api.bybit.com/v5/market/tickers",
            params={"category": "linear"},
        )
        for t in data.get("result", {}).get("list", []):
            sym = t.get("symbol")
            if sym not in self.limits_meta:
                continue
            try:
                last = float(t.get("lastPrice") or 0)
                mark = float(t.get("markPrice") or 0)
                vol = float(t.get("turnover24h") or 0)
                bid = float(t.get("bid1Price") or 0) or None
                ask = float(t.get("ask1Price") or 0) or None
            except ValueError:
                continue

            up_mult, down_mult = self.limits_meta[sym]
            await self.engine.evaluate(
                exchange=self.name,
                symbol=sym,
                price=last,
                mark_price=mark,
                up_limit=mark * up_mult,
                down_limit=mark * down_mult,
                volume_24h_usd=vol,
                best_bid=bid,
                best_ask=ask,
            )


class OKXSwapScanner(BaseExchangeScanner):
    """
    OKX Perpetual Swap.
    Двухэтапный точный детектор:
    1) Отбирает ликвидные пары, где отклонение Last от Mark Price >= 1.5%.
    2) Для кандидатов запрашивает /api/v5/public/price-limit (точные buyLmt и sellLmt).
    """
    name = "OKX"
    poll_interval = 3.5

    async def refresh_metadata(self) -> None:
        data = await self._get_json(
            "https://www.okx.com/api/v5/public/instruments",
            params={"instType": "SWAP"},
        )
        new_meta: Dict[str, Tuple[float, float]] = {}
        for item in data.get("data", []):
            if item.get("state") == "live" and item.get("settleCcy") == "USDT":
                ct_val = float(item.get("ctVal") or 1.0)
                new_meta[item["instId"]] = (ct_val, 1.0)
        self.limits_meta = new_meta
        logger.info(f"[{self.name}] Загружены спецификации для {len(self.limits_meta)} контрактов.")

    async def scan_once(self) -> None:
        tickers_resp, marks_resp = await asyncio.gather(
            self._get_json("https://www.okx.com/api/v5/market/tickers", params={"instType": "SWAP"}),
            self._get_json("https://www.okx.com/api/v5/public/mark-price", params={"instType": "SWAP"}),
        )
        marks = {m["instId"]: float(m["markPx"]) for m in marks_resp.get("data", []) if m.get("markPx")}

        candidates = []
        for t in tickers_resp.get("data", []):
            inst_id = t.get("instId")
            if inst_id not in self.limits_meta or inst_id not in marks:
                continue
            last = float(t.get("last") or 0)
            mark = marks[inst_id]
            if last <= 0 or mark <= 0:
                continue

            vol_usd = float(t.get("volCcy24h") or 0) * last
            if vol_usd < self.engine.min_volume_usd:
                continue

            if abs(last - mark) / mark >= 0.015:
                bid = float(t.get("bidPx") or 0) or None
                ask = float(t.get("askPx") or 0) or None
                candidates.append((inst_id, last, mark, vol_usd, bid, ask))

        for inst_id, last, mark, vol_usd, bid, ask in candidates[:15]:
            try:
                pl_resp = await self._get_json(
                    "https://www.okx.com/api/v5/public/price-limit",
                    params={"instId": inst_id},
                )
                pl_data = pl_resp.get("data", [])
                if not pl_data:
                    continue
                buy_lmt = float(pl_data[0].get("buyLmt") or 0)
                sell_lmt = float(pl_data[0].get("sellLmt") or 0)
                if buy_lmt > 0 and sell_lmt > 0:
                    await self.engine.evaluate(
                        exchange=self.name,
                        symbol=inst_id.replace("-USDT-SWAP", "USDT"),
                        price=last,
                        mark_price=mark,
                        up_limit=buy_lmt,
                        down_limit=sell_lmt,
                        volume_24h_usd=vol_usd,
                        best_bid=bid,
                        best_ask=ask,
                    )
            except Exception:
                continue


class BitgetFuturesScanner(BaseExchangeScanner):
    """
    Bitget USDT-M Futures.
    API прямо отдает buyLimitPriceRatio и sellLimitPriceRatio для каждого контракта.
    """
    name = "Bitget"
    poll_interval = 3.0

    async def refresh_metadata(self) -> None:
        data = await self._get_json(
            "https://api.bitget.com/api/v2/mix/market/contracts",
            params={"productType": "USDT-FUTURES"},
        )
        new_meta: Dict[str, Tuple[float, float]] = {}
        for c in data.get("data", []):
            if c.get("symbolStatus") != "normal":
                continue
            buy_ratio = float(c.get("buyLimitPriceRatio") or 0.05)
            sell_ratio = float(c.get("sellLimitPriceRatio") or 0.05)
            new_meta[c["symbol"]] = (1.0 + buy_ratio, 1.0 - sell_ratio)
        self.limits_meta = new_meta
        logger.info(f"[{self.name}] Загружены лимиты для {len(self.limits_meta)} контрактов.")

    async def scan_once(self) -> None:
        data = await self._get_json(
            "https://api.bitget.com/api/v2/mix/market/tickers",
            params={"productType": "USDT-FUTURES"},
        )
        for t in data.get("data", []):
            sym = t.get("symbol")
            if sym not in self.limits_meta:
                continue
            last = float(t.get("lastPr") or 0)
            mark = float(t.get("markPrice") or 0)
            vol = float(t.get("usdtVolume") or 0)
            bid = float(t.get("bidPr") or 0) or None
            ask = float(t.get("askPr") or 0) or None

            up_mult, down_mult = self.limits_meta[sym]
            await self.engine.evaluate(
                exchange=self.name,
                symbol=sym,
                price=last,
                mark_price=mark,
                up_limit=mark * up_mult,
                down_limit=mark * down_mult,
                volume_24h_usd=vol,
                best_bid=bid,
                best_ask=ask,
            )


class GateFuturesScanner(BaseExchangeScanner):
    """
    Gate.io USDT Futures.
    Коридор задается полем order_price_deviate (максимальное отклонение от Mark Price).
    """
    name = "Gate"
    poll_interval = 3.0

    async def refresh_metadata(self) -> None:
        data = await self._get_json("https://api.gateio.ws/api/v4/futures/usdt/contracts")
        new_meta: Dict[str, Tuple[float, float]] = {}
        for c in data:
            if c.get("in_delisting"):
                continue
            dev = float(c.get("order_price_deviate") or 0.1)
            new_meta[c["name"]] = (1.0 + dev, 1.0 - dev)
        self.limits_meta = new_meta
        logger.info(f"[{self.name}] Загружены лимиты для {len(self.limits_meta)} контрактов.")

    async def scan_once(self) -> None:
        data = await self._get_json("https://api.gateio.ws/api/v4/futures/usdt/tickers")
        for t in data:
            contract = t.get("contract")
            if contract not in self.limits_meta:
                continue
            last = float(t.get("last") or 0)
            mark = float(t.get("mark_price") or 0)
            vol = float(t.get("volume_24h_quote") or 0)
            bid = float(t.get("highest_bid") or 0) or None
            ask = float(t.get("lowest_ask") or 0) or None

            up_mult, down_mult = self.limits_meta[contract]
            await self.engine.evaluate(
                exchange=self.name,
                symbol=contract.replace("_", ""),
                price=last,
                mark_price=mark,
                up_limit=mark * up_mult,
                down_limit=mark * down_mult,
                volume_24h_usd=vol,
                best_bid=bid,
                best_ask=ask,
            )


class KuCoinFuturesScanner(BaseExchangeScanner):
    """
    KuCoin Futures.
    Получает активные контракты с Mark Price, Last Price и 24h Turnover.
    """
    name = "KuCoin"
    poll_interval = 4.0

    async def refresh_metadata(self) -> None:
        self.limits_meta = {"_initialized": (1.05, 0.95)}

    async def scan_once(self) -> None:
        data = await self._get_json("https://api-futures.kucoin.com/api/v1/contracts/active")
        for c in data.get("data", []):
            if c.get("status") != "Open" or c.get("quoteCurrency") != "USDT":
                continue
            sym = c.get("symbol", "")
            last = float(c.get("lastTradePrice") or 0)
            mark = float(c.get("markPrice") or 0)
            vol = float(c.get("turnoverOf24h") or 0)
            if last <= 0 or mark <= 0:
                continue

            up_limit = mark * 1.05
            down_limit = mark * 0.95

            await self.engine.evaluate(
                exchange=self.name,
                symbol=sym.replace("M", "") if sym.endswith("USDTM") else sym,
                price=last,
                mark_price=mark,
                up_limit=up_limit,
                down_limit=down_limit,
                volume_24h_usd=vol,
            )


# ==============================================================================
# КОНФИГУРАЦИЯ, БАЗА ДАННЫХ (SQLite) И TELEGRAM-БОТ
# ==============================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()
DB_PATH = os.getenv("DB_PATH", "upticks.db")
DEFAULT_MIN_VOLUME = 100_000.0

bot: Optional[Bot] = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()


async def init_db() -> None:
    """Создает таблицы пользователей и истории аптиков в SQLite."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                min_volume REAL DEFAULT 100000
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        await db.commit()


async def handle_uptick_alert(event: UptickEvent) -> None:
    """
    Обработчик нового аптика:
    1) Логирует событие.
    2) Сохраняет его в SQLite (для отображения в Mini App).
    3) Рассылает уведомления всем подписчикам бота с учетом их фильтра по обороту.
    """
    logger.info(
        f"🚨 АПТИК | {event.exchange} | {event.symbol} | {event.side.value} | "
        f"Цена: {event.price} | Оборот: ${event.volume_24h_usd:,.0f}"
    )

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
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
            await db.commit()

            async with db.execute("SELECT user_id, min_volume FROM users") as cursor:
                users = await cursor.fetchall()
    except Exception as exc:
        logger.error(f"Ошибка записи в БД при обработке аптика: {exc}")
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

    reply_markup = None
    if WEBAPP_URL:
        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📱 Открыть приложение", web_app=WebAppInfo(url=WEBAPP_URL))]
            ]
        )

    for user_id, min_vol in users:
        if event.volume_24h_usd >= float(min_vol):
            try:
                await bot.send_message(
                    user_id,
                    text,
                    parse_mode="HTML",
                    reply_markup=reply_markup,
                )
            except Exception as exc:
                logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {exc}")


# Глобальный экземпляр ядра детекции
engine = UptickEngine(
    min_volume_usd=DEFAULT_MIN_VOLUME,
    trigger_tolerance=0.0025,
    release_tolerance=0.0100,
    min_cooldown_sec=180.0,
    on_uptick=handle_uptick_alert,
)


# === КОМАНДЫ TELEGRAM-БОТА ===

def _build_main_keyboard() -> Optional[InlineKeyboardMarkup]:
    """Создает кнопку для открытия Telegram Mini App."""
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Открыть Аптик-скринер", web_app=WebAppInfo(url=WEBAPP_URL))]
        ]
    )


@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    """Регистрирует пользователя и отправляет кнопку входа в Mini App."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id, min_volume) VALUES (?, ?)",
            (message.from_user.id, DEFAULT_MIN_VOLUME),
        )
        await db.commit()

    kb = _build_main_keyboard()
    hint = (
        "Нажмите кнопку ниже, чтобы открыть приложение."
        if kb
        else "⚠️ Переменная WEBAPP_URL еще не задана — кнопка приложения появится после настройки."
    )
    await message.answer(
        "👋 <b>Детектор аптиков запущен!</b>\n\n"
        "Отслеживаемые биржи (6): Binance, Bybit, OKX, Bitget, Gate, KuCoin.\n"
        "Вы подписаны на мгновенные уведомления.\n\n"
        "Команды:\n"
        "• <code>/vol 500000</code> — задать мин. оборот 24ч ($)\n"
        "• <code>/status</code> — активные планки прямо сейчас\n\n"
        f"{hint}",
        parse_mode="HTML",
        reply_markup=kb,
    )


@dp.message(Command("vol"))
async def cmd_vol(message: types.Message) -> None:
    """Изменяет минимальный суточный оборот для уведомлений пользователя."""
    try:
        parts = (message.text or "").split()
        if len(parts) < 2:
            raise ValueError("Не указано значение")
        new_vol = max(100_000.0, float(parts[1].replace(",", "").replace("$", "")))
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO users (user_id, min_volume) VALUES (?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET min_volume = excluded.min_volume",
                (message.from_user.id, new_vol),
            )
            await db.commit()
        await message.answer(f"✅ Минимальный оборот для уведомлений установлен: <b>${new_vol:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/vol 250000</code> (минимум $100,000)", parse_mode="HTML")


@dp.message(Command("status"))
async def cmd_status(message: types.Message) -> None:
    """Показывает список монет, находящихся на планке прямо сейчас."""
    active = engine.get_active_upticks()
    if not active:
        await message.answer("Сейчас нет монет, упершихся в планку.", reply_markup=_build_main_keyboard())
        return

    lines = ["⚡ <b>Активные аптики прямо сейчас:</b>\n"]
    for ex, sym, side in active:
        emoji = "🟢" if side == UptickSide.LONG else "🔴"
        lines.append(f"{emoji} <b>{sym}</b> ({ex}) — {side.value}")
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=_build_main_keyboard())


# ==============================================================================
# FASTAPI + TELEGRAM MINI APP (ВСТРОЕННЫЙ ИНТЕРФЕЙС)
# ==============================================================================

WEBAPP_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Аптик Скринер</title>
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
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 14px;
        }
        .title { font-size: 19px; font-weight: 700; display: flex; align-items: center; gap: 8px; }
        .live-dot {
            width: 8px; height: 8px; border-radius: 50%; background: var(--long);
            box-shadow: 0 0 8px var(--long); display: inline-block;
        }
        .utc-badge {
            font-size: 12px; color: var(--hint); background: var(--card);
            padding: 5px 9px; border-radius: 8px; border: 1px solid var(--border);
        }
        .tabs {
            display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px;
            background: var(--card); padding: 4px; border-radius: 12px; margin-bottom: 14px;
        }
        .tab-btn {
            border: none; background: transparent; color: var(--hint);
            padding: 9px 6px; border-radius: 9px; font-size: 13px; font-weight: 600; cursor: pointer;
            transition: all 0.2s;
        }
        .tab-btn.active { background: var(--accent); color: var(--accent-text); }
        .filters {
            display: flex; gap: 6px; overflow-x: auto; padding-bottom: 8px; margin-bottom: 8px;
            scrollbar-width: none;
        }
        .filters::-webkit-scrollbar { display: none; }
        .chip {
            border: 1px solid var(--border); background: var(--card); color: var(--hint);
            padding: 6px 11px; border-radius: 20px; font-size: 12px; font-weight: 600;
            white-space: nowrap; cursor: pointer;
        }
        .chip.active { background: var(--accent); color: var(--accent-text); border-color: transparent; }
        .card {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 14px; padding: 13px; margin-bottom: 10px;
            display: flex; justify-content: space-between; align-items: center; gap: 10px;
        }
        .sym-row { display: flex; align-items: center; gap: 7px; margin-bottom: 4px; }
        .symbol { font-size: 16px; font-weight: 700; }
        .ex-tag {
            font-size: 11px; font-weight: 600; padding: 2px 7px; border-radius: 6px;
            background: rgba(255, 255, 255, 0.08); color: var(--text);
        }
        .meta { font-size: 12px; color: var(--hint); line-height: 1.45; }
        .right-col { text-align: right; flex-shrink: 0; }
        .side-badge {
            display: inline-block; font-size: 11px; font-weight: 700;
            padding: 4px 8px; border-radius: 7px; margin-bottom: 5px;
        }
        .side-long { background: rgba(22, 199, 132, 0.16); color: var(--long); }
        .side-short { background: rgba(234, 57, 67, 0.16); color: var(--short); }
        .time-str { font-size: 12px; color: var(--hint); font-variant-numeric: tabular-nums; }
        .empty {
            text-align: center; padding: 36px 16px; color: var(--hint);
            background: var(--card); border-radius: 14px; font-size: 14px;
        }
        .panel { display: none; }
        .panel.active { display: block; }
        .setting-box {
            background: var(--card); border-radius: 14px; padding: 16px; border: 1px solid var(--border);
        }
        .setting-label { font-size: 14px; font-weight: 600; margin-bottom: 8px; display: block; }
        .input-row { display: flex; gap: 8px; margin-bottom: 12px; }
        input[type="number"] {
            flex: 1; padding: 11px 12px; border-radius: 10px; border: 1px solid var(--border);
            background: var(--bg); color: var(--text); font-size: 15px; outline: none;
        }
        .save-btn {
            background: var(--accent); color: var(--accent-text); border: none;
            padding: 0 16px; border-radius: 10px; font-weight: 600; font-size: 14px; cursor: pointer;
        }
        .presets { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; }
        .preset-btn {
            background: var(--bg); color: var(--text); border: 1px solid var(--border);
            padding: 8px 4px; border-radius: 8px; font-size: 12px; font-weight: 600; cursor: pointer;
        }
        .toast {
            position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%);
            background: #222733; color: #fff; padding: 10px 16px; border-radius: 10px;
            font-size: 13px; font-weight: 600; opacity: 0; pointer-events: none;
            transition: opacity 0.25s; box-shadow: 0 6px 20px rgba(0,0,0,0.4);
        }
        .toast.show { opacity: 1; }
    </style>
</head>
<body>
    <div class="header">
        <div class="title"><span class="live-dot"></span> Аптик Скринер</div>
        <div class="utc-badge" id="utc-clock">UTC --:--:--</div>
    </div>

    <div class="tabs">
        <button class="tab-btn active" onclick="switchTab('today', this)">Сегодня</button>
        <button class="tab-btn" onclick="switchTab('live', this)">Сейчас (<span id="live-count">0</span>)</button>
        <button class="tab-btn" onclick="switchTab('settings', this)">Настройки</button>
    </div>

    <div id="panel-today" class="panel active">
        <div class="filters" id="exchange-filters">
            <button class="chip active" onclick="setExchange('ALL', this)">Все (6)</button>
            <button class="chip" onclick="setExchange('Binance', this)">Binance</button>
            <button class="chip" onclick="setExchange('Bybit', this)">Bybit</button>
            <button class="chip" onclick="setExchange('OKX', this)">OKX</button>
            <button class="chip" onclick="setExchange('Bitget', this)">Bitget</button>
            <button class="chip" onclick="setExchange('Gate', this)">Gate</button>
            <button class="chip" onclick="setExchange('KuCoin', this)">KuCoin</button>
        </div>
        <div id="history-list"><div class="empty">Загрузка данных...</div></div>
    </div>

    <div id="panel-live" class="panel">
        <div id="live-list"><div class="empty">Сейчас активных планок нет.</div></div>
    </div>

    <div id="panel-settings" class="panel">
        <div class="setting-box">
            <label class="setting-label">Минимальный оборот 24ч ($) для уведомлений</label>
            <div class="input-row">
                <input type="number" id="min-vol-input" min="100000" step="50000" placeholder="100000">
                <button class="save-btn" onclick="saveVolume()">Сохранить</button>
            </div>
            <div class="presets">
                <button class="preset-btn" onclick="applyPreset(100000)">$100K</button>
                <button class="preset-btn" onclick="applyPreset(500000)">$500K</button>
                <button class="preset-btn" onclick="applyPreset(1000000)">$1M</button>
                <button class="preset-btn" onclick="applyPreset(5000000)">$5M</button>
            </div>
            <p class="meta" style="margin-top:12px;">
                Биржи: Binance, Bybit, OKX, Bitget, Gate, KuCoin.<br>
                История автоматически сбрасывается в 00:00 UTC.
            </p>
        </div>
    </div>

    <div class="toast" id="toast">Сохранено</div>

    <script>
        const tg = window.Telegram.WebApp;
        tg.expand();

        const userId = tg.initDataUnsafe?.user?.id || 0;
        let selectedExchange = 'ALL';
        let cachedHistory = [];
        let volInitialized = false;

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
            setTimeout(() => el.classList.remove('show'), 2200);
        }

        function switchTab(name, btn) {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
            btn.classList.add('active');
            document.getElementById('panel-' + name).classList.add('active');
        }

        function setExchange(ex, btn) {
            selectedExchange = ex;
            document.querySelectorAll('#exchange-filters .chip').forEach(c => c.classList.remove('active'));
            btn.classList.add('active');
            renderHistory();
        }

        function formatVol(v) {
            if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
            if (v >= 1e3) return '$' + Math.round(v / 1e3) + 'K';
            return '$' + Math.round(v);
        }

        function renderHistory() {
            const list = document.getElementById('history-list');
            const filtered = selectedExchange === 'ALL'
                ? cachedHistory
                : cachedHistory.filter(i => i.exchange === selectedExchange);

            if (!filtered.length) {
                list.innerHTML = '<div class="empty">Сегодня касаний планки по выбранному фильтру еще не было.</div>';
                return;
            }

            list.innerHTML = filtered.map(item => {
                const isLong = item.side.includes('LONG');
                return `
                    <div class="card">
                        <div>
                            <div class="sym-row">
                                <span class="symbol">${item.symbol}</span>
                                <span class="ex-tag">${item.exchange}</span>
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

        function renderLive(active) {
            document.getElementById('live-count').textContent = active.length;
            const list = document.getElementById('live-list');
            if (!active.length) {
                list.innerHTML = '<div class="empty">Прямо сейчас ни одна монета не держит планку.</div>';
                return;
            }
            list.innerHTML = active.map(item => {
                const isLong = item.side.includes('LONG');
                return `
                    <div class="card">
                        <div class="sym-row" style="margin:0;">
                            <span class="symbol">${item.symbol}</span>
                            <span class="ex-tag">${item.exchange}</span>
                        </div>
                        <div class="side-badge ${isLong ? 'side-long' : 'side-short'}" style="margin:0;">
                            ${isLong ? '▲ LONG' : '▼ SHORT'}
                        </div>
                    </div>
                `;
            }).join('');
        }

        async function loadData() {
            try {
                const res = await fetch(`/api/data?user_id=${userId}`);
                const data = await res.json();
                cachedHistory = data.history || [];
                renderHistory();
                renderLive(data.active || []);
                if (!volInitialized && data.user_min_volume) {
                    document.getElementById('min-vol-input').value = Math.round(data.user_min_volume);
                    volInitialized = true;
                }
            } catch (e) {
                console.error('Ошибка загрузки:', e);
            }
        }

        function applyPreset(val) {
            document.getElementById('min-vol-input').value = val;
            saveVolume();
        }

        async function saveVolume() {
            const val = Number(document.getElementById('min-vol-input').value);
            if (!val || val < 100000) {
                showToast('Минимум $100,000');
                return;
            }
            if (!userId) {
                showToast('Откройте приложение через Telegram');
                return;
            }
            try {
                const res = await fetch('/api/settings', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, min_volume: val })
                });
                if (res.ok) {
                    showToast(`Сохранено: ${formatVol(val)}`);
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
    min_volume: float


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """Управляет запуском и корректной остановкой БД, 6 сканеров бирж и Telegram-бота."""
    await init_db()

    connector = aiohttp.TCPConnector(limit=100)
    session = aiohttp.ClientSession(connector=connector)

    # 6 бирж (Binance, Bybit, OKX, Bitget, Gate, KuCoin)
    scanners: List[BaseExchangeScanner] = [
        BinanceFuturesScanner(engine, session),
        BybitFuturesScanner(engine, session),
        OKXSwapScanner(engine, session),
        BitgetFuturesScanner(engine, session),
        GateFuturesScanner(engine, session),
        KuCoinFuturesScanner(engine, session),
    ]

    logger.info("Запуск мультибиржевого детектора аптиков (6 бирж)...")
    bg_tasks: List[asyncio.Task] = [asyncio.create_task(s.run_forever()) for s in scanners]

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


app = FastAPI(title="Uptick Detector & Telegram Mini App", lifespan=lifespan)


@app.get("/")
async def root() -> dict:
    """Базовый маршрут для проверки работоспособности сервиса."""
    return {"status": "ok", "service": "Uptick Detector", "exchanges": 6}


@app.get("/ping")
async def ping() -> dict:
    """Эндпоинт для UptimeRobot, чтобы бесплатный сервер не засыпал."""
    return {"status": "ok", "exchanges": 6}


@app.get("/api/data")
async def api_data(user_id: int = Query(default=0)) -> JSONResponse:
    """Отдает данные для Mini App: историю за текущие сутки UTC, активные планки и настройки юзера."""
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    history_rows = []
    user_min_vol = DEFAULT_MIN_VOLUME

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM history WHERE utc_date = ? ORDER BY id DESC LIMIT 100",
                (today_utc,),
            ) as cursor:
                history_rows = [dict(r) for r in await cursor.fetchall()]

            if user_id:
                async with db.execute(
                    "SELECT min_volume FROM users WHERE user_id = ?", (user_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        user_min_vol = float(row["min_volume"])
    except Exception as exc:
        logger.error(f"Ошибка чтения /api/data: {exc}")

    active_list = [
        {"exchange": ex, "symbol": sym, "side": side.value}
        for ex, sym, side in engine.get_active_upticks()
    ]

    return JSONResponse(
        {
            "utc_date": today_utc,
            "active": active_list,
            "history": history_rows,
            "user_min_volume": user_min_vol,
        }
    )


@app.post("/api/settings")
async def api_settings(payload: SettingsPayload) -> dict:
    """Сохраняет индивидуальный минимальный оборот из Telegram Mini App."""
    safe_vol = max(100_000.0, float(payload.min_volume))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO users (user_id, min_volume) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET min_volume = excluded.min_volume",
            (payload.user_id, safe_vol),
        )
        await db.commit()
    return {"status": "ok", "min_volume": safe_vol}


@app.get("/webapp", response_class=HTMLResponse)
async def webapp_page() -> HTMLResponse:
    """Отдает интерфейс Telegram Mini App."""
    return HTMLResponse(content=WEBAPP_HTML)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)