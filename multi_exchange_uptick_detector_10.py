"""
Мультибиржевой WebSocket-детектор «Аптиков» (Binance, Bybit, OKX, Aster)
+ WebSocket-детектор «Покупаш / Продаваш» (Binance, Bybit, OKX, Bitget, Gate, Aster)
+ Telegram Bot + Mini App + Tiger.Trade.
Поддержка PostgreSQL (Neon/Supabase через DATABASE_URL) с автоматическим откатом на SQLite.
"""

import asyncio
# ... existing code ...
DEFAULT_MIN_ROBOT_USD = 25_000.0  # Мин. объем работы Покупаша/Продаваша за окно ($)
ROBOT_WINDOW_SEC = 25.0           # Окно анализа ленты сделок (сек)
ROBOT_COOLDOWN_SEC = 180          # 3 минуты между повторами по одной монете и стороне
ROBOT_MAX_SYMBOLS = 40            # Топ активных альткоинов на биржу для WS-потока сделок
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
        if fallback_min_total is not None and fallback_min_total > 0
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
# ... existing code ...
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
# ... existing code ...
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        min_volume DOUBLE PRECISION DEFAULT 500000,
                        min_density_spot DOUBLE PRECISION DEFAULT 250000,
                        min_density_futures DOUBLE PRECISION DEFAULT 1000000,
                        coin_rules TEXT DEFAULT '{}',
                        banned_coins TEXT DEFAULT '{}',
                        enable_upticks INTEGER DEFAULT 1,
                        enable_robots INTEGER DEFAULT 1,
                        min_robot_usd DOUBLE PRECISION DEFAULT 25000,
                        robot_config TEXT DEFAULT '{}'
                    )
                    """
                )
# ... existing code ...
                for col_def in (
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS min_density_spot DOUBLE PRECISION DEFAULT 250000",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS min_density_futures DOUBLE PRECISION DEFAULT 1000000",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS coin_rules TEXT DEFAULT '{}'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS banned_coins TEXT DEFAULT '{}'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS enable_upticks INTEGER DEFAULT 1",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS enable_robots INTEGER DEFAULT 1",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS min_robot_usd DOUBLE PRECISION DEFAULT 25000",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS robot_config TEXT DEFAULT '{}'",
                    "ALTER TABLE upticks ADD COLUMN IF NOT EXISTS wall_usd DOUBLE PRECISION DEFAULT 0",
                ):
                    await conn.execute(col_def)
# ... existing code ...
    async with aiosqlite.connect(SQLITE_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                min_volume REAL DEFAULT 500000,
                min_density_spot REAL DEFAULT 250000,
                min_density_futures REAL DEFAULT 1000000,
                coin_rules TEXT DEFAULT '{}',
                banned_coins TEXT DEFAULT '{}',
                enable_upticks INTEGER DEFAULT 1,
                enable_robots INTEGER DEFAULT 1,
                min_robot_usd REAL DEFAULT 25000,
                robot_config TEXT DEFAULT '{}'
            )
            """
        )
# ... existing code ...
        if "enable_robots" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN enable_robots INTEGER DEFAULT 1")
        if "min_robot_usd" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN min_robot_usd REAL DEFAULT 25000")
        if "robot_config" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN robot_config TEXT DEFAULT '{}'")
        cur_up = await db.execute("PRAGMA table_info(upticks)")
# ... existing code ...
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
        if price <= 0 or qty_usd <= 0 or is_blacklisted_symbol(symbol):
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

        # Объединяем части одного рыночного ордера в один принт (адаптивно под мин. интервал)
        merge_sec = min(0.25, max(0.05, float(cfg["min_interval_sec"]) * 0.45))
        if dq and dq[-1][1] == is_buy and (ts - dq[-1][0]) <= merge_sec:
            dq[-1][2] += qty_usd
            dq[-1][3] = price
        else:
            dq.append([ts, is_buy, qty_usd, price])

        # Держим до 125 сек истории, чтобы при смене окна не терять накопленные принты
        max_keep_cutoff = ts - max(125.0, float(cfg["window_sec"]) + 5.0)
        while dq and dq[0][0] < max_keep_cutoff:
            dq.popleft()

        # Проверяем паттерн не чаще 1 раза в секунду на пару
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

        # Фильтруем принты по заданному пользователем диапазону одного удара ($)
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

        # Проверяем общий объём именно подходящих принтов
        matched_usd = sum(p[2] for p in sig_prints)
        if matched_usd < min_total_usd * 0.75:
            return

        # Вычисляем интервалы между ударами
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
# ... existing code ...
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


async def handle_robot_event(event: RobotEvent) -> None:
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
    """Загружает сохранённую конфигурацию Покупаша/Продаваша из БД при запуске."""
    try:
        rows = await db_fetchall(
            "SELECT user_id, min_robot_usd, robot_config FROM users WHERE robot_config IS NOT NULL AND robot_config != '{}'"
        )
        if rows:
            row = rows[-1]
            rcfg = normalize_robot_config(row.get("robot_config"), fallback_min_total=row.get("min_robot_usd"))
            robot_engine.update_config(rcfg)
    except Exception as exc:
        logger.debug("Не удалось загрузить robot_config из БД: %s", exc)


async def notify_bot_updated() -> None:
    """Отправляет пользователям уведомление в Telegram после успешного обновления и запуска бота."""
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
            "Скринер снова работает 24/7."
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
        "• ⚡ <b>Аптики</b> (Binance, Bybit, OKX, Aster) — проверка реальной плотности у планки\n"
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
# ... existing code ...
    lines.append("")
    if robots:
        lines.append("🤖 <b>Покупаш / Продаваш сейчас:</b>")
        for ex, sym, side, usd, cnt, avg_int, avg_pr, _, _ in robots[:10]:
            lbl = "🟢 ПОКУПАШ" if side == RobotSide.BUY else "🔴 ПРОДАВАШ"
            lines.append(f"• {ex} | <code>{sym}</code> — <b>{lbl}</b> (${usd:,.0f}, {cnt}× ~${avg_pr:,.0f} / {avg_int:.1f}с)")
    else:
        lines.append("🤖 Активных Покупашей / Продавашей сейчас нет.")
# ... existing code ...
        .mode-pill .st { font-size: 11px; padding: 2px 6px; border-radius: 6px; background: rgba(255,255,255,0.06); }
        .mode-pill.on .st { background: rgba(22,199,132,0.22); color: var(--long); }

        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 8px; }
        .field-group { display: flex; flex-direction: column; gap: 4px; }
        .field-group input, .field-group select { width: 100%; box-sizing: border-box; }
        .field-lbl { font-size: 11px; color: var(--hint); font-weight: 600; }
        .preset-row { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; margin-bottom: 10px; }
        .preset-btn {
            padding: 8px 6px; border-radius: 9px; border: 1px solid var(--border);
            background: rgba(255,255,255,0.04); color: var(--text); font-size: 11px;
            font-weight: 700; cursor: pointer; transition: 0.15s;
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
# ... existing code ...
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
# ... existing code ...
        let cachedRobots = [];
        let cachedActiveRob = [];
        let userBannedCoins = {};
        let selectedRobotExchanges = [...ROBOT_EXCHANGES];
        let settingsInitialized = false;
        let lastUptickId = 0;
        let lastRobotId = 0;
        let firstLoadDone = false;
        let tigerWs = null;
# ... existing code ...
        function renderRobotExChecks() {
            const box = document.getElementById('rob-ex-checks');
            if (!box) return;
            box.innerHTML = ROBOT_EXCHANGES.map(ex => {
                const on = selectedRobotExchanges.includes(ex);
                return `<button type="button" class="ex-chk ${on ? 'on' : ''}" onclick="toggleRobotEx('${ex}')">
                    <span class="ex-logo" style="width:15px;height:15px">${EX_SVG[ex]||''}</span>${ex}
                </button>`;
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
                const p = JSON.parse(localStorage.getItem('uptick_prefs_v3') || '{}');
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
            localStorage.setItem('uptick_prefs_v3', JSON.stringify({
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
# ... existing code ...
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
# ... existing code ...
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
# ... existing code ...
class SettingsPayload(BaseModel):
    user_id: int
    min_volume: float = DEFAULT_MIN_VOLUME
    min_robot_usd: float = DEFAULT_MIN_ROBOT_USD
    enable_upticks: bool = True
    enable_robots: bool = True
    robot_config: Optional[Dict[str, Any]] = None
# ... existing code ...
@asynccontextmanager
async def lifespan(_app: FastAPI):
    await init_db()
    await load_engine_robot_config()
    asyncio.create_task(notify_bot_updated())

    ws_session = aiohttp.ClientSession(
# ... existing code ...
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
        if not is_blacklisted_symbol(str(r.get("symbol") or ""))
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
        if not is_blacklisted_symbol(sym)
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
# ... existing code ...
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)