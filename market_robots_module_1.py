"""
Независимый модуль WebSocket-детектора «Покупаш / Продаваш» (6 бирж:
Binance, Bybit, OKX, Bitget, Gate, Aster).
Полностью исключает акции, RWA, ETF, металлы, нефть/газ, форекс,
тяжёлые монеты и стейблкоины (динамически через API всех 6 бирж + ручные списки).
"""

# ... existing code ...

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
        s = (symbol or "").upper().strip()
        for sep in ("-USDT-SWAP", "-USD-SWAP", "_USDT", "-USDT", "USDT", "USDC"):
            if s.endswith(sep) and len(s) > len(sep):
                s = s[: -len(sep)]
                break
        for pref in ("1000000", "100000", "10000", "1000"):
            if s.startswith(pref) and len(s) > len(pref):
                s = s[len(pref) :]
                break
        return s

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
        # Периодическая очистка устаревших кандидатов
        stale_pend = [k for k, (first_ts, _) in self._pending.items() if now - first_ts > 120]
        for k in stale_pend:
            self._pending.pop(k, None)
        return [
            (ex, sym, side, usd, cnt, avg_int, avg_print, dom, vol)
            for (ex, sym), (side, _, usd, cnt, avg_int, avg_print, dom, vol) in self._active.items()
        ]


# ==================== WEBSOCKET-СКАНЕРЫ ПОКУПАШ / ПРОДАВАШ (6 БИРЖ) ====================

# ... existing code ...
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
# ... existing code ...
                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send_json({"op": "ping"})
                        except Exception:
                            break

                pt = asyncio.create_task(pinger())
                try:
# ... existing code ...
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
# ... existing code ...
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
# ... existing code ...
                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(25)
                        try:
                            await ws.send_str("ping")
                        except Exception:
                            break

                pt = asyncio.create_task(pinger())
                try:
# ... existing code ...
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
# ... existing code ...
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
# ... existing code ...
        asyncio.create_task(bitget_robot_ws(session, rob_eng)),
        asyncio.create_task(gate_robot_ws(session, rob_eng)),
    ]