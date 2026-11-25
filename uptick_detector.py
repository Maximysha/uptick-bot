"""
Мультибиржевой детектор «Аптиков» + Сканер «Плотностей» (Спот и Фьючерсы) + Telegram Bot + Mini App + Tiger.Trade API.
# ... existing code ...
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
# ... existing code ...
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
                pairs.append((vol, sym, px))
# ... existing code ...
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
                pairs.append((vol, sym, px))
# ... existing code ...
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
                pairs.append((vol_usd, inst_id, clean_sym, px))
# ... existing code ...
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
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if is_blacklisted_symbol(clean_sym) and not is_watched:
                continue
            try:
                px = float(t.get("last") or 0)
                vol = float(t.get("volValue") or 0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, sym_raw, clean_sym, px))
# ... existing code ...
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
                pairs.append((vol, sym_raw, clean_sym, px, abs(mult)))
# ... existing code ...
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
                pairs.append((vol, sym, px))
# ... existing code ...
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
            is_watched = extract_base_asset(clean_sym) in self.engine.watched_bases
            if is_blacklisted_symbol(clean_sym) and not is_watched:
                continue
            try:
                px = float(t.get("last") or 0)
                vol = float(t.get("quote_volume") or 0)
            except (TypeError, ValueError):
                continue
            if px > 0 and (vol >= self.min_pair_volume_usd or is_watched):
                pairs.append((vol, pair_raw, clean_sym, px))
# ... existing code ...
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
                pairs.append((vol, name_raw, clean_sym, px, mult))
# ... existing code ...
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
                pairs.append((vol, sym, px))
# ... existing code ...
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
                pairs.append((vol, sym, px))
# ... existing code ...
def normalize_ban_scope(scope: Optional[str]) -> str:
    s = (scope or "ALL").strip().upper()
    if s in ("UPTICK", "UPTICKS", "U", "АПТИК", "АПТИКИ"):
        return "UPTICK"
    if s in ("DENSITY", "DENSITIES", "D", "ПЛОТНОСТЬ", "ПЛОТНОСТИ"):
        return "DENSITY"
    return "ALL"


def parse_banned_coins(raw: Optional[str]) -> Dict[str, str]:
    """Возвращает словарь {BASE: 'ALL' | 'UPTICK' | 'DENSITY'} (совместимо со старым списком)."""
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
    """Проверяет, скрыта ли монета для target ('UPTICK' или 'DENSITY')."""
    scope = banned_map.get(base)
    return scope in ("ALL", target)
# ... existing code ...
    base = extract_base_asset(event.symbol)
    target_ids: List[int] = []
    for u in users:
        banned_map = parse_banned_coins(u.get("banned_coins"))
        if is_coin_banned(base, banned_map, "UPTICK"):
            continue
        if event.volume_24h_usd >= float(u.get("min_volume") or DEFAULT_MIN_VOLUME):
            target_ids.append(int(u["user_id"]))

    await _broadcast_messages(target_ids, text, _build_main_keyboard())
# ... existing code ...
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
# ... existing code ...
        "• <code>/coin WIF 100000 500000</code> — свой порог плотности для любой монеты\n"
        "• <code>/ban MOODENG [all|uptick|density]</code> — убрать монету (оба / аптики / плотности)\n"
        "• <code>/unban MOODENG</code> — вернуть монету обратно\n"
# ... existing code ...
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
        spot_val = max(10_000.0, float(parts[2].replace(",", "").replace("$", "")))
# ... existing code ...
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
            await db_execute(
                "UPDATE users SET banned_coins = ? WHERE user_id = ?",
                (json.dumps(banned), message.from_user.id),
            )
            await message.answer(f"✅ Монета <b>{base}</b> возвращена в алерты.", parse_mode="HTML")
        else:
            await message.answer(f"Монета <b>{base}</b> не была скрыта.", parse_mode="HTML")
    except Exception:
        await message.answer("Использование: <code>/unban MOODENG</code>", parse_mode="HTML")
# ... existing code ...
        .banned-chip button {
            border: none; background: transparent; color: var(--short);
            font-weight: 800; cursor: pointer; padding: 0; font-size: 13px;
        }
        .banned-scope {
            font-size: 10px; font-weight: 700; padding: 2px 5px; border-radius: 6px;
            background: rgba(255, 255, 255, 0.1); color: var(--hint);
        }
        .modal-backdrop {
            position: fixed; inset: 0; background: rgba(0, 0, 0, 0.65);
            display: none; align-items: flex-end; justify-content: center;
            z-index: 120; padding: 14px;
        }
        .modal-backdrop.open { display: flex; }
        .modal-sheet {
            width: 100%; max-width: 440px; background: var(--card);
            border: 1px solid var(--border); border-radius: 16px;
            padding: 16px; box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
        }
        .modal-title { font-size: 15px; font-weight: 700; margin-bottom: 6px; }
        .modal-actions { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; }
        .modal-btn {
            width: 100%; padding: 11px 12px; border-radius: 10px; font-size: 13px;
            font-weight: 700; cursor: pointer; border: 1px solid var(--border);
            background: var(--bg); color: var(--text); text-align: left;
        }
        .modal-btn.danger {
            background: rgba(234, 57, 67, 0.16); color: var(--short);
            border-color: rgba(234, 57, 67, 0.45);
        }
        .modal-btn.cancel {
            text-align: center; color: var(--hint); background: transparent;
        }
        .toast {
# ... existing code ...
        <!-- Скрытые монеты (личный черный список) -->
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

        <!-- Индивидуальный порог плотности по монете -->
        <div class="setting-box">
            <label class="setting-label">🎯 Свой порог плотности для любой монеты</label>
            <div class="meta" style="margin-bottom:10px;">
                Можно добавить <b>абсолютно любую монету</b> (включая BTC, ETH, SOL, PEPE, WIF и др.) и задать свой порог плотности ($).
            </div>
# ... existing code ...
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
# ... existing code ...
        let cachedHistory = [];
        let cachedDensities = [];
        let userCoinRules = {};
        let userBannedCoins = {};
        let pendingBanSymbol = '';
        let settingsInitialized = false;
# ... existing code ...
        // ===== ЛИЧНЫЙ ЧЕРНЫЙ СПИСОК МОНЕТ =====
        const SCOPE_LABELS = {
            'ALL': 'Оба',
            'UPTICK': 'Аптики',
            'DENSITY': 'Плотн.'
        };

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
                    const lbl = SCOPE_LABELS[data.scope] || 'Оба';
                    showToast(`🚫 ${data.base} скрыта (${lbl})`);
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
            } catch {
                showToast('Ошибка');
            }
        }
# ... existing code ...
                    userCoinRules = data.user_coin_rules || {};
                    userBannedCoins = normalizeBannedObj(data.user_banned_coins);
                    renderCoinRules();
                    renderBannedCoins();
                    settingsInitialized = true;
# ... existing code ...
class SymbolPayload(BaseModel):
    user_id: int
    symbol: str


class BanCoinPayload(BaseModel):
    user_id: int
    symbol: str
    scope: str = "ALL"
# ... existing code ...
    user_coin_rules: Dict[str, Dict[str, float]] = {}
    user_banned_map: Dict[str, str] = {}

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
                user_banned_map = parse_banned_coins(row.get("banned_coins"))
# ... existing code ...
    history_rows = [
        h
        for h in _api_cache["history"]
        if not is_blacklisted_symbol(str(h.get("symbol") or ""))
        and not is_coin_banned(extract_base_asset(str(h.get("symbol") or "")), user_banned_map, "UPTICK")
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
# ... existing code ...
@app.post("/api/coin-rule")
async def api_set_coin_rule(payload: CoinRulePayload) -> JSONResponse:
    base = extract_base_asset(payload.symbol)
    if not base:
        return JSONResponse(
            {"status": "error", "detail": "Укажите корректный тикер монеты"},
            status_code=400,
        )
# ... existing code ...
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
        "INSERT INTO users (user_id, banned_coins) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET banned_coins = excluded.banned_coins",
        (payload.user_id, json.dumps(banned)),
    )
    return JSONResponse({"status": "ok", "banned_coins": banned})


@app.get("/webapp", response_class=HTMLResponse)
async def webapp_page() -> HTMLResponse:
    return HTMLResponse(content=WEBAPP_HTML)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
