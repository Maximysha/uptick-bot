"""
Независимый модуль отправки алертов в Discord через Webhooks (aiohttp).
Не требует установки discord.py и отдельного Discord-бота.

Поддерживает:
- Отдельные каналы для Аптиков и Покупаш/Продаваш, либо один общий канал.
- Настройку через переменные окружения Render:
    DISCORD_WEBHOOK_URL         — общий канал
    DISCORD_UPTICK_WEBHOOK_URL  — отдельный канал только для Аптиков
    DISCORD_ROBOT_WEBHOOK_URL   — отдельный канал только для Покупаш / Продаваш
- Настройку прямо из Mini App (сохраняется в БД).
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from robots import RobotEvent, RobotSide
from upticks import UptickEvent, UptickSide

logger = logging.getLogger("uptick_bot.discord")

# Цвета Discord Embed
COLOR_GREEN = 0x16C784
COLOR_RED = 0xEA3943
COLOR_BLUE = 0x3B82F6

VALID_WEBHOOK_PREFIXES = (
    "https://discord.com/api/webhooks/",
    "https://discordapp.com/api/webhooks/",
    "https://canary.discord.com/api/webhooks/",
    "https://ptb.discord.com/api/webhooks/",
)

DEFAULT_DISCORD_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "send_upticks": True,
    "send_robots": True,
    "uptick_webhook": "",
    "robot_webhook": "",
}


def is_valid_discord_webhook(url: str) -> bool:
    """Проверяет, что строка является корректным URL Discord Webhook."""
    clean = (url or "").strip()
    return bool(clean) and clean.startswith(VALID_WEBHOOK_PREFIXES) and len(clean) > 45


def get_env_webhooks() -> Tuple[str, str]:
    """Возвращает (uptick_webhook, robot_webhook) из переменных окружения Render."""
    common = (os.getenv("DISCORD_WEBHOOK_URL") or "").strip()
    up_env = (os.getenv("DISCORD_UPTICK_WEBHOOK_URL") or "").strip() or common
    rob_env = (os.getenv("DISCORD_ROBOT_WEBHOOK_URL") or "").strip() or common
    return (
        up_env if is_valid_discord_webhook(up_env) else "",
        rob_env if is_valid_discord_webhook(rob_env) else "",
    )


def normalize_discord_config(raw: Any) -> Dict[str, Any]:
    """Нормализует пользовательские настройки Discord из БД или запроса Mini App."""
    cfg = dict(DEFAULT_DISCORD_CONFIG)
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

    cfg["enabled"] = bool(data.get("enabled", cfg["enabled"]))
    cfg["send_upticks"] = bool(data.get("send_upticks", cfg["send_upticks"]))
    cfg["send_robots"] = bool(data.get("send_robots", cfg["send_robots"]))

    up_url = str(data.get("uptick_webhook") or "").strip()
    rob_url = str(data.get("robot_webhook") or "").strip()
    cfg["uptick_webhook"] = up_url if is_valid_discord_webhook(up_url) else ""
    cfg["robot_webhook"] = rob_url if is_valid_discord_webhook(rob_url) else ""
    return cfg


def resolve_webhook_url(cfg: Dict[str, Any], kind: str) -> str:
    """
    Определяет итоговый Discord Webhook URL для типа сигнала ('UPTICK' или 'ROBOT').
    Приоритет:
    1) Заданный пользователем URL в Mini App (если заполнен только верхний — он работает как общий).
    2) Переменные окружения Render (DISCORD_UPTICK_WEBHOOK_URL / DISCORD_ROBOT_WEBHOOK_URL / DISCORD_WEBHOOK_URL).
    """
    if not cfg.get("enabled", True):
        return ""
    env_up, env_rob = get_env_webhooks()

    user_up = str(cfg.get("uptick_webhook") or "").strip()
    user_rob = str(cfg.get("robot_webhook") or "").strip()

    if kind == "UPTICK":
        if not cfg.get("send_upticks", True):
            return ""
        cand = user_up or env_up
        return cand if is_valid_discord_webhook(cand) else ""

    if kind == "ROBOT":
        if not cfg.get("send_robots", True):
            return ""
        # Если поле для роботов пустое, но указан верхний вебхук — используем его как общий канал
        cand = user_rob or user_up or env_rob
        return cand if is_valid_discord_webhook(cand) else ""

    return ""


async def _post_discord_webhook(
    webhook_url: str,
    payload: Dict[str, Any],
    session: Optional[aiohttp.ClientSession] = None,
) -> Tuple[bool, str]:
    """Отправляет JSON-payload в Discord Webhook с обработкой rate limit (429)."""
    if not is_valid_discord_webhook(webhook_url):
        return False, "Некорректный URL Discord Webhook"

    own_session = session is None or session.closed
    client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) if own_session else session
    try:
        for attempt in range(2):
            async with client.post(
                webhook_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status in (200, 204):
                    return True, "OK"
                if resp.status == 429 and attempt == 0:
                    try:
                        body = await resp.json()
                        retry_after = min(5.0, float(body.get("retry_after") or 1.5))
                    except Exception:
                        retry_after = 1.5
                    await asyncio.sleep(retry_after)
                    continue
                err_txt = (await resp.text())[:200]
                logger.warning("[Discord] HTTP %s: %s", resp.status, err_txt)
                return False, f"HTTP {resp.status}"
        return False, "Rate limited"
    except Exception as exc:
        logger.warning("[Discord] Ошибка отправки: %s", exc)
        return False, str(exc)
    finally:
        if own_session:
            await client.close()


async def send_uptick_discord(
    webhook_url: str,
    event: UptickEvent,
    session: Optional[aiohttp.ClientSession] = None,
) -> bool:
    """Отправляет минималистичную карточку (Embed) Аптика в Discord-канал: монета, биржа, LONG/SHORT, цена и время."""
    is_long = event.side == UptickSide.LONG
    side_emoji = "🟢" if is_long else "🔴"
    payload = {
        "username": "Uptick Screener",
        "embeds": [
            {
                "title": f"{side_emoji} {event.symbol}",
                "color": COLOR_GREEN if is_long else COLOR_RED,
                "fields": [
                    {"name": "Биржа", "value": f"**{event.exchange}**", "inline": True},
                    {"name": "Направление", "value": f"{side_emoji} **{event.side.value}**", "inline": True},
                    {"name": "Цена", "value": f"`{event.price:g}`", "inline": True},
                ],
                "footer": {"text": f"{event.timestamp_utc} UTC"},
            }
        ],
    }
    ok, _ = await _post_discord_webhook(webhook_url, payload, session=session)
    return ok


async def send_robot_discord(
    webhook_url: str,
    event: RobotEvent,
    session: Optional[aiohttp.ClientSession] = None,
) -> bool:
    """Отправляет красивую карточку (Embed) Покупаша / Продаваша в Discord-канал."""
    is_buy = event.side == RobotSide.BUY
    label = "ПОКУПАШ" if is_buy else "ПРОДАВАШ"
    side_emoji = "🟢" if is_buy else "🔴"
    payload = {
        "username": "Market Robots Screener",
        "embeds": [
            {
                "title": f"🤖 {label} | {event.exchange} · {event.symbol}",
                "color": COLOR_GREEN if is_buy else COLOR_RED,
                "fields": [
                    {
                        "name": f"{side_emoji} Объём ({event.window_sec}с)",
                        "value": f"**${event.total_usd:,.0f}** ({event.dominance_pct:.0f}% перекос)",
                        "inline": True,
                    },
                    {
                        "name": "Принты / ритм",
                        "value": f"**{event.prints_count}×** ~**${event.avg_print_usd:,.0f}** / раз в **{event.avg_interval_sec:g}с**",
                        "inline": True,
                    },
                    {"name": "Цена", "value": f"`{event.price:g}`", "inline": True},
                    {"name": "Оборот 24ч", "value": f"**${event.volume_24h_usd:,.0f}**", "inline": True},
                ],
                "footer": {"text": f"Robots WebSocket · {event.timestamp_utc} UTC"},
            }
        ],
    }
    ok, _ = await _post_discord_webhook(webhook_url, payload, session=session)
    return ok


async def send_startup_discord(
    webhook_urls: List[str],
    commit_sha: str,
    now_utc: str,
    robot_cfg: Dict[str, Any],
    session: Optional[aiohttp.ClientSession] = None,
) -> None:
    """Отправляет уведомление о запуске/обновлении бота в уникальные Discord-каналы."""
    unique_urls = list(dict.fromkeys(u for u in webhook_urls if is_valid_discord_webhook(u)))
    if not unique_urls:
        return

    ver_line = f"Версия: `{commit_sha}`\n" if commit_sha else ""
    desc = (
        f"{ver_line}"
        f"Время запуска: `{now_utc}`\n"
        f"• ⚡ Аптики (WebSocket · 4 биржи): активны\n"
        f"• 🤖 Покупаш/Продаваш (WebSocket · {len(robot_cfg.get('exchanges', []))} бирж): "
        f"от **${float(robot_cfg.get('min_total_usd', 25000)):,.0f}** / {float(robot_cfg.get('window_sec', 25)):g}с"
    )
    payload = {
        "username": "Screener Status",
        "embeds": [
            {
                "title": "🚀 Бот обновлён и запущен!",
                "description": desc,
                "color": COLOR_BLUE,
                "footer": {"text": "Скринер работает 24/7"},
            }
        ],
    }
    for url in unique_urls:
        await _post_discord_webhook(url, payload, session=session)


async def send_test_discord(
    webhook_url: str,
    label: str = "Общий канал",
    session: Optional[aiohttp.ClientSession] = None,
) -> Tuple[bool, str]:
    """Отправляет тестовое сообщение в Discord для проверки вебхука."""
    payload = {
        "username": "Uptick & Robot Screener",
        "embeds": [
            {
                "title": f"✅ Тест Discord ({label})",
                "description": "Вебхук подключён успешно! Новые алерты будут приходить в этот канал.",
                "color": COLOR_GREEN,
            }
        ],
    }
    return await _post_discord_webhook(webhook_url, payload, session=session)