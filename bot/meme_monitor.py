"""
MemeTokenMonitor — следит за ростом мем-токенов на Stellar SDEX
Публикует алерты когда токен вырос на X% за последний час
"""

import requests
import logging
import time
from datetime import datetime, timezone
from config import Config

log = logging.getLogger(__name__)

# Stellar Expert API
STELLAR_EXPERT = "https://api.stellar.expert/explorer/public"


def _fmt_fdv(fdv: float) -> str:
    """Форматирует FDV красиво."""
    if fdv >= 1_000_000_000:
        return f"${fdv/1_000_000_000:.2f}B"
    elif fdv >= 1_000_000:
        return f"${fdv/1_000_000:.1f}M"
    elif fdv >= 1_000:
        return f"${fdv/1_000:.0f}K"
    return f"${fdv:.2f}"


def _fmt_pct(pct: float) -> str:
    if pct >= 0:
        return f"+{pct:.0f}%"
    return f"{pct:.0f}%"


class MemeTokenMonitor:
    def __init__(self, config: Config):
        self.config = config
        # хранит историю цен: {asset_key: [(timestamp, price), ...]}
        self.price_history: dict[str, list] = {}
        self.alerted: dict[str, float] = {}  # asset_key: last_alert_time
        self.known_assets: list[dict] = []
        self.last_asset_refresh = 0

    def stream(self):
        """Генератор мем-токен алертов."""
        while True:
            try:
                self._refresh_asset_list()
                alerts = self._check_pumps()
                for alert in alerts:
                    yield alert
                time.sleep(60)  # проверяем каждую минуту
            except Exception as e:
                log.error(f"MemeMonitor error: {e}")
                time.sleep(30)

    def _refresh_asset_list(self):
        """Обновляем список торгуемых активов раз в 10 минут."""
        now = time.time()
        if now - self.last_asset_refresh < 600:
            return
        self.last_asset_refresh = now

        try:
            # Получаем топ активов по объёму за 24ч
            r = requests.get(
                f"{STELLAR_EXPERT}/asset?order=volume24h&limit=200",
                timeout=10
            )
            assets = r.json().get("_embedded", {}).get("records", [])

            # Фильтруем: исключаем XLM, USDC, USDT (не мем)
            skip = {"XLM", "USDC", "USDT", "yXLM", "AQUA", "BTC", "ETH"}
            self.known_assets = [
                a for a in assets
                if a.get("asset", "").split("-")[0] not in skip
                and float(a.get("volume24h", 0)) > 0
            ]
            log.debug(f"Tracking {len(self.known_assets)} assets for pump detection")
        except Exception as e:
            log.debug(f"Asset list refresh failed: {e}")

    def _get_price(self, asset_code: str, asset_issuer: str) -> float | None:
        """Получает текущую цену актива в USD."""
        try:
            asset_id = f"{asset_code}-{asset_issuer}"
            r = requests.get(
                f"{STELLAR_EXPERT}/asset/{asset_id}",
                timeout=8
            )
            data = r.json()
            price = data.get("price", {})
            if isinstance(price, dict):
                return float(price.get("USD", 0)) or None
            return float(price) if price else None
        except Exception:
            return None

    def _get_supply(self, asset_code: str, asset_issuer: str) -> float:
        """Получает circulating supply."""
        try:
            asset_id = f"{asset_code}-{asset_issuer}"
            r = requests.get(
                f"{STELLAR_EXPERT}/asset/{asset_id}",
                timeout=8
            )
            data = r.json()
            return float(data.get("supply", 0))
        except Exception:
            return 0

    def _check_pumps(self) -> list:
        """Проверяет все активы на pump."""
        alerts = []
        now = time.time()

        for asset in self.known_assets[:50]:  # топ 50 по объёму
            try:
                raw = asset.get("asset", "")
                if "-" not in raw:
                    continue
                parts = raw.split("-")
                code = parts[0]
                issuer = parts[1] if len(parts) > 1 else ""
                if not issuer:
                    continue

                asset_key = f"{code}-{issuer[:8]}"

                # Получаем текущую цену
                price = self._get_price(code, issuer)
                if not price:
                    continue

                # Добавляем в историю
                if asset_key not in self.price_history:
                    self.price_history[asset_key] = []
                self.price_history[asset_key].append((now, price))

                # Убираем старые записи (> 2 часов)
                self.price_history[asset_key] = [
                    (t, p) for t, p in self.price_history[asset_key]
                    if now - t <= 7200
                ]

                history = self.price_history[asset_key]
                if len(history) < 2:
                    continue

                # Цена час назад
                hour_ago_entries = [
                    (t, p) for t, p in history
                    if 3600 <= now - t <= 4200  # 60-70 минут назад
                ]
                if not hour_ago_entries:
                    # Берём самую старую запись если нет hourly
                    oldest_time, oldest_price = history[0]
                    age_minutes = (now - oldest_time) / 60
                    if age_minutes < 10:
                        continue
                    price_then = oldest_price
                    timeframe = f"{age_minutes:.0f}m"
                else:
                    _, price_then = hour_ago_entries[-1]
                    timeframe = "1h"

                if price_then <= 0:
                    continue

                # Процент изменения
                pct_change = ((price - price_then) / price_then) * 100

                # Пороги алертов
                thresholds = [
                    (self.config.MEME_PUMP_1H_PCT, "🚀"),    # 100%+
                    (self.config.MEME_PUMP_MEGA_PCT, "🔥"),   # 500%+
                ]

                triggered_emoji = None
                for threshold, emoji in sorted(thresholds, reverse=True):
                    if pct_change >= threshold:
                        triggered_emoji = emoji
                        break

                if not triggered_emoji:
                    continue

                # Не алертить одно и то же чаще раза в 2 часа
                last_alert = self.alerted.get(asset_key, 0)
                if now - last_alert < 7200:
                    continue

                # Получаем supply для FDV
                supply = self._get_supply(code, issuer)
                fdv = price * supply if supply else 0

                # Объём
                volume_24h = float(asset.get("volume24h", 0))

                self.alerted[asset_key] = now

                alert = {
                    "type": "MEME_PUMP",
                    "code": code,
                    "issuer_short": issuer[:8],
                    "issuer_full": issuer,
                    "price": price,
                    "price_then": price_then,
                    "pct_change": pct_change,
                    "timeframe": timeframe,
                    "fdv": fdv,
                    "volume_24h": volume_24h,
                    "emoji": triggered_emoji,
                    "link": f"https://stellar.expert/explorer/public/asset/{code}-{issuer}",
                }
                alerts.append(alert)
                log.info(f"🚀 Pump detected: {code} {_fmt_pct(pct_change)} in {timeframe}")

            except Exception as e:
                log.debug(f"Error checking {asset.get('asset', '?')}: {e}")
                continue

        return alerts
