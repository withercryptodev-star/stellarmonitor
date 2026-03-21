"""
WeeklyStats — статистика Stellar сети
- Автопост каждое воскресенье 12:00 UTC
- Команда /stats в Telegram — отчёт по запросу
"""

import time
import logging
import threading
import requests
import json
import os
from datetime import datetime, timezone
from collections import defaultdict

log = logging.getLogger(__name__)

TG_API = "https://api.telegram.org/bot{token}/{method}"
STATS_FILE = "stellarmonitor_stats.json"


class WeeklyStats:
    def __init__(self, config):
        self.config = config
        self.reset()
        self._load()  # загружаем сохранённые данные

        t = threading.Thread(target=self._scheduler, daemon=True, name="WeeklyStats")
        t.start()
        log.info("WeeklyStats scheduler started")

        if config.TELEGRAM_ENABLED and config.TELEGRAM_BOT_TOKEN:
            t2 = threading.Thread(target=self._tg_commands_listener,
                                  daemon=True, name="TgCommands")
            t2.start()
            log.info("Telegram /stats command listener started")

    def reset(self):
        self.week_start = datetime.now(timezone.utc)
        self.total_alerts = 0
        self.whale_xlm_count = 0
        self.whale_usdc_count = 0
        self.sdex_count = 0
        self.cross_border_count = 0
        self.soroban_count = 0
        self.meme_pump_count = 0
        self.airdrop_count = 0
        self.total_xlm_volume = 0.0
        self.total_usdc_volume = 0.0
        self.biggest_xlm = 0.0
        self.biggest_xlm_usd = 0.0
        self.biggest_usdc = 0.0
        self.biggest_sdex_usd = 0.0
        self.biggest_sdex_pair = ""
        self.sdex_pairs: dict = defaultdict(int)
        self.meme_tokens: dict = defaultdict(int)
        self._last_update_id = 0

    def _save(self):
        """Сохраняет статистику в JSON файл."""
        try:
            data = {
                "week_start": self.week_start.isoformat(),
                "total_alerts": self.total_alerts,
                "whale_xlm_count": self.whale_xlm_count,
                "whale_usdc_count": self.whale_usdc_count,
                "sdex_count": self.sdex_count,
                "cross_border_count": self.cross_border_count,
                "soroban_count": self.soroban_count,
                "meme_pump_count": self.meme_pump_count,
                "airdrop_count": self.airdrop_count,
                "total_xlm_volume": self.total_xlm_volume,
                "total_usdc_volume": self.total_usdc_volume,
                "biggest_xlm": self.biggest_xlm,
                "biggest_xlm_usd": self.biggest_xlm_usd,
                "biggest_usdc": self.biggest_usdc,
                "biggest_sdex_usd": self.biggest_sdex_usd,
                "biggest_sdex_pair": self.biggest_sdex_pair,
                "sdex_pairs": dict(self.sdex_pairs),
                "meme_tokens": dict(self.meme_tokens),
                "last_update_id": self._last_update_id,
            }
            with open(STATS_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log.warning(f"Stats save error: {e}")

    def _load(self):
        """Загружает статистику из файла если есть и неделя ещё та же."""
        if not os.path.exists(STATS_FILE):
            log.info("No stats file found, starting fresh")
            return
        try:
            with open(STATS_FILE) as f:
                data = json.load(f)

            saved_start = datetime.fromisoformat(data["week_start"])
            now = datetime.now(timezone.utc)

            # Проверяем — та же неделя?
            saved_week = saved_start.isocalendar()[1]
            current_week = now.isocalendar()[1]

            if saved_week != current_week:
                log.info(f"Stats from week #{saved_week}, current week #{current_week} — starting fresh")
                os.remove(STATS_FILE)
                return

            # Восстанавливаем данные
            self.week_start = saved_start
            self.total_alerts = data.get("total_alerts", 0)
            self.whale_xlm_count = data.get("whale_xlm_count", 0)
            self.whale_usdc_count = data.get("whale_usdc_count", 0)
            self.sdex_count = data.get("sdex_count", 0)
            self.cross_border_count = data.get("cross_border_count", 0)
            self.soroban_count = data.get("soroban_count", 0)
            self.meme_pump_count = data.get("meme_pump_count", 0)
            self.airdrop_count = data.get("airdrop_count", 0)
            self.total_xlm_volume = data.get("total_xlm_volume", 0.0)
            self.total_usdc_volume = data.get("total_usdc_volume", 0.0)
            self.biggest_xlm = data.get("biggest_xlm", 0.0)
            self.biggest_xlm_usd = data.get("biggest_xlm_usd", 0.0)
            self.biggest_usdc = data.get("biggest_usdc", 0.0)
            self.biggest_sdex_usd = data.get("biggest_sdex_usd", 0.0)
            self.biggest_sdex_pair = data.get("biggest_sdex_pair", "")
            self.sdex_pairs = defaultdict(int, data.get("sdex_pairs", {}))
            self.meme_tokens = defaultdict(int, data.get("meme_tokens", {}))
            self._last_update_id = data.get("last_update_id", 0)

            log.info(f"Stats loaded: {self.total_alerts} alerts since {saved_start.strftime('%b %d %H:%M UTC')}")

        except Exception as e:
            log.warning(f"Stats load error: {e}, starting fresh")

    def record(self, alert: dict):
        atype = alert.get("type", "")
        self.total_alerts += 1

        if atype == "WHALE_XLM":
            self.whale_xlm_count += 1
            amount = alert.get("amount", 0)
            usd = alert.get("usd_value", 0)
            self.total_xlm_volume += amount
            if amount > self.biggest_xlm:
                self.biggest_xlm = amount
                self.biggest_xlm_usd = usd

        elif atype == "WHALE_USDC":
            self.whale_usdc_count += 1
            amount = alert.get("amount", 0)
            self.total_usdc_volume += amount
            if amount > self.biggest_usdc:
                self.biggest_usdc = amount

        elif atype in ("SDEX", "SDEX_MULTI"):
            self.sdex_count += 1
            usd = alert.get("usd_value", 0)
            pair = alert.get("pair", "")
            self.sdex_pairs[pair] += 1
            if usd > self.biggest_sdex_usd:
                self.biggest_sdex_usd = usd
                self.biggest_sdex_pair = pair

        elif atype == "CROSS_BORDER":
            self.cross_border_count += 1

        elif atype in ("SOROBAN_LIQUIDITY", "SOROBAN_SWAP", "SOROBAN_NEW_POOL"):
            self.soroban_count += 1

        elif atype == "MEME_PUMP":
            self.meme_pump_count += 1
            code = alert.get("code", "")
            if code:
                self.meme_tokens[code] += 1

        elif atype == "AIRDROP_SUMMARY":
            self.airdrop_count += 1

        # Сохраняем после каждого алерта
        self._save()

    def build_report(self, period_label: str = None) -> str:
        now = datetime.now(timezone.utc)
        days = max(1, (now - self.week_start).days)
        week_num = now.isocalendar()[1]
        label = period_label or f"Week #{week_num}"

        def fmt_num(n):
            if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
            if n >= 1_000: return f"{n/1_000:.0f}K"
            return str(int(n))

        def fmt_usd(n):
            if n >= 1_000_000_000: return f"${n/1_000_000_000:.2f}B"
            if n >= 1_000_000: return f"${n/1_000_000:.1f}M"
            if n >= 1_000: return f"${n/1_000:.0f}K"
            return f"${n:.0f}"

        top_pairs = sorted(self.sdex_pairs.items(), key=lambda x: x[1], reverse=True)[:3]
        pairs_str = " · ".join([f"{p[0]} ({p[1]})" for p in top_pairs]) if top_pairs else "—"

        top_memes = sorted(self.meme_tokens.items(), key=lambda x: x[1], reverse=True)[:3]
        memes_str = " · ".join([f"${p[0]}" for p in top_memes]) if top_memes else "—"

        since_str = self.week_start.strftime("%b %d, %H:%M UTC")

        report = (
            f"📊 *Stellar Monitor — {label}*\n"
            f"_Since {since_str} · {days}d tracked_\n\n"
            f"🔔 Total alerts: *{self.total_alerts}*\n\n"
            f"🐋 *Whale activity:*\n"
            f"  XLM transfers: {self.whale_xlm_count}\n"
            f"  USDC transfers: {self.whale_usdc_count}\n"
        )

        if self.biggest_xlm > 0:
            report += f"  Biggest XLM: {fmt_num(self.biggest_xlm)} XLM ({fmt_usd(self.biggest_xlm_usd)})\n"
        if self.biggest_usdc > 0:
            report += f"  Biggest USDC: {fmt_usd(self.biggest_usdc)}\n"

        report += (
            f"\n📊 *SDEX:*\n"
            f"  Large trades: {self.sdex_count}\n"
        )
        if self.biggest_sdex_usd > 0:
            report += f"  Biggest trade: {fmt_usd(self.biggest_sdex_usd)} ({self.biggest_sdex_pair})\n"
        report += f"  Top pairs: {pairs_str}\n"

        if self.airdrop_count > 0:
            report += f"\n📦 Mass distributions: {self.airdrop_count}\n"

        if self.meme_pump_count > 0:
            report += (
                f"\n🚀 *Meme pumps:* {self.meme_pump_count}\n"
                f"  Hot tokens: {memes_str}\n"
            )

        if self.soroban_count > 0:
            report += f"\n🤖 Soroban events: {self.soroban_count}\n"

        if self.cross_border_count > 0:
            report += f"\n🌐 Cross-border: {self.cross_border_count}\n"

        return report

    # ── Telegram /stats команда ──────────────────────────────────────────────

    def _tg_commands_listener(self):
        """Слушает команды от бота через getUpdates long polling."""
        token = self.config.TELEGRAM_BOT_TOKEN
        private_id = getattr(self.config, "TELEGRAM_PRIVATE_ID", "")
        url = TG_API.format(token=token, method="getUpdates")

        while True:
            try:
                resp = requests.post(url, json={
                    "offset": self._last_update_id + 1,
                    "timeout": 30,
                    "allowed_updates": ["message"],
                }, timeout=40)

                data = resp.json()
                if not data.get("ok"):
                    time.sleep(5)
                    continue

                for update in data.get("result", []):
                    self._last_update_id = update["update_id"]
                    msg = update.get("message", {})
                    text = msg.get("text", "").strip()
                    chat_id = str(msg.get("chat", {}).get("id", ""))

                    # Принимаем команды только из приватного чата
                    if not private_id or chat_id != str(private_id):
                        continue

                    if text in ("/stats", "/report"):
                        log.info("Received /stats command from private chat")
                        report = self.build_report(period_label="On-demand Report")
                        self._send_tg(report, chat_id, token)

                    elif text == "/reset":
                        self.reset()
                        self._save()
                        self._send_tg("✅ Stats reset", chat_id, token)

            except Exception as e:
                log.warning(f"TgCommands error: {e}")
                time.sleep(10)

    def _send_tg(self, text: str, chat_id: str, token: str):
        url = TG_API.format(token=token, method="sendMessage")
        try:
            requests.post(url, json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }, timeout=10)
        except Exception as e:
            log.warning(f"Stats TG send error: {e}")

    # ── Автопост воскресенье ─────────────────────────────────────────────────

    def _scheduler(self):
        posted_this_week = False
        while True:
            try:
                now = datetime.now(timezone.utc)
                is_sunday = now.weekday() == 6
                is_noon = now.hour == 12 and now.minute < 10

                if is_sunday and is_noon and not posted_this_week:
                    log.info("Weekly stats: posting report...")
                    report = self.build_report()
                    self._post_callback(report)
                    posted_this_week = True
                    self.reset()
                    self._save()
                    log.info("Weekly stats: reset for new week")

                elif not is_sunday:
                    posted_this_week = False

                time.sleep(600)

            except Exception as e:
                log.error(f"WeeklyStats scheduler error: {e}")
                time.sleep(60)

    def _post_callback(self, text: str):
        """Переопределяется в main.py."""
        log.info(f"[WeeklyStats] Report:\n{text}")
