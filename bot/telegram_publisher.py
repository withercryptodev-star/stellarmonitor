"""
TelegramPublisher — отправляет алерты в Telegram

Два канала:
- Публичный (TELEGRAM_CHAT_ID)   — все алерты
- Приватный (TELEGRAM_PRIVATE_ID) — только VIP алерты с аналитикой
"""

import time
import logging
import requests
from collections import deque
from config import Config

log = logging.getLogger(__name__)
TG_API = "https://api.telegram.org/bot{token}/{method}"

# ── Scoring критерии для приватного чата ─────────────────────────────────────
# Алерт попадает в приватный чат если набирает >= PRIVATE_SCORE_THRESHOLD

PRIVATE_SCORE_THRESHOLD = 10

def _interest_score(alert: dict) -> tuple[int, list[str]]:
    """
    Считает "interesting score" алерта.
    Возвращает (score, список причин почему интересно).
    Чем выше score — тем интереснее алерт для приватного чата.
    """
    score = 0
    reasons = []
    atype = alert.get("type", "")
    usd   = alert.get("usd_value", 0) or alert.get("total_usd", 0)

    # ── Объём ──────────────────────────────────────────────────────────
    if usd >= 100_000_000:
        score += 30; reasons.append(f"🚨 Massive: {_usd(usd)}")
    elif usd >= 10_000_000:
        score += 20; reasons.append(f"🔴 Very large: {_usd(usd)}")
    elif usd >= 5_000_000:
        score += 15; reasons.append(f"🟠 Large: {_usd(usd)}")
    elif usd >= 1_000_000:
        score += 8;  reasons.append(f"💰 {_usd(usd)}")
    elif usd >= 500_000:
        score += 4

    # ── Тип алерта ─────────────────────────────────────────────────────
    if atype == "AIRDROP_SUMMARY":
        score += 15; reasons.append("📦 Mass distribution detected")
    elif atype == "VOLUME_SPIKE":
        ratio = alert.get("spike_ratio", 0)
        if ratio >= 5:
            score += 12; reasons.append(f"📈 Volume spike {ratio:.1f}x")
        elif ratio >= 3:
            score += 6
    elif atype == "MEME_PUMP":
        pct = alert.get("pct_change", 0)
        if pct >= 500:
            score += 15; reasons.append(f"🔥 {pct:.0f}% pump")
        elif pct >= 200:
            score += 8; reasons.append(f"🚀 {pct:.0f}% pump")
    elif atype == "SOROBAN_NEW_POOL":
        score += 10; reasons.append("🆕 New Soroban pool")
    elif atype in ("SOROBAN_LIQUIDITY", "SOROBAN_SWAP") and usd >= 500_000:
        score += 8; reasons.append("🤖 Soroban DeFi activity")

    # ── Контекст движения ──────────────────────────────────────────────
    move_ctx = alert.get("move_ctx", "")
    if move_ctx == "Exchange → Exchange" and usd >= 1_000_000:
        score += 8; reasons.append("🔀 Exchange to Exchange flow")
    elif move_ctx == "Exchange Outflow" and usd >= 2_000_000:
        score += 6; reasons.append("📤 Large exchange outflow")
    elif move_ctx == "Exchange Inflow" and usd >= 2_000_000:
        score += 6; reasons.append("📥 Large exchange inflow")

    # ── SDEX специфика ─────────────────────────────────────────────────
    if atype in ("SDEX", "SDEX_MULTI"):
        buying = alert.get("buying", "")
        # Необычные пары (не просто XLM/USDC)
        if buying not in ("XLM", "USDC", "USDT", ""):
            score += 3; reasons.append(f"🔄 Unusual pair: {alert.get('pair','')}")

    return score, reasons


def _usd(value: float) -> str:
    if value >= 1_000_000_000: return f"${value/1_000_000_000:.2f}B"
    if value >= 1_000_000:     return f"${value/1_000_000:.2f}M"
    if value >= 1_000:         return f"${value/1_000:.0f}K"
    return f"${value:.0f}"


class TelegramPublisher:
    def __init__(self, config: Config):
        self.config     = config
        self.token      = config.TELEGRAM_BOT_TOKEN
        self.chat_id    = config.TELEGRAM_CHAT_ID
        self.private_id = getattr(config, "TELEGRAM_PRIVATE_ID", "")
        self.msg_times  = deque()
        self.last_msg_time = 0

        self.enabled = bool(
            config.TELEGRAM_ENABLED and self.token and self.chat_id
        )
        self.private_enabled = bool(self.enabled and self.private_id)

        if self.enabled:
            log.info(f"Telegram publisher ready (chat_id: {self.chat_id})")
            if self.private_enabled:
                log.info(f"Private alerts enabled (chat_id: {self.private_id})")
            self._send_startup_message()
        elif config.TELEGRAM_ENABLED:
            log.warning("Telegram enabled but TOKEN or CHAT_ID missing!")

    # ── Публичные методы ─────────────────────────────────────────────────────

    def send_alert(self, text: str, alert: dict = None) -> bool:
        """
        Отправляет алерт в публичный канал.
        Если alert передан и набирает высокий score — дополнительно в приватный.
        """
        if not self.enabled:
            return False

        # Rate limit — количество в час
        now = time.time()
        self.msg_times = deque(t for t in self.msg_times if now - t < 3600)
        if len(self.msg_times) >= self.config.MAX_TG_PER_HOUR:
            log.warning("Telegram hourly limit reached, skipping")
            return False

        # Rate limit — минимальный интервал между сообщениями
        min_interval = getattr(self.config, "MIN_TG_INTERVAL", 5)
        elapsed = now - self.last_msg_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        result = self._send(text, self.chat_id)
        if result:
            self.msg_times.append(time.time())
            self.last_msg_time = time.time()

        # Проверяем нужно ли слать в приватный чат
        if alert and self.private_enabled:
            score, reasons = _interest_score(alert)
            if score >= PRIVATE_SCORE_THRESHOLD:
                self._send_private(text, alert, score, reasons)

        return result

    def send_status(self, text: str):
        if self.enabled:
            self._send(text, self.chat_id)

    def send_error(self, error_text: str):
        if self.enabled:
            self._send(f"⚠️ Error\n\n`{error_text[:500]}`", self.chat_id)

    # ── Приватный чат ────────────────────────────────────────────────────────

    def _send_private(self, alert_text: str, alert: dict,
                      score: int, reasons: list[str]):
        """Отправляет расширенный алерт в приватный чат с аналитикой."""
        atype = alert.get("type", "")
        usd   = alert.get("usd_value", 0) or alert.get("total_usd", 0)
        link  = alert.get("link", "")

        # Строим аналитический блок
        why_lines = "\n".join(f"  · {r}" for r in reasons)

        private_text = (
            f"🔔 *VIP Alert* (score: {score})\n\n"
            f"{alert_text}\n\n"
            f"*Why this matters:*\n{why_lines}"
        )

        if link:
            private_text += f"\n\n🔗 {link}"

        self._send(private_text, self.private_id)
        log.info(f"  ↳ Private alert sent (score={score}, type={atype})")

    # ── Низкоуровневая отправка ──────────────────────────────────────────────

    def _send_startup_message(self):
        """Статус запуска — только в приватный чат."""
        import datetime
        now_str = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
        msg = (
            f"✅ *StellarMonitor запущен*\n"
            f"🕐 {now_str}\n\n"
            f"Пороги:\n"
            f"🐋 XLM >{self.config.WHALE_XLM:,} / USDC >${self.config.WHALE_USDC:,}\n"
            f"📊 SDEX >${self.config.SDEX_MIN_USDC:,}\n\n"
            f"Канал: {self.chat_id}"
        )
        target = self.private_id if self.private_enabled else self.chat_id
        self._send(msg, target)

    def send_shutdown(self, reason: str = ""):
        """Уведомление об остановке — только в приватный чат."""
        import datetime
        now_str = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
        msg = f"🔴 *StellarMonitor остановлен*\n🕐 {now_str}"
        if reason:
            msg += f"\n`{reason[:200]}`"
        target = self.private_id if self.private_enabled else self.chat_id
        if self.enabled:
            self._send(msg, target)

    def send_error(self, error_text: str):
        """Ошибка — только в приватный чат."""
        import datetime
        now_str = datetime.datetime.now().strftime("%H:%M:%S")
        msg = f"⚠️ *Ошибка* [{now_str}]\n\n`{error_text[:500]}`"
        target = self.private_id if self.private_enabled else self.chat_id
        if self.enabled:
            self._send(msg, target)

    def send_status(self, text: str):
        """Статусное сообщение — только в приватный чат."""
        target = self.private_id if self.private_enabled else self.chat_id
        if self.enabled:
            self._send(text, target)

    def _send(self, text: str, chat_id: str, retries: int = 3) -> bool:
        url = TG_API.format(token=self.token, method="sendMessage")
        for attempt in range(retries):
            try:
                resp = requests.post(url, json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                }, timeout=10)
                data = resp.json()
                if data.get("ok"):
                    return True
                err = data.get("description", "Unknown error")
                log.warning(f"Telegram API error: {err}")
                if "parse" in err.lower() or "markdown" in err.lower():
                    resp2 = requests.post(url, json={
                        "chat_id": chat_id,
                        "text": text,
                        "disable_web_page_preview": True,
                    }, timeout=10)
                    return resp2.json().get("ok", False)
                return False
            except requests.exceptions.RequestException as e:
                log.warning(f"Telegram request failed (attempt {attempt+1}): {e}")
                if attempt < retries - 1:
                    time.sleep(3)
        return False
