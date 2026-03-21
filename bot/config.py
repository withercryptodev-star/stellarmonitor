"""
Config — загружает настройки из settings.env
Раздельные пороги для Telegram и Twitter.
"""

import os
from dotenv import load_dotenv

load_dotenv("settings.env", override=True)


class Config:

    # ── Twitter API ───────────────────────────────────────────────────
    TWITTER_API_KEY       = os.getenv("TWITTER_API_KEY", "")
    TWITTER_API_SECRET    = os.getenv("TWITTER_API_SECRET", "")
    TWITTER_ACCESS_TOKEN  = os.getenv("TWITTER_ACCESS_TOKEN", "")
    TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET", "")
    TWITTER_BEARER_TOKEN  = os.getenv("TWITTER_BEARER_TOKEN", "")

    # ── Telegram ──────────────────────────────────────────────────────
    TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
    TELEGRAM_PRIVATE_ID = os.getenv("TELEGRAM_PRIVATE_ID", "")

    # ── Режимы работы ─────────────────────────────────────────────────
    DRY_RUN          = os.getenv("DRY_RUN",          "true").lower()  == "true"
    TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower()  == "true"
    TWITTER_ENABLED  = os.getenv("TWITTER_ENABLED",  "false").lower() == "true"

    # ── Stellar ───────────────────────────────────────────────────────
    HORIZON_URL = os.getenv("HORIZON_URL", "https://horizon.stellar.org")

    # ══════════════════════════════════════════════════════════════════
    #  ПОРОГИ TELEGRAM (ниже — больше алертов в канал)
    # ══════════════════════════════════════════════════════════════════
    TG_WHALE_XLM           = int(os.getenv("TG_WHALE_XLM",           "500000"))
    TG_WHALE_USDC          = int(os.getenv("TG_WHALE_USDC",          "50000"))
    TG_SDEX_MIN_USDC       = int(os.getenv("TG_SDEX_MIN_USDC",       "75000"))
    TG_CROSS_BORDER_MIN    = int(os.getenv("TG_CROSS_BORDER_MIN",    "200000"))
    TG_MEME_PUMP_1H_PCT    = float(os.getenv("TG_MEME_PUMP_1H_PCT",  "50"))
    TG_MEME_PUMP_MEGA_PCT  = float(os.getenv("TG_MEME_PUMP_MEGA_PCT","300"))
    TG_MEME_MIN_VOLUME_24H = float(os.getenv("TG_MEME_MIN_VOLUME_24H","500"))

    # ══════════════════════════════════════════════════════════════════
    #  ПОРОГИ TWITTER (выше — только самые крупные)
    # ══════════════════════════════════════════════════════════════════
    TW_WHALE_XLM           = int(os.getenv("TW_WHALE_XLM",           "1000000"))
    TW_WHALE_USDC          = int(os.getenv("TW_WHALE_USDC",          "200000"))
    TW_SDEX_MIN_USDC       = int(os.getenv("TW_SDEX_MIN_USDC",       "200000"))
    TW_CROSS_BORDER_MIN    = int(os.getenv("TW_CROSS_BORDER_MIN",    "500000"))
    TW_MEME_PUMP_1H_PCT    = float(os.getenv("TW_MEME_PUMP_1H_PCT",  "100"))
    TW_MEME_PUMP_MEGA_PCT  = float(os.getenv("TW_MEME_PUMP_MEGA_PCT","500"))
    TW_MEME_MIN_VOLUME_24H = float(os.getenv("TW_MEME_MIN_VOLUME_24H","1000"))

    # Обратная совместимость — monitor.py использует эти имена для SSE фильтрации
    # Ставим минимальные (TG) чтобы ничего не пропустить на уровне стрима
    @property
    def WHALE_XLM(self):           return self.TG_WHALE_XLM
    @property
    def WHALE_USDC(self):          return self.TG_WHALE_USDC
    @property
    def SDEX_MIN_USDC(self):       return self.TG_SDEX_MIN_USDC
    @property
    def CROSS_BORDER_MIN_USDC(self): return self.TG_CROSS_BORDER_MIN
    @property
    def MEME_PUMP_1H_PCT(self):    return self.TG_MEME_PUMP_1H_PCT
    @property
    def MEME_PUMP_MEGA_PCT(self):  return self.TG_MEME_PUMP_MEGA_PCT
    @property
    def MEME_MIN_VOLUME_24H(self): return self.TG_MEME_MIN_VOLUME_24H

    # ── Rate limiting Twitter ─────────────────────────────────────────
    MAX_TWEETS_PER_HOUR = int(os.getenv("MAX_TWEETS_PER_HOUR", "5"))
    MIN_TWEET_INTERVAL  = int(os.getenv("MIN_TWEET_INTERVAL",  "720"))

    # ── Rate limiting Telegram ────────────────────────────────────────
    MAX_TG_PER_HOUR = int(os.getenv("MAX_TG_PER_HOUR", "120"))
    MIN_TG_INTERVAL = int(os.getenv("MIN_TG_INTERVAL", "5"))

    # ── Логирование ───────────────────────────────────────────────────
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE  = os.getenv("LOG_FILE",  "stellarmonitor.log")

    def validate(self):
        errors = []
        if self.TWITTER_ENABLED and not self.DRY_RUN:
            for k in ["TWITTER_API_KEY", "TWITTER_API_SECRET",
                      "TWITTER_ACCESS_TOKEN", "TWITTER_ACCESS_SECRET"]:
                if not getattr(self, k):
                    errors.append(f"  - {k} не заполнен")
        if self.TELEGRAM_ENABLED:
            if not self.TELEGRAM_BOT_TOKEN:
                errors.append("  - TELEGRAM_BOT_TOKEN не заполнен")
            if not self.TELEGRAM_CHAT_ID:
                errors.append("  - TELEGRAM_CHAT_ID не заполнен")
        if errors:
            raise ValueError("Ошибки в settings.env:\n" + "\n".join(errors))
