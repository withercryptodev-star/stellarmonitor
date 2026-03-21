"""
StellarPulse — Real-time Stellar Network intelligence bot
Мониторит: Whale переводы, SDEX, Soroban события, Meme памп
Публикует алерты в: Telegram (тест) + Twitter/X (продакшн)
"""

import sys
import io
import os
import time
import logging
import threading
import queue

# Fix Windows console Unicode / emoji encoding
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace"
        )
        sys.stderr = io.TextIOWrapper(
            sys.stderr.buffer, encoding="utf-8", errors="replace"
        )
    except Exception:
        pass

from config import Config
from monitor import StellarMonitor
from soroban_monitor import SorobanMonitor
from meme_monitor import MemeTokenMonitor
from publisher import TwitterPublisher
from telegram_publisher import TelegramPublisher
from formatter import format_alert
from weekly_stats import WeeklyStats
from tx_grouper import TxGrouper, MMBlacklist, PairActivityTracker


def setup_logging(config: Config):
    """Настраивает логирование в файл и консоль."""
    level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"

    handlers = [logging.StreamHandler(sys.stdout)]

    # Лог файл
    try:
        handlers.append(logging.FileHandler(config.LOG_FILE, encoding="utf-8"))
    except Exception as e:
        print(f"Warning: cannot open log file {config.LOG_FILE}: {e}")

    logging.basicConfig(level=level, format=fmt, handlers=handlers)
    return logging.getLogger(__name__)


def feed_queue(generator, q: queue.Queue, name: str, log):
    """Запускает монитор в отдельном треде, кладёт алерты в очередь."""
    log.info(f"[{name}] thread started")
    while True:
        try:
            for alert in generator:
                q.put(alert)
        except Exception as e:
            log.error(f"[{name}] crashed: {e}. Restarting in 15s...")
            time.sleep(15)


def print_config(config: Config, log):
    """Выводит текущие настройки при старте."""
    log.info("=" * 55)
    log.info("  StellarPulse - starting")
    log.info("=" * 55)
    log.info(f"  Mode:           DRY_RUN={config.DRY_RUN}")
    log.info(f"  Twitter:        {'ON' if config.TWITTER_ENABLED else 'OFF'}")
    log.info(f"  Telegram:       {'ON' if config.TELEGRAM_ENABLED else 'OFF'}")
    log.info("-" * 55)
    log.info(f"  Whale XLM:      > {config.WHALE_XLM:,} XLM")
    log.info(f"  Whale USDC:     > ${config.WHALE_USDC:,}")
    log.info(f"  SDEX min:       > ${config.SDEX_MIN_USDC:,}")
    log.info(f"  Meme pump 1h:   > {config.MEME_PUMP_1H_PCT:.0f}%")
    log.info(f"  Meme mega 1h:   > {config.MEME_PUMP_MEGA_PCT:.0f}%")
    log.info(f"  Meme min vol:   > ${config.MEME_MIN_VOLUME_24H:,.0f}/day")
    log.info("-" * 55)
    log.info(f"  Max tweets/hr:  {config.MAX_TWEETS_PER_HOUR}")
    log.info(f"  Tweet interval: {config.MIN_TWEET_INTERVAL}s")
    log.info(f"  Max TG/hr:      {config.MAX_TG_PER_HOUR}")
    log.info("=" * 55)


def main():
    # Загружаем конфиг
    config = Config()
    log = setup_logging(config)

    print_config(config, log)

    # Валидируем ключи
    try:
        config.validate()
    except ValueError as e:
        log.error(str(e))
        sys.exit(1)

    # Инициализируем publishers
    twitter = TwitterPublisher(config)
    telegram = TelegramPublisher(config)

    # Инициализируем БД
    try:
        from db import init_db
        init_db()
    except Exception as e:
        log.warning(f"DB init failed (alerts won't be saved): {e}")

    # Еженедельная статистика
    stats = WeeklyStats(config)

    # TX группировка и MM blacklist
    tx_grouper  = TxGrouper(window_sec=5)
    mm_blacklist = MMBlacklist()
    pair_tracker = PairActivityTracker()

    # Background флаш TX группировки каждые 10 сек
    def flush_tx_grouper():
        while True:
            time.sleep(10)
            try:
                flushed = tx_grouper.flush_pending()
                for ready in flushed:
                    alert_queue.put(ready)
            except Exception as e:
                log.error(f"TxGrouper flush error: {e}")

    threading.Thread(target=flush_tx_grouper, daemon=True, name="TxFlush").start()

    # Подключаем callback для постинга отчёта
    def post_weekly_report(text: str):
        if config.TELEGRAM_ENABLED:
            telegram.send_alert(text)
        if config.TWITTER_ENABLED and not config.DRY_RUN:
            twitter.post(text)
        elif config.DRY_RUN:
            print("\n[WeeklyStats DRY RUN]\n" + text)

    stats._post_callback = post_weekly_report

    # Общая очередь для всех мониторов
    alert_queue = queue.Queue()

    # Запускаем 3 монитора в параллельных тредах
    monitors = [
        (StellarMonitor(config).stream,   "Stellar"),
        (SorobanMonitor(config).stream,   "Soroban"),
        (MemeTokenMonitor(config).stream, "MemeTokens"),
    ]

    for stream_fn, name in monitors:
        t = threading.Thread(
            target=feed_queue,
            args=(stream_fn(), alert_queue, name, log),
            daemon=True,
            name=name,
        )
        t.start()

    log.info("All 3 monitors running. Waiting for alerts...")

    # Главный цикл
    while True:
        try:
            alert = alert_queue.get(timeout=60)
            # ── Sender Blacklist (MM + airdrop спам) ──────────────────────
            source_addr = alert.get("from_full", alert.get("source_account", ""))
            atype_check  = alert.get("type", "")

            # Блокируем адрес если он слишком активен
            if mm_blacklist.is_blocked(source_addr):
                log.debug(f"Skipped blacklisted sender {source_addr[:8]}...")
                continue

            # Для WHALE алертов: детектируем airdrop/payroll ботов
            # (один адрес → много разных получателей, одинаковая сумма)
            if atype_check in ("WHALE_USDC", "WHALE_XLM"):
                amount_usd = alert.get("usd_value", 0)
                pair = f"WHALE_{alert.get('asset','?')}"
                # Записываем получателя для summary
                recipient = alert.get("to_full", alert.get("to", ""))
                link = alert.get("link", "")
                mm_blacklist.record_recipient(source_addr, recipient, link)
                newly_blocked = mm_blacklist.record(source_addr, pair, amount_usd)
                if newly_blocked:
                    # Постим один сводный алерт вместо спама
                    summary = mm_blacklist.build_summary(source_addr, pair, amount_usd)
                    if summary:
                        alert_queue.put(summary)
                    log.warning(f"Airdrop/payroll bot blocked: {source_addr[:8]}...")
                    continue

            # Для SDEX: детектируем маркет-мейкеров
            if atype_check == "SDEX":
                pair = alert.get("pair", "")
                amount_usd = alert.get("usd_value", 0)
                newly_blocked = mm_blacklist.record(source_addr, pair, amount_usd)
                if newly_blocked:
                    log.warning(f"New MM blocked: {source_addr[:8]}...")

                # Volume spike детектор
                spike = pair_tracker.record(pair, amount_usd)
                if spike:
                    alert_queue.put(spike)

            # TX группировка для SDEX
            alert.setdefault("tx_hash", "")
            ready_alerts = tx_grouper.add(alert)

            for ready in ready_alerts:
                text = format_alert(ready)
                if not text:
                    continue

                atype = ready.get("type", "?")
                log.info(f"Alert [{atype}] detected")

                # Записываем в недельную статистику
                stats.record(ready)

                # Сохраняем в БД для API и сайта
                try:
                    from db import save_alert
                    save_alert(ready)
                except Exception as _dbe:
                    log.debug(f"DB save skipped: {_dbe}")

                if config.DRY_RUN:
                    print(f"\n[DRY RUN] {atype}\n{text}\n")
                else:
                    # Telegram — полный формат с адресами и ссылками
                    if config.TELEGRAM_ENABLED:
                        from formatter import format_telegram
                        tg_text = format_telegram(ready)
                        if tg_text:
                            telegram.send_alert(tg_text, alert=ready)

                    # Twitter — проверяем TW_ пороги (выше чем TG_)
                    if config.TWITTER_ENABLED:
                        usd = ready.get("usd_value", 0)
                        tw_atype = ready.get("type", "")

                        # Проверяем соответствие TW_ порогам
                        tw_passes = False
                        if tw_atype in ("WHALE_XLM",):
                            tw_passes = ready.get("amount", 0) >= config.TW_WHALE_XLM
                        elif tw_atype in ("WHALE_USDC",):
                            tw_passes = usd >= config.TW_WHALE_USDC
                        elif tw_atype in ("SDEX", "SDEX_MULTI"):
                            tw_passes = usd >= config.TW_SDEX_MIN_USDC
                        elif tw_atype == "CROSS_BORDER":
                            tw_passes = usd >= config.TW_CROSS_BORDER_MIN
                        elif tw_atype == "MEME_PUMP":
                            tw_passes = (
                                ready.get("pct_change", 0) >= config.TW_MEME_PUMP_1H_PCT
                                and ready.get("volume_24h", 0) >= config.TW_MEME_MIN_VOLUME_24H
                            )
                        elif tw_atype in (
                            "AIRDROP_SUMMARY", "VOLUME_SPIKE",
                            "SOROBAN_LIQUIDITY", "SOROBAN_SWAP",
                            "SOROBAN_NEW_POOL",
                        ):
                            tw_passes = True  # эти типы всегда идут в Twitter

                        if not tw_passes:
                            continue  # не проходит TW_ порог — пропускаем Twitter

                        sender = ready.get("from_full", "")
                        not_spammer = not mm_blacklist.is_approaching_limit(sender)

                        if not_spammer or tw_atype == "AIRDROP_SUMMARY":
                            usd_val = ready.get("usd_value", 0)
                            sndr = ready.get("from_full", "")

                            # Mass Transfer: удаляем предыдущие твиты от этого sender
                            if tw_atype == "AIRDROP_SUMMARY":
                                sender_addr = ready.get("sender", sndr)
                                deleted = twitter.delete_by_sender(sender_addr)
                                if deleted:
                                    log.info(f"Consolidated {len(deleted)} tweets → 1 summary")
                                from formatter import format_airdrop_reply
                                reply_text = format_airdrop_reply(ready)
                            else:
                                from formatter import format_reply
                                reply_text = format_reply(ready)

                            twitter.post(text, reply_text=reply_text,
                                        usd_value=usd_val, sender=sndr)


        except queue.Empty:
            log.debug("No alerts in last 60s, still listening...")

        except KeyboardInterrupt:
            log.info("Shutting down StellarMonitor...")
            telegram.send_shutdown("Остановлен вручную (Ctrl+C)")
            break

        except Exception as e:
            log.error(f"Main loop error: {e}")
            telegram.send_error(str(e))
            time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Краш на уровне запуска — пробуем уведомить
        import logging
        logging.error(f"Fatal crash: {e}")
        try:
            from config import Config
            from telegram_publisher import TelegramPublisher
            cfg = Config()
            tg = TelegramPublisher.__new__(TelegramPublisher)
            tg.config = cfg
            tg.token = cfg.TELEGRAM_BOT_TOKEN
            tg.private_id = cfg.TELEGRAM_PRIVATE_ID
            tg.private_enabled = bool(cfg.TELEGRAM_BOT_TOKEN and cfg.TELEGRAM_PRIVATE_ID)
            tg.enabled = bool(cfg.TELEGRAM_BOT_TOKEN and cfg.TELEGRAM_CHAT_ID)
            tg.chat_id = cfg.TELEGRAM_CHAT_ID
            tg.send_shutdown(f"FATAL CRASH: {str(e)[:300]}")
        except Exception:
            pass
        raise
