"""
TxGrouper — умная группировка алертов из одной транзакции
MMBlacklist — автоматический blacklist маркет-мейкеров
PairActivityTracker — детектор аномальной активности пар
"""

import time
import logging
from collections import defaultdict

log = logging.getLogger(__name__)


# ─── TX Grouper ───────────────────────────────────────────────────────────────

class TxGrouper:
    """
    Группирует несколько SDEX алертов из одной TX в один сводный.
    
    Пример: TX bde6b868 генерирует XLM/SHX + XLM/VELO + XLM/XFF
    Вместо 3 отдельных постов → один:
    
    📊 Multi-pair TX — Stellar DEX
    🔄 XLM/SHX  ~$73K
    🔄 XLM/VELO ~$34K  
    🔄 XLM/XFF  ~$34K
    💰 Total: ~$141K
    """

    def __init__(self, window_sec: int = 3):
        # tx_hash -> {"ts": float, "alerts": list, "link": str}
        self.pending: dict = {}
        self.window = window_sec
        self.emitted: set = set()

    def add(self, alert: dict) -> list[dict]:
        """
        Добавляет алерт. Возвращает список алертов готовых к публикации.
        Для одиночных TX — возвращает сразу.
        Для TX с несколькими операциями — буферизует и возвращает сводный.
        """
        tx = alert.get("tx_hash", "")

        if not tx or alert.get("type") not in ("SDEX",):
            # Не SDEX или нет TX hash — публикуем сразу без группировки
            return [alert]

        now = time.time()

        if tx not in self.pending:
            self.pending[tx] = {"ts": now, "alerts": [], "link": alert.get("link", "")}

        self.pending[tx]["alerts"].append(alert)

        # Флашим устаревшие TX (старше window секунд, но не текущую!)
        ready = []
        expired = [h for h, v in self.pending.items()
                   if h != tx  # не флашим TX которую сейчас заполняем
                   and now - v["ts"] > self.window
                   and h not in self.emitted]

        for h in expired:
            grouped = self._build_group(h)
            if grouped:
                ready.append(grouped)
            self.emitted.add(h)
            del self.pending[h]

        # Чистим старые emitted
        if len(self.emitted) > 500:
            self.emitted = set()

        return ready

    def flush_pending(self) -> list[dict]:
        """Флашит TX старше window_sec. Вызывается из background треда каждые 10 сек."""
        now = time.time()
        ready = []
        expired = [h for h, v in list(self.pending.items())
                   if now - v["ts"] > self.window
                   and h not in self.emitted]
        for h in expired:
            grouped = self._build_group(h)
            if grouped:
                ready.append(grouped)
            self.emitted.add(h)
            del self.pending[h]
        return ready

    def flush_all(self) -> list[dict]:
        """Принудительный флаш всех pending TX (при остановке)."""
        ready = []
        for h in list(self.pending.keys()):
            if h not in self.emitted:
                grouped = self._build_group(h)
                if grouped:
                    ready.append(grouped)
                self.emitted.add(h)
        self.pending.clear()
        return ready

    def _build_group(self, tx_hash: str) -> dict | None:
        """Строит сводный алерт из нескольких SDEX операций одной TX."""
        data = self.pending.get(tx_hash)
        if not data:
            return None

        alerts = data["alerts"]
        if not alerts:
            return None

        if len(alerts) == 1:
            # Одиночная операция — без изменений
            return alerts[0]

        # Несколько операций — строим сводный
        total_usd = sum(a.get("usd_value", 0) for a in alerts)
        pairs = [(a.get("pair", "?"), a.get("usd_value", 0)) for a in alerts]
        # Сортируем по объёму убывающе
        pairs.sort(key=lambda x: x[1], reverse=True)

        return {
            "type": "SDEX_MULTI",
            "pairs": pairs,
            "usd_value": total_usd,
            "tx_hash": tx_hash,
            "link": data["link"],
            "count": len(alerts),
        }


# ─── MM Blacklist ─────────────────────────────────────────────────────────────

class MMBlacklist:
    """
    Автоматически детектирует и блокирует маркет-мейкеров.
    
    Логика: если один адрес делает одинаковую сделку (пара + ±10% объём)
    более 3 раз за 30 минут — это MM, добавляем в blacklist на 2 часа.
    """

    def __init__(self):
        # address -> list of (timestamp, pair, amount)
        self.history: dict = defaultdict(list)
        # address -> blacklisted_until timestamp
        self.blacklist: dict = {}
        self.window = 1800      # 30 минут наблюдения
        self.threshold = 3      # 3 одинаковые сделки → blacklist (MM/SDEX)
        self.whale_threshold = 4  # 4 whale переводов одного размера → airdrop бот
        self.ban_duration = 7200  # 2 часа бана
        # Храним получателей для каждого sender
        self._recipient_history: dict = {}

    def record_recipient(self, sender: str, recipient: str, link: str = ""):
        """Записывает получателя для последующего отображения в summary."""
        if not sender or not recipient:
            return
        if sender not in self._recipient_history:
            self._recipient_history[sender] = []
        self._recipient_history[sender].append({
            "address": recipient,
            "link": link,
        })
        # Не хранить больше 50 получателей
        if len(self._recipient_history[sender]) > 50:
            self._recipient_history[sender] = self._recipient_history[sender][-50:]

    def is_blocked(self, address: str) -> bool:
        """Проверяет заблокирован ли адрес."""
        if not address:
            return False
        until = self.blacklist.get(address, 0)
        if time.time() < until:
            return True
        elif until > 0:
            # Бан истёк
            del self.blacklist[address]
        return False

    def record(self, address: str, pair: str, amount: float) -> bool:
        """
        Записывает активность адреса.
        Возвращает True если адрес только что добавлен в blacklist.
        """
        if not address:
            return False

        now = time.time()
        history = self.history[address]

        # Чистим старые записи
        history[:] = [(ts, p, a) for ts, p, a in history if now - ts < self.window]

        # Добавляем текущую запись
        history.append((now, pair, amount))

        # Считаем похожие сделки (одна пара + ±20% объём)
        similar = sum(
            1 for _, p, a in history
            if p == pair and abs(a - amount) / max(amount, 1) < 0.2
        )

        # Whale/airdrop ботам достаточно 4 повторов (они шлют каждые 1-2 мин)
        thresh = self.whale_threshold if pair.startswith("WHALE_") else self.threshold
        if similar >= thresh and address not in self.blacklist:
            self.blacklist[address] = now + self.ban_duration
            log.warning(f"Sender blacklisted: {address[:8]}... ({pair} x{similar} times)")
            # Сбрасываем recipient_history — новый кластер будет чистым
            self._recipient_history.pop(address, None)
            return True

        return False

    def is_approaching_limit(self, address: str) -> bool:
        """
        Возвращает True если адрес уже сделал 2+ одинаковых перевода
        (т.е. скоро будет заблокирован как airdrop бот).
        Используется чтобы не постить в Twitter промежуточные алерты.
        """
        if not address:
            return False
        if self.is_blocked(address):
            return True
        history = self.history.get(address, [])
        now = time.time()
        recent = [(ts, p, a) for ts, p, a in history if now - ts < self.window]
        if len(recent) >= 2:
            # Проверяем есть ли повторяющиеся суммы
            from collections import Counter
            pairs = [(p, round(a, -3)) for _, p, a in recent]
            counts = Counter(pairs)
            return max(counts.values()) >= 2
        return False

    def get_stats(self) -> dict:
        """Статистика для логов."""
        active = sum(1 for until in self.blacklist.values() if time.time() < until)
        return {"blacklisted": active, "tracked": len(self.history)}

    def build_summary(self, address: str, pair: str, amount: float) -> dict | None:
        """Строит сводный алерт когда адрес заблокирован как airdrop/payroll бот."""
        history = self.history.get(address, [])
        now = time.time()
        recent = [(ts, p, a) for ts, p, a in history if now - ts < self.window]
        if not recent:
            return None

        # Считаем только похожие транзакции (одинаковая пара + ±20% сумма)
        similar = [(ts, p, a) for ts, p, a in recent
                   if p == pair and abs(a - amount) / max(amount, 1) < 0.2]
        count = max(len(similar), len(recent))
        # Total = сумма реальных похожих транзакций
        total = sum(a for _, _, a in similar) if similar else amount * count

        # Получаем список получателей
        recipients = getattr(self, '_recipient_history', {}).get(address, [])

        return {
            "type": "AIRDROP_SUMMARY",
            "sender": address,
            "pair": pair,
            "count": count,
            "total_usd": total,
            "per_tx_usd": amount,
            "recipients": recipients[-20:],
        }


# ─── Pair Activity Tracker ────────────────────────────────────────────────────

class PairActivityTracker:
    """
    Отслеживает объём пар и детектирует аномальный рост.
    
    Если объём пары за последние 10 минут > 3x средней за прошлый час
    → публикует Volume Spike алерт.
    """

    def __init__(self):
        # pair -> list of (timestamp, usd_value)
        self.volumes: dict = defaultdict(list)
        self.spike_cooldown: dict = {}  # pair -> last spike alert timestamp
        self.spike_window = 3600       # 1 час для spike cooldown

    def record(self, pair: str, usd_value: float) -> dict | None:
        """
        Записывает сделку. Возвращает spike alert если обнаружен аномальный рост.
        """
        now = time.time()
        self.volumes[pair].append((now, usd_value))

        # Чистим данные старше 2 часов
        self.volumes[pair] = [(ts, v) for ts, v in self.volumes[pair]
                               if now - ts < 7200]

        # Нужно минимум 5 минут данных для анализа
        if len(self.volumes[pair]) < 5:
            return None

        # Объём за последние 10 минут
        recent = sum(v for ts, v in self.volumes[pair] if now - ts < 600)
        # Объём за предыдущие 50 минут (10-60 мин назад)
        baseline_period = [(ts, v) for ts, v in self.volumes[pair]
                           if 600 < now - ts < 3600]

        if len(baseline_period) < 3:
            return None

        baseline_per_10min = sum(v for _, v in baseline_period) / (len(baseline_period) / 6 + 1)

        if baseline_per_10min < 10000:
            return None  # слишком мало данных для сравнения

        spike_ratio = recent / baseline_per_10min if baseline_per_10min > 0 else 0

        # Спайк: текущий объём в 3x выше обычного
        if spike_ratio >= 3.0:
            last_spike = self.spike_cooldown.get(pair, 0)
            if now - last_spike > self.spike_window:
                self.spike_cooldown[pair] = now
                log.info(f"Volume spike: {pair} x{spike_ratio:.1f} (${recent/1000:.0f}K in 10min)")
                return {
                    "type": "VOLUME_SPIKE",
                    "pair": pair,
                    "usd_10min": recent,
                    "spike_ratio": spike_ratio,
                    "baseline": baseline_per_10min,
                }

        return None

    def top_pairs(self, n: int = 3, window_sec: int = 3600) -> list[tuple]:
        """Возвращает топ N пар по объёму за window_sec секунд."""
        now = time.time()
        totals = {}
        for pair, records in self.volumes.items():
            vol = sum(v for ts, v in records if now - ts < window_sec)
            if vol > 0:
                totals[pair] = vol
        return sorted(totals.items(), key=lambda x: x[1], reverse=True)[:n]
