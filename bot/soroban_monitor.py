"""
SorobanMonitor — мониторинг Soroban смарт-контрактов
Использует официальный Stellar Python SDK и правильный API

Документация: https://developers.stellar.org/docs/data/apis/rpc/api-reference/methods/getEvents
SDK: https://stellar-sdk.readthedocs.io
"""

import time
import logging
from stellar_sdk import SorobanServer
from stellar_sdk.soroban_rpc import EventFilter, EventFilterType
from config import Config

log = logging.getLogger(__name__)

# ─── Публичные RPC эндпоинты (несколько для fallback) ────────────────────────
# Источник: https://developers.stellar.org/docs/data/apis/rpc/providers
RPC_ENDPOINTS = [
    "https://soroban-rpc.mainnet.stellar.gateway.fm",  # Gateway.fm
    "https://mainnet.sorobanrpc.com",                   # Quasar
]

# ─── Известные контракты Soroban на mainnet ───────────────────────────────────
# Aquarius: https://docs.aqua.network/developers/code-examples/prerequisites-and-basics
# Blend: https://docs.blend.capital
KNOWN_CONTRACTS = {
    "CBQDHNBFBZYE4MKPWBSJOPIYLW4SFSXAXUTSXJN76GNKYVYPCKWC6QUK": "Aquarius AMM",
    "CAGJZSZ4A7RQKAVUBVSPJTCJPNQ3Y4M7NB3LNFUOUQXAZLPTJL37TOF": "Soroswap",
    "CDVQVKOY2YSXS2IC7KN6MNASSHPAO7UN2UR2ON4OI2SKMFJNVAMDX6DP": "Blend Protocol",
}

# ─── Топик фильтры для DeFi событий ──────────────────────────────────────────
# Топики кодируются в base64 XDR. "*" = любое значение (wildcard)
# Источник: https://developers.stellar.org/docs/data/apis/rpc/api-reference/methods/getEvents
DEFI_TOPIC_FILTERS = [
    # Любые события от известных контрактов
    ["*", "*", "*", "*"],
]


def _contract_label(contract_id: str) -> str:
    return KNOWN_CONTRACTS.get(contract_id, f"{contract_id[:6]}...{contract_id[-4:]}")


def _parse_amount(value) -> float | None:
    """Пытается извлечь сумму из XDR value."""
    try:
        if hasattr(value, 'i128'):
            # i128 — стандартный тип для сумм в Soroban
            hi = value.i128.hi
            lo = value.i128.lo
            amount = (hi << 64) + lo
            return float(amount) / 1e7  # stroops → XLM
        elif hasattr(value, 'u128'):
            hi = value.u128.hi
            lo = value.u128.lo
            return float((hi << 64) + lo) / 1e7
        elif hasattr(value, 'i64'):
            return float(value.i64) / 1e7
        elif hasattr(value, 'u64'):
            return float(value.u64) / 1e7
    except Exception:
        pass
    return None


def _detect_event_type(topics: list) -> str:
    """Определяет тип события по топикам."""
    topic_strs = []
    for t in topics:
        try:
            if hasattr(t, 'sym'):
                topic_strs.append(str(t.sym).lower())
            elif hasattr(t, 'str'):
                topic_strs.append(str(t.str).lower())
            else:
                topic_strs.append(str(t).lower())
        except Exception:
            topic_strs.append("")

    joined = " ".join(topic_strs)

    if any(k in joined for k in ["deposit", "provide", "add_liquidity", "mint"]):
        return "LIQUIDITY_DEPOSIT"
    elif any(k in joined for k in ["withdraw", "remove_liquidity", "burn"]):
        return "LIQUIDITY_WITHDRAW"
    elif any(k in joined for k in ["swap", "exchange", "trade"]):
        return "SWAP"
    elif any(k in joined for k in ["create_pool", "initialize", "new_pool", "init"]):
        return "NEW_POOL"
    elif any(k in joined for k in ["transfer", "send"]):
        return "TRANSFER"
    else:
        return "UNKNOWN"


class SorobanMonitor:
    def __init__(self, config: Config):
        self.config = config
        self.server = None
        self.current_rpc_idx = 0
        self.last_ledger = None
        self.seen_event_ids = set()
        # Дедупликация N/A событий: contract+action → timestamp последнего алерта
        self._na_alert_times: dict = {}
        self._na_dedup_window = 7200  # 2 часа — не спамим одним и тем же N/A
        self._connect()

    def _connect(self):
        """Подключается к Soroban RPC, пробует все эндпоинты по очереди."""
        for i, endpoint in enumerate(RPC_ENDPOINTS):
            try:
                server = SorobanServer(endpoint)
                # Проверяем соединение
                health = server.get_health()
                latest = server.get_latest_ledger()
                self.server = server
                self.current_rpc_idx = i
                self.last_ledger = latest.sequence - 2  # стартуем с -2 чтобы не пропустить
                log.info(
                    f"Soroban RPC connected: {endpoint} "
                    f"(ledger={latest.sequence}, status={health.status})"
                )
                return
            except Exception as e:
                log.warning(f"Soroban RPC {endpoint} unavailable: {e}")

        log.error("All Soroban RPC endpoints failed. Soroban monitoring disabled.")
        self.server = None

    def _reconnect(self):
        """Пробует следующий эндпоинт при ошибке."""
        self.current_rpc_idx = (self.current_rpc_idx + 1) % len(RPC_ENDPOINTS)
        endpoint = RPC_ENDPOINTS[self.current_rpc_idx]
        log.info(f"Trying next Soroban RPC: {endpoint}")
        try:
            self.server = SorobanServer(endpoint)
            health = self.server.get_health()
            log.info(f"Soroban RPC reconnected: {endpoint} (status={health.status})")
        except Exception as e:
            log.warning(f"Reconnect failed: {e}")
            self.server = None

    def stream(self):
        """Генератор Soroban событий."""
        if not self.server:
            log.warning("Soroban monitor not connected, skipping...")
            return

        while True:
            try:
                alerts = self._poll_events()
                for alert in alerts:
                    yield alert

                # Soroban ledger каждые ~5 сек, поллим каждые 10
                time.sleep(10)

            except Exception as e:
                log.error(f"Soroban monitor error: {e}")
                self._reconnect()
                time.sleep(20)

    def _poll_events(self) -> list:
        """Запрашивает новые события из Soroban RPC."""
        if not self.server:
            return []

        try:
            latest = self.server.get_latest_ledger()
            current_ledger = latest.sequence

            if current_ledger <= self.last_ledger:
                return []

            start = self.last_ledger + 1

            # Запрашиваем ВСЕ контрактные события без фильтра по contract_ids
            # RPC API отвергает фильтрацию по нескольким контрактам —
            # фильтруем сами в _process_events()
            filters = [
                EventFilter(
                    event_type=EventFilterType.CONTRACT,
                )
            ]

            response = self.server.get_events(
                start_ledger=start,
                filters=filters,
                limit=100,
            )

            self.last_ledger = current_ledger
            return self._process_events(response.events)

        except Exception as e:
            err_str = str(e)
            if "startLedger" in err_str or "not found" in err_str.lower():
                try:
                    latest = self.server.get_latest_ledger()
                    self.last_ledger = latest.sequence
                    log.debug("Soroban: reset to latest ledger")
                except Exception:
                    pass
                return []
            raise

    def _process_events(self, events: list) -> list:
        """Обрабатывает список событий и возвращает алерты."""
        alerts = []

        for event in events:
            event_id = event.id
            if event_id in self.seen_event_ids:
                continue
            self.seen_event_ids.add(event_id)

            # Чистим кэш
            if len(self.seen_event_ids) > 5000:
                self.seen_event_ids = set(list(self.seen_event_ids)[-2500:])

            contract_id = event.contract_id or ""
            contract_name = _contract_label(contract_id)
            tx_hash = event.transaction_hash or ""
            link = f"https://stellar.expert/explorer/public/tx/{tx_hash}"

            # Определяем тип события
            event_type = _detect_event_type(event.topic)

            # Пытаемся извлечь сумму из value
            amount_usd = None
            if event.value:
                raw_amount = _parse_amount(event.value)
                if raw_amount:
                    # Конвертируем в USD (примерно)
                    amount_usd = raw_amount * 0.12  # XLM price

            # Фильтруем по минимальной сумме
            min_usd = self.config.SDEX_MIN_USDC * 2  # порог для Soroban

            if event_type in ("LIQUIDITY_DEPOSIT", "LIQUIDITY_WITHDRAW"):
                if amount_usd and amount_usd >= min_usd:
                    alerts.append({
                        "type": "SOROBAN_LIQUIDITY",
                        "action": "Deposit" if event_type == "LIQUIDITY_DEPOSIT" else "Withdraw",
                        "contract": contract_name,
                        "amount_usd": amount_usd,
                        "link": link,
                    })
                elif not amount_usd and contract_name != contract_id:
                    # Известный контракт но сумму не смогли распарсить.
                    # N/A события не публикуем — они не несут информации.
                    # Просто логируем для отладки.
                    log.debug(f"Soroban N/A event skipped: {contract_name} {event_type}")
                    continue

            elif event_type == "SWAP":
                if amount_usd and amount_usd >= self.config.SDEX_MIN_USDC:
                    alerts.append({
                        "type": "SOROBAN_SWAP",
                        "contract": contract_name,
                        "amount_usd": amount_usd,
                        "link": link,
                    })

            elif event_type == "NEW_POOL":
                # Дедупликация: один и тот же контракт не чаще раз в 24ч
                dedup_key = f"new_pool:{contract_name}"
                now_ts = time.time()
                last_sent = self._na_alert_times.get(dedup_key, 0)
                if now_ts - last_sent < 86400:  # 24 часа
                    continue
                self._na_alert_times[dedup_key] = now_ts
                alerts.append({
                    "type": "SOROBAN_NEW_POOL",
                    "contract": contract_name,
                    "link": link,
                })

        return alerts
