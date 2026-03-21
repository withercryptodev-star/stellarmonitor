"""
StellarMonitor — real-time мониторинг через /operations SSE стрим

Ключевое отличие от предыдущей версии:
- Подписываемся на /operations напрямую (не /transactions)
- Каждая операция приходит уже с полными деталями
- Нет дополнительных HTTP запросов — алерт за 1-3 секунды
- Обрабатываем ~150 операций/сек без очереди

Документация Horizon: https://developers.stellar.org/docs/data/horizon/api-reference/resources/operations
"""

import json
import logging
import time
import requests
import httpx
from config import Config

log = logging.getLogger(__name__)

# ─── Известные адреса для красивых лейблов ───────────────────────────────────
KNOWN_ADDRESSES = {
    # ── Circle / USDC ─────────────────────────────────────────────────
    # Circle — официальный эмитент USDC на Stellar
    "GBICPPFMVFLHWBP22NQUQ7LNXJLPFZIWKP7DXIGLBCZLZQPBVQDXQFS": "Circle Issuer",
    # Circle дистрибьютор — раздаёт USDC институциональным клиентам
    "GAVA7FY3KBXJVZDBX254LPM53YXRUEVLM5BXMXZOC7ZIW3HXFP6LT4SR": "Circle Distributor",

    # ── Биржи ─────────────────────────────────────────────────────────
    "GBVUDZFNMQBBKXZ3FGPXFCFG5ZFWDRXCWB7EBJSN4Y5BVMHFEVVVT2": "Binance",
    "GBDEVU63Y6NTHJQQZIKVTC23NWLQKSDWJD7FKXUQNZRM3FEUQQV7HME": "Binance",
    "GCO2IP3MJNUOKS4PUDI4C7LGGMQDJGXG3COYX3WSB4HHNAHKYV5YL3VC": "Kraken",
    "GD5J6HLF5UIUZEFIMZGF755QBWJV7MXFPBXLBPBM6QEFGLS6UVRDKZ":  "Kraken",
    "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN": "Coinbase",
    "GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A": "Bitfinex",
    "GCEZWKCA5VLDNRLN3RPRJMRZOX3Z6G5CHCGK7WI7SJNRH4BNFFT56FKG": "OKX",
    "GCZJM35NKZSETER554YINGH76BMB6XTPG2APKFDJSYQE2QBVHXQTCTM": "Upbit",
    "GDQP2KPQGKIHYJGXNUIYOMHARUARCA7DJT5FO2FFOOKY3B2WSQHG4W37": "Huobi",
    "GB6CKGFALETSK5MZKFVHSSL7OGBWD5IQCFPBP3T6WKZROGB4AJ2W7ZI":  "Kucoin",
    "GCCD6AJOYZCUAQLX32ZJF2MKFFAUJ53POOBUGNLYGUPRFYTQE5SQKBNQ": "Kucoin2",
    "GBTMY37BHEUNXSESORCPQR4BKRW3QABQBLSRLHM62MQLPJGZJ5EJWQP": "Bybit",
    "GCM4PT6XDZBWOOENDS6FVH4GZMEBXJATD3SSKPKFPUDIEFULBKDFOZY":  "Gate.io",
    "GCZJM35NKZSETER554YINGH76BMB6XTPG2APKFDJSYQE2QBVHXQTCTM": "Upbit",

    # ── Stellar Foundation & экосистема ───────────────────────────────
    "GCEZWKCA5VLDNRLN3RPRJMRZOX3Z6G5CHCGK7WI7SJNRH4BNFFT56FKG": "SDF",
    "GCKFBEIYV2U22IO2BJ4KVJOIP7XPWQGQFKKWXR6DOSJBV7STMAQSMTG": "SDF Treasury",
    "GBVTJNZTFEOKTMS636QQTQEDD5FKCPG7LYL7BEXOQP7PNPK3TMTBJED": "Aquarius",
    "GBNZILSTVQZ4R7IKQDGHYGY2QXL5QOFJYQMXPKWRRM5PAV7Y4M67AQUA": "Aquarius AQUA",

    # ── Токенизированные активы ────────────────────────────────────────────────
    # CETES = мексиканские гособлигации на Stellar (WisdomTree/Bitso)
    "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5": "CETES Issuer",


    # ── Маркет-мейкеры & фонды ────────────────────────────────────────
    "GDMXNQBJMS3FYI4PFSYCCB4XODQMNMTKPQ5HIKOUWBOWX2D2YSV3A3OQ": "Market Maker",
    "GBZ35ZJRIKJGYH5PBKLKOZ5L5CXWM4JQCL7QJNMYQC4FJDFPQZUCDPQ": "Interstellar",
    "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN":  "Stellar.org MM",
}

# Адреса бирж — для детекции движения фондов между биржами
EXCHANGE_ADDRESSES = {
    "GBVUDZFNMQBBKXZ3FGPXFCFG5ZFWDRXCWB7EBJSN4Y5BVMHFEVVVT2",
    "GBDEVU63Y6NTHJQQZIKVTC23NWLQKSDWJD7FKXUQNZRM3FEUQQV7HME",
    "GCO2IP3MJNUOKS4PUDI4C7LGGMQDJGXG3COYX3WSB4HHNAHKYV5YL3VC",
    "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
    "GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A",
    "GCEZWKCA5VLDNRLN3RPRJMRZOX3Z6G5CHCGK7WI7SJNRH4BNFFT56FKG",
    "GCZJM35NKZSETER554YINGH76BMB6XTPG2APKFDJSYQE2QBVHXQTCTM",
    "GCCD6AJOYZCUAQLX32ZJF2MKFFAUJ53POOBUGNLYGUPRFYTQE5SQKBNQ",
    "GBTMY37BHEUNXSESORCPQR4BKRW3QABQBLSRLHM62MQLPJGZJ5EJWQP",
    "GCM4PT6XDZBWOOENDS6FVH4GZMEBXJATD3SSKPKFPUDIEFULBKDFOZY",
}

# Адреса маркет-мейкеров — для детекции их активности
MARKET_MAKER_ADDRESSES = {
    "GDMXNQBJMS3FYI4PFSYCCB4XODQMNMTKPQ5HIKOUWBOWX2D2YSV3A3OQ",
    "GBZ35ZJRIKJGYH5PBKLKOZ5L5CXWM4JQCL7QJNMYQC4FJDFPQZUCDPQ",
    "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN",
}

# Фолбек цена если API недоступен
XLM_PRICE_FALLBACK = 0.12


def get_xlm_price() -> float:
    """Получает текущую цену XLM в USD."""
    try:
        r = requests.get(
            "https://api.stellar.expert/explorer/public/asset/XLM",
            timeout=5
        )
        data = r.json()
        price = data.get("price", {})
        if isinstance(price, dict):
            return float(price.get("USD", XLM_PRICE_FALLBACK))
        return float(price) if price else XLM_PRICE_FALLBACK
    except Exception:
        return XLM_PRICE_FALLBACK


def label_address(address: str) -> str:
    """Возвращает лейбл если известен, иначе None."""
    if not address:
        return None
    if address in KNOWN_ADDRESSES:
        return KNOWN_ADDRESSES[address]
    return None  # неизвестный адрес — не показываем


class StellarMonitor:
    def __init__(self, config: Config):
        self.config = config
        self.seen_op_ids = set()
        self.xlm_price = get_xlm_price()
        self.price_updated = time.time()
        # Дедупликация повторяющихся сделок одного размера
        # ключ: (тип, пара, округлённая сумма) → timestamp последнего алерта
        self.recent_alerts: dict = {}
        self.dedup_window = 1200  # 20 минут — убирает повторы
        self.tx_seen: dict = {}
        self.tx_dedup_window = 30  # 30 сек — дедупликация дублей в одной TX
        # Volume spike tracking: pair -> list of (timestamp, usd_value)
        self.pair_volume: dict = {}
        log.info(f"XLM price: ${self.xlm_price:.4f}")

    def _refresh_price(self):
        """Обновляет цену XLM каждые 10 минут."""
        if time.time() - self.price_updated > 600:
            new_price = get_xlm_price()
            if new_price != self.xlm_price:
                log.info(f"XLM price updated: ${new_price:.4f}")
            self.xlm_price = new_price
            self.price_updated = time.time()

    def stream(self):
        """
        Основной генератор — стримит операции напрямую через Horizon SSE.

        Используем /operations?include_failed=false&cursor=now
        Каждая операция содержит все данные сразу — никаких доп. запросов.

        Важно: Horizon шлёт SSE heartbeat каждые ~5-8 сек ("hello").
        Используем requests вместо httpx — он корректно держит SSE соединение
        открытым через stream=True без автозакрытия на heartbeat.
        """
        import requests as req_lib

        url = f"{self.config.HORIZON_URL}/operations"
        params = {
            "order": "asc",
            "cursor": "now",
            "include_failed": "false",
        }
        headers = {
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }

        # Cursor для resume после разрыва — не теряем события
        last_cursor = "now"

        while True:
            try:
                log.info("Connecting to Horizon SSE /operations...")
                params["cursor"] = last_cursor

                resp = req_lib.get(
                    url,
                    params=params,
                    headers=headers,
                    stream=True,
                    timeout=(10, 90),  # (connect, read) — read 90s без данных
                )
                resp.raise_for_status()
                log.info("Horizon /operations SSE connected OK")

                buffer = ""
                for chunk in resp.iter_content(chunk_size=None, decode_unicode=True):
                    if not chunk:
                        continue

                    buffer += chunk

                    # SSE события разделены двойным \n\n
                    while "\n\n" in buffer:
                        event, buffer = buffer.split("\n\n", 1)

                        data_line = None
                        cursor_line = None

                        for line in event.splitlines():
                            if line.startswith("data:"):
                                data_line = line[5:].strip()
                            elif line.startswith("id:"):
                                cursor_line = line[3:].strip()

                        # Обновляем cursor для resume
                        if cursor_line:
                            last_cursor = cursor_line

                        if not data_line or data_line in ('"hello"', '"byebye"', ""):
                            continue

                        try:
                            op = json.loads(data_line)
                        except json.JSONDecodeError:
                            continue

                        # Дедупликация по ID операции
                        op_id = op.get("id", "")
                        if op_id in self.seen_op_ids:
                            continue
                        self.seen_op_ids.add(op_id)

                        # Чистим кэш чтобы не расти бесконечно
                        if len(self.seen_op_ids) > 50000:
                            self.seen_op_ids = set(
                                list(self.seen_op_ids)[-25000:]
                            )

                        self._refresh_price()

                        # Анализируем операцию прямо здесь
                        alert = self._process_operation(op)
                        if alert:
                            yield alert

            except req_lib.exceptions.ConnectionError as e:
                log.warning(f"Connection error: {e}. Retrying in 10s...")
                time.sleep(10)
            except req_lib.exceptions.ReadTimeout:
                log.info("SSE read timeout (90s no data), reconnecting...")
                time.sleep(1)
            except req_lib.exceptions.RequestException as e:
                log.error(f"SSE request error: {e}. Retrying in 10s...")
                time.sleep(10)
            except Exception as e:
                log.error(f"SSE stream error: {e}. Retrying in 10s...")
                time.sleep(10)

    def _is_duplicate(self, alert_key: str) -> bool:
        """Проверяет не было ли такого алерта недавно."""
        now = time.time()
        # Чистим старые записи
        self.recent_alerts = {
            k: v for k, v in self.recent_alerts.items()
            if now - v < self.dedup_window
        }
        if alert_key in self.recent_alerts:
            return True
        self.recent_alerts[alert_key] = now
        return False

    def _process_operation(self, op: dict) -> dict | None:
        """
        Анализирует одну операцию и возвращает алерт если нужно.
        Все данные уже в op — никаких доп. запросов.
        """
        op_type = op.get("type", "")
        tx_hash = op.get("transaction_hash", "")
        link = f"https://stellar.expert/explorer/public/tx/{tx_hash}"

        # Чистим старый TX кэш
        now = time.time()
        self.tx_seen = {k: v for k, v in self.tx_seen.items()
                        if now - v["ts"] < self.tx_dedup_window}

        # ── Payment: прямой перевод XLM или токенов ───────────────────
        if op_type == "payment":
            return self._check_payment(op, link)

        # ── SDEX: сделки на встроенной бирже ─────────────────────────
        elif op_type in (
            "manage_sell_offer",
            "manage_buy_offer",
            "create_passive_sell_offer",
        ):
            return self._check_sdex(op, link)

        # ── Path payment: кросс-цепочечные / кросс-валютные платежи ──
        elif op_type in (
            "path_payment_strict_send",
            "path_payment_strict_receive",
        ):
            return self._check_path_payment(op, link)

        # ── Claimable balance: крупные locked переводы ────────────────
        elif op_type == "create_claimable_balance":
            return self._check_claimable_balance(op, link)

        return None

    # ─── Обработчики по типам операций ───────────────────────────────────────

    def _check_payment(self, op: dict, link: str) -> dict | None:
        """Whale alert для payment операций."""
        asset_type = op.get("asset_type", "")
        asset_code = op.get("asset_code", "")

        try:
            amount = float(op.get("amount", 0))
        except (ValueError, TypeError):
            return None

        sender   = op.get("from", op.get("source_account", ""))
        receiver = op.get("to", "")

        from_label = label_address(sender)
        to_label   = label_address(receiver)

        # Определяем тип движения (биржа ↔ биржа / биржа → неизв. / неизв. → биржа)
        from_ex = sender   in EXCHANGE_ADDRESSES
        to_ex   = receiver in EXCHANGE_ADDRESSES
        if from_ex and to_ex:
            move_ctx = "Exchange → Exchange"
        elif from_ex:
            move_ctx = "Exchange Outflow"
        elif to_ex:
            move_ctx = "Exchange Inflow"
        else:
            move_ctx = None

        # XLM (native)
        if asset_type == "native":
            usd_value = amount * self.xlm_price
            if amount >= self.config.WHALE_XLM:
                # Дедупликация по размеру
                dedup_key = f"pay_xlm_{sender[:8]}_{receiver[:8]}_{round(amount,-4)}"
                if self._is_duplicate(dedup_key):
                    return None
                # Дедупликация XLM+USDC в одной транзакции
                tx_key = op.get("transaction_hash", "")[:16]
                if tx_key and tx_key in self.tx_seen:
                    if "WHALE" in self.tx_seen[tx_key].get("types", []):
                        return None
                if tx_key:
                    self.tx_seen.setdefault(tx_key, {"ts": time.time(), "types": []})
                    self.tx_seen[tx_key]["types"].append("WHALE")
                return {
                    "type": "WHALE_XLM",
                    "amount": amount,
                    "asset": "XLM",
                    "usd_value": usd_value,
                    "from": from_label,
                    "to": to_label,
                    "from_full": sender,
                    "to_full": receiver,
                    "move_ctx": move_ctx,
                    "link": link,
                }

        # USDC / USDT
        elif asset_code in ("USDC", "USDT"):
            if amount >= self.config.WHALE_USDC:
                # Дедупликация XLM+USDC в одной транзакции
                tx_key = op.get("transaction_hash", "")[:16]
                if tx_key and tx_key in self.tx_seen:
                    if "WHALE" in self.tx_seen[tx_key].get("types", []):
                        return None
                if tx_key:
                    self.tx_seen.setdefault(tx_key, {"ts": time.time(), "types": []})
                    self.tx_seen[tx_key]["types"].append("WHALE")
                return {
                    "type": "WHALE_USDC",
                    "amount": amount,
                    "asset": asset_code,
                    "usd_value": amount,
                    "from": from_label,
                    "to": to_label,
                    "from_full": sender,
                    "to_full": receiver,
                    "move_ctx": move_ctx,
                    "link": link,
                }

        return None

    def _check_sdex(self, op: dict, link: str) -> dict | None:
        """
        Алерт для крупных SDEX сделок.

        Считаем USD только через XLM или стейблкоин сторону сделки.
        Это исключает фейковые токены (напр. PENDULUM на SDEX) где
        price завышен и даёт фантомные миллионы долларов.
        """
        try:
            amount = float(op.get("amount", 0))
            price  = float(op.get("price", 0))
        except (ValueError, TypeError):
            return None

        selling_type = op.get("selling_asset_type", "native")
        buying_type  = op.get("buying_asset_type",  "native")
        selling_code = "XLM" if selling_type == "native" else op.get("selling_asset_code", "?")
        buying_code  = "XLM" if buying_type  == "native" else op.get("buying_asset_code",  "?")

        STABLECOINS = {"USDC", "USDT", "AUDD", "EURT", "NGNT"}

        # Считаем USD только когда ПРОДАЁМ XLM или USDC/USDT.
        # Почему только selling сторона?
        # manage_offer — это ордер в стакане, он может висеть неисполненным.
        # Реальный объём = только то что ты реально отдаёшь (selling).
        # Если считать buying XLM — получим фантомные суммы от стоячих ордеров
        # типа "покупаю 7M XLM за 34 мусорных токена" (никогда не исполнится).

        if selling_code == "XLM":
            # Продаём реальный XLM — точный расчёт
            usd_value = amount * self.xlm_price

        elif selling_code in STABLECOINS:
            # Продаём стейблкоин — прямая оценка
            usd_value = amount

        else:
            # Продаём токен — не можем надёжно оценить USD
            return None

        # Фильтр бессмысленных пар (XLM/XLM и т.п.)
        if selling_code == buying_code:
            return None

        if usd_value >= self.config.SDEX_MIN_USDC:
            # Дедупликация: одна и та же пара + округлённый объём → не спамить
            dedup_key = f"sdex_{selling_code}_{buying_code}_{round(amount, -3)}"
            if self._is_duplicate(dedup_key):
                return None

            # Детектируем маркет-мейкера по source_account
            source = op.get("source_account", "")
            is_mm = source in MARKET_MAKER_ADDRESSES

            # tx_hash передаём для TX группировки
            tx_hash = op.get("transaction_hash", "")

            return {
                "type": "SDEX",
                "amount": amount,
                "selling": selling_code,
                "buying": buying_code,
                "price": price,
                "usd_value": usd_value,
                "pair": f"{selling_code}/{buying_code}",
                "is_market_maker": is_mm,
                "tx_hash": tx_hash,
                "link": link,
            }
        return None

    def _check_path_payment(self, op: dict, link: str) -> dict | None:
        """Алерт для крупных path payment (кросс-валютные переводы)."""
        # Whitelist: публикуем только если хотя бы один актив — XLM/USDC/USDT
        KNOWN_ASSETS = {"XLM", "USDC", "USDT"}

        try:
            amount = float(
                op.get("source_amount") or op.get("amount", 0)
            )
        except (ValueError, TypeError):
            return None

        src_type = op.get("source_asset_type", "native")
        dst_type = op.get("asset_type", "native")
        src_code = "XLM" if src_type == "native" else op.get("source_asset_code", "?")
        dst_code = "XLM" if dst_type == "native" else op.get("asset_code", "?")

        # Нормализуем технические суффиксы: USDCAllow → USDC, USDCDebit → USDC
        def _normalize(code: str) -> str:
            for base in ("USDC", "USDT", "XLM", "BTC", "ETH"):
                if code.startswith(base):
                    return base
            return code
        src_code = _normalize(src_code)
        dst_code = _normalize(dst_code)

        # Фильтр мусорных токенов: нужен хотя бы один известный актив
        if src_code not in KNOWN_ASSETS and dst_code not in KNOWN_ASSETS:
            return None

        sender   = op.get("from", "")
        receiver = op.get("to", "")

        # Фильтр self-to-self: конвертация через себя не интересна
        if sender and receiver and sender == receiver:
            return None

        usd_value = amount * self.xlm_price if src_code == "XLM" else amount

        if usd_value >= self.config.CROSS_BORDER_MIN_USDC:
            dedup_key = f"cross_{sender[:8]}_{src_code}_{round(amount, -3)}"
            if self._is_duplicate(dedup_key):
                return None
            return {
                "type": "CROSS_BORDER",
                "amount": amount,
                "src_asset": src_code,
                "dest_asset": dst_code,
                "usd_value": usd_value,
                "from": label_address(sender),
                "to": label_address(receiver),
                "link": link,
            }
        return None

    def _check_claimable_balance(self, op: dict, link: str) -> dict | None:
        """Алерт для крупных claimable balance (locked funds)."""
        asset = op.get("asset", "")
        try:
            amount = float(op.get("amount", 0))
        except (ValueError, TypeError):
            return None

        is_xlm  = asset == "native" or asset == "XLM"
        is_usdc = "USDC" in asset
        is_usdt = "USDT" in asset

        if is_xlm:
            usd_value = amount * self.xlm_price
            threshold = self.config.WHALE_XLM
        elif is_usdc or is_usdt:
            usd_value = amount
            threshold = self.config.WHALE_USDC
        else:
            return None

        if amount >= threshold:
            creator = op.get("source_account", "")
            asset_label = "XLM" if is_xlm else ("USDC" if is_usdc else "USDT")
            return {
                "type": "WHALE_XLM" if is_xlm else "WHALE_USDC",
                "amount": amount,
                "asset": asset_label,
                "usd_value": usd_value,
                "from": label_address(creator),
                "to": "Claimable Balance (locked)",
                "from_full": creator,
                "to_full": "",
                "link": link,
            }
        return None
