"""
TwitterPublisher — постит твиты через API v2

Фичи:
- Rate limiting (MAX_TWEETS_PER_HOUR, MIN_TWEET_INTERVAL)
- Дедупликация по контенту
- Reply со ссылкой на транзакцию
- Smart replace: при Mass Transfer удаляет предыдущие твиты по этому sender
- Priority replace: при очень крупной сделке удаляет самый мелкий твит
"""

import time
import logging
import hashlib
from collections import deque
from config import Config

log = logging.getLogger(__name__)

# Порог для priority replace — сделка крупнее этого вытесняет мелкий твит
PRIORITY_THRESHOLD = 5_000_000  # $5M+


class TwitterPublisher:
    def __init__(self, config: Config):
        self.config = config
        self.tweet_times  = deque()   # (timestamp, tweet_id, usd_value)
        self.posted_hashes = set()
        self.last_tweet_time = 0

        # История твитов за час: list of {id, usd, ts, sender}
        self.recent_tweets: list = []

        if config.DRY_RUN:
            log.info("🔇 DRY RUN mode — tweets will be printed, not posted")
            self.client = None
        else:
            config.validate()
            import tweepy
            self.client = tweepy.Client(
                consumer_key=config.TWITTER_API_KEY,
                consumer_secret=config.TWITTER_API_SECRET,
                access_token=config.TWITTER_ACCESS_TOKEN,
                access_token_secret=config.TWITTER_ACCESS_SECRET,
            )
            log.info("✅ Twitter client initialized")

    # ── Публичный API ────────────────────────────────────────────────────────

    def post(self, text: str, reply_text: str = None,
             usd_value: float = 0, sender: str = "") -> bool:
        """
        Постит твит. Возвращает True если успешно.
        usd_value — объём сделки для priority replace логики.
        sender    — адрес отправителя для Mass Transfer логики.
        """
        # Дедупликация
        content_hash = hashlib.md5(text.encode()).hexdigest()
        if content_hash in self.posted_hashes:
            log.debug("Duplicate tweet skipped")
            return False
        self.posted_hashes.add(content_hash)
        if len(self.posted_hashes) > 1000:
            self.posted_hashes = set(list(self.posted_hashes)[-500:])

        # Минимальный интервал
        now = time.time()
        elapsed = now - self.last_tweet_time
        if elapsed < self.config.MIN_TWEET_INTERVAL:
            wait = self.config.MIN_TWEET_INTERVAL - elapsed
            log.info(f"⏳ Rate limit: waiting {wait:.0f}s before next tweet")
            time.sleep(wait)

        # Чистим историю старше 1 часа
        now = time.time()
        self.recent_tweets = [t for t in self.recent_tweets
                               if now - t["ts"] < 3600]
        hour_count = len(self.recent_tweets)

        # Лимит в час достигнут — проверяем priority replace
        if hour_count >= self.config.MAX_TWEETS_PER_HOUR:
            replaced = self._try_priority_replace(usd_value)
            if not replaced:
                oldest_ts = self.recent_tweets[0]["ts"] if self.recent_tweets else now
                wait = 3600 - (now - oldest_ts)
                log.warning(f"⏳ Hourly limit reached. Waiting {wait/60:.1f}min")
                time.sleep(wait + 5)

        # DRY RUN
        if self.config.DRY_RUN:
            print("\n" + "═" * 50)
            print("📤 [DRY RUN] Would tweet:")
            print(text)
            if reply_text:
                print(f"  ↳ Reply: {reply_text}")
            print("═" * 50)
            self._track(tweet_id="dry_" + str(int(time.time())),
                        usd=usd_value, sender=sender)
            return True

        # Реальная публикация
        try:
            response = self.client.create_tweet(text=text)
            if response.data:
                tweet_id = response.data["id"]
                log.info(f"✅ Tweet posted: https://twitter.com/i/web/status/{tweet_id}")

                reply_id = None
                if reply_text:
                    try:
                        reply_resp = self.client.create_tweet(
                            text=reply_text,
                            in_reply_to_tweet_id=tweet_id
                        )
                        if reply_resp.data:
                            reply_id = reply_resp.data["id"]
                        log.info("  ↳ Reply with link posted")
                    except Exception as re:
                        log.warning(f"Reply failed: {re}")

                self._track(tweet_id=tweet_id, usd=usd_value,
                           sender=sender, reply_id=reply_id)
                return True

        except Exception as e:
            log.error(f"❌ Twitter API error: {e}")
            if "429" in str(e) or "rate limit" in str(e).lower():
                log.warning("Twitter rate limit hit, sleeping 15 min")
                time.sleep(900)

        return False

    def delete_by_sender(self, sender: str) -> list[str]:
        """
        Удаляет все твиты И их reply от указанного sender за последний час.
        """
        if not sender or self.config.DRY_RUN:
            return []

        deleted = []
        to_keep = []
        for t in self.recent_tweets:
            if t.get("sender") == sender and t.get("tweet_id"):
                # Сначала удаляем reply
                if t.get("reply_id"):
                    try:
                        self.client.delete_tweet(id=t["reply_id"])
                        log.info(f"🗑️ Deleted reply {t['reply_id']}")
                    except Exception as e:
                        log.warning(f"Failed to delete reply {t['reply_id']}: {e}")
                # Затем основной твит
                try:
                    self.client.delete_tweet(id=t["tweet_id"])
                    deleted.append(t["tweet_id"])
                    log.info(f"🗑️ Deleted tweet {t['tweet_id']} (sender: {sender[:8]}...)")
                except Exception as e:
                    log.warning(f"Failed to delete {t['tweet_id']}: {e}")
                    to_keep.append(t)
            else:
                to_keep.append(t)

        self.recent_tweets = to_keep
        if deleted:
            log.info(f"Freed {len(deleted)} slots after mass transfer consolidation")
        return deleted

    # ── Приватные методы ────────────────────────────────────────────────────

    def _track(self, tweet_id: str, usd: float, sender: str, reply_id: str = None):
        """Записывает твит в историю."""
        self.recent_tweets.append({
            "tweet_id": tweet_id,
            "reply_id": reply_id,
            "usd": usd,
            "sender": sender,
            "ts": time.time(),
        })
        self.last_tweet_time = time.time()

    def _try_priority_replace(self, new_usd: float) -> bool:
        """
        Если новая сделка >= PRIORITY_THRESHOLD и есть твит с меньшим объёмом —
        удаляем его (и его reply) и возвращаем True.
        """
        if new_usd < PRIORITY_THRESHOLD:
            return False

        candidates = [t for t in self.recent_tweets
                      if t.get("usd", 0) < new_usd and t.get("tweet_id")]
        if not candidates:
            return False

        weakest = min(candidates, key=lambda t: t["usd"])

        if self.config.DRY_RUN:
            log.info(f"[DRY RUN] Would delete weak tweet (${weakest['usd']:,.0f}) "
                     f"for priority tweet (${new_usd:,.0f})")
            self.recent_tweets = [t for t in self.recent_tweets if t != weakest]
            return True

        try:
            # Удаляем reply если есть
            if weakest.get("reply_id"):
                try:
                    self.client.delete_tweet(id=weakest["reply_id"])
                    log.info(f"🗑️ Priority replace: deleted reply {weakest['reply_id']}")
                except Exception as e:
                    log.warning(f"Failed to delete reply on priority replace: {e}")
            # Удаляем основной твит
            self.client.delete_tweet(id=weakest["tweet_id"])
            self.recent_tweets = [t for t in self.recent_tweets if t != weakest]
            log.info(f"🗑️ Priority replace: deleted ${weakest['usd']:,.0f} tweet "
                     f"for ${new_usd:,.0f} tweet")
            return True
        except Exception as e:
            log.warning(f"Priority replace failed: {e}")
            return False
