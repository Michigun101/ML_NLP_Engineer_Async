import aiohttp   
import asyncio
import logging
import random
import time
from urllib.parse import urlparse

from crawler_day3 import AdvancedCrawler


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

def normalize_url(url: str) -> str: # https://python.org и https://python.org/ должны быть одинаковыми url
        """Приводит URL к нормальному виду"""
        parsed = urlparse(url)

        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        if scheme == "http" and netloc.endswith(":80"):
            netloc = netloc[:-3]
        elif scheme == "https" and netloc.endswith(":443"):
            netloc = netloc[:-4]

        path = parsed.path.rstrip("/")

        query = parsed.query

        normalized = parsed._replace(
            scheme=scheme,
            netloc=netloc,
            path=path,
            query=query,
            fragment="",
        )
        return normalized.geturl()

class RateLimiter:
    """Token bucket rate limiter"""
    def __init__(
        self,
        requests_per_second: float = 1.0,
        burst: int = 1,
        per_domain: bool = True,
    ):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second должен быть > 0")
        if not isinstance(burst, int) or burst < 1:
            raise ValueError("burst должен быть целым >= 1")

        self.requests_per_second = float(requests_per_second)
        self.burst = burst
        self.per_domain = per_domain

        # key -> [tokens, last_update]
        self._buckets: dict[str, list[float]] = {}

        # key -> Lock
        self._locks: dict[str, asyncio.Lock] = {}

        # Статистика
        self._waits: list[float] = []     
        self._acquired = 0                

    def _key(self, domain: str | None) -> str:
        if self.per_domain:
            return domain or "__default__"
        return "__global__"

    def _get_lock(self, key: str) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def _get_bucket(self, key: str, now: float) -> list[float]:
        bucket = self._buckets.get(key)
        if bucket is None:
            # стартуем с полным ведром — разрешаем burst на старте
            bucket = [float(self.burst), now]
            self._buckets[key] = bucket
        return bucket

    async def acquire(self, domain: str | None = None) -> float:
        """
        Дождаться разрешения на запрос.
        Возвращает, сколько секунд реально пришлось ждать (0.0 если сразу).
        """
        key = self._key(domain)
        lock = self._get_lock(key)

        async with lock:
            total_wait = 0.0

            while True:
                now = time.time()
                bucket = self._get_bucket(key, now)
                tokens, last_update = bucket

                # Пополняем ведро
                elapsed = now - last_update
                tokens = min(
                    float(self.burst),
                    tokens + elapsed * self.requests_per_second,
                )
                bucket[0] = tokens
                bucket[1] = now

                if tokens >= 1.0:
                    # Токен есть — забираем
                    bucket[0] = tokens - 1.0
                    self._acquired += 1
                    if total_wait > 0:
                        self._waits.append(total_wait)
                    return total_wait

                # Токена нет — считаем, сколько ждать до 1.0
                deficit = 1.0 - tokens
                wait = deficit / self.requests_per_second

                logger.debug(
                    "  Token bucket: ждём %.3f сек для %s (токенов %.3f)",
                    wait, key, tokens,
                )

                await asyncio.sleep(wait)
                total_wait += wait

    def stats(self) -> dict:
        """Статистика по лимитеру."""
        avg_wait = sum(self._waits) / len(self._waits) if self._waits else 0.0
        return {
            "requests_per_second": self.requests_per_second,
            "burst": self.burst,
            "per_domain": self.per_domain,
            "keys_tracked": len(self._buckets),
            "acquired": self._acquired,
            "times_waited": len(self._waits),
            "avg_wait": round(avg_wait, 4),
        }

if __name__ == "__main__":
    async def test_rate_limiter():
        print()
        print("=" * 70)
        print("ТЕСТ RateLimiter")
        print("=" * 70)

        # 2 запроса в секунду
        limiter = RateLimiter(requests_per_second=2.0, per_domain=True)

        print("\n--- 2 RPS, per_domain=True ---")
        start = time.time()

        for i in range(5):
            await limiter.acquire("example.com")
            elapsed = time.time() - start
            print(f"  Запрос {i+1}: t = {elapsed:.2f} сек")

        # Всего ~2 сек (5 запросов / 2 RPS = 2.5 сек, но первый — сразу)
        print(f"\n  Итого: {time.time() - start:.2f} сек")

        # Глобальный режим
        print("\n--- 2 RPS, per_domain=False ---")
        limiter2 = RateLimiter(requests_per_second=2.0, per_domain=False)

        start = time.time()
        for i in range(5):
            await limiter2.acquire(f"domain{i}.com")  # разные домены
            elapsed = time.time() - start
            print(f"  Запрос {i+1}: t = {elapsed:.2f} сек")

        print(f"\n  Итого: {time.time() - start:.2f} сек")
        print("  (глобальный лимит — работает независимо от домена)")


class RobotsParser:
    """Парсер robots.txt"""

    def __init__(self, crawler, user_agent: str = "*"):
        self.crawler = crawler
        self.user_agent = user_agent
        self._cache: dict[str, dict] = {}
        self._lock = asyncio.Lock()

    async def fetch_robots(self, base_url: str) -> dict:
        """Загрузка robots.txt"""
        parsed = urlparse(base_url)
        domain = parsed.netloc
        scheme = parsed.scheme or "https"

        # Быстрая проверка без lock 
        if domain in self._cache:
            return self._cache[domain]

        async with self._lock:
            # Повторная проверка внутри lock — другой воркер мог уже загрузить
            if domain in self._cache:
                return self._cache[domain]

            robots_url = f"{scheme}://{domain}/robots.txt"

            rules = {
                "domain": domain,
                "disallow": [],
                "allow": [],
                "crawl_delay": 0.0,
                "loaded": False,
            }

            try:
                text = await self.crawler.fetch_url(robots_url)
            except Exception as e:
                logger.info(
                    f" robots.txt недоступен: {domain} "
                    f"({type(e).__name__}) — всё разрешено"
                )
                self._cache[domain] = rules
                return rules

            if text:
                rules = self._parse(text, domain)
                rules["loaded"] = True
                logger.info(f" Загружен robots.txt: {domain}")
            else:
                logger.info(f" robots.txt пуст: {domain} (всё разрешено)")

            self._cache[domain] = rules
            return rules

    def _parse(self, text: str, domain: str) -> dict:
        """Распарсить содержимое robots.txt."""
        blocks: dict[str, dict] = {}   
        current_agent = None

        for raw_line in text.splitlines():
            # Убираем комментарии
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                current_agent = None
                continue

            if ":" not in line:
                continue

            key, _, value = line.partition(":")
            key = key.strip().lower()
            value = value.strip()

            if key == "user-agent":
                current_agent = value.lower()
                if current_agent not in blocks:
                    blocks[current_agent] = {
                        "disallow": [],
                        "allow": [],
                        "crawl_delay": 0.0,
                    }
            elif key == "disallow" and current_agent and value:
                blocks[current_agent]["disallow"].append(value)
            elif key == "allow" and current_agent and value:
                blocks[current_agent]["allow"].append(value)
            elif key == "crawl-delay" and current_agent:
                try:
                    blocks[current_agent]["crawl_delay"] = float(value)
                except ValueError:
                    pass

        wildcard = blocks.get("*", {"disallow": [], "allow": [], "crawl_delay": 0.0})
        specific = blocks.get(self.user_agent.lower())

        rules = {
            "domain": domain,
            "disallow": list(wildcard["disallow"]),
            "allow": list(wildcard["allow"]),
            "crawl_delay": wildcard["crawl_delay"],
            "loaded": True,
        }

        if specific:
            rules["disallow"] = specific["disallow"]
            rules["allow"] = specific["allow"]
            rules["crawl_delay"] = specific["crawl_delay"]

        return rules

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Проверка разрешения"""
        parsed = urlparse(url)
        domain = parsed.netloc

        if domain not in self._cache:
            return True   # ещё не загрузили — по умолчанию разрешаем

        rules = self._cache[domain]
        path = parsed.path or "/"

        best_disallow = None
        best_allow = None

        for pattern in rules["disallow"]:
            if path.startswith(pattern):
                if best_disallow is None or len(pattern) > len(best_disallow):
                    best_disallow = pattern

        for pattern in rules["allow"]:
            if path.startswith(pattern):
                if best_allow is None or len(pattern) > len(best_allow):
                    best_allow = pattern

        if best_disallow is None:
            return True

        if best_allow is not None and len(best_allow) >= len(best_disallow):
            return True

        return False

    def get_crawl_delay(self, url: str) -> float:
        """Получение задержки из robots.txt"""
        domain = urlparse(url).netloc
        if domain in self._cache:
            return self._cache[domain]["crawl_delay"]
        return 0.0

    def stats(self) -> dict:
        """Статистика по загруженным robots.txt."""
        return {
            "domains_cached": len(self._cache),
            "loaded": sum(1 for r in self._cache.values() if r["loaded"]),
        }

class PoliteCrawler(AdvancedCrawler):
    """Краулер, соблюдающий правила вежливости"""

    MAX_RETRIES = 3
    BASE_BACKOFF = 0.5   # сек

    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 2,
        requests_per_second: float = 1.0,
        burst: int = 1,
        per_domain_limit: bool = True,
        respect_robots: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        user_agent: str = "PoliteBot/1.0",
    ):
        super().__init__(
            max_concurrent=max_concurrent,
            max_depth=max_depth,
            global_limit=max_concurrent,
            per_domain_limit=5,
        )

        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            burst=burst,
            per_domain=per_domain_limit,
        )

        self.user_agent = user_agent

        self.respect_robots = respect_robots
        self.robots_parser = RobotsParser(self, user_agent=user_agent) if respect_robots else None

        self.min_delay = min_delay
        self.jitter = jitter

        self._blocked_by_robots = 0
        self._response_times: list[float] = []   
        self._rate_limited = 0                   

    async def _fetch_with_backoff(self, url: str) -> dict | None:
        """Загрузка с экспоненциальным backoff при ошибках"""
        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                result = await self.fetch_and_parse(url)
                if result is not None:
                    return result
                last_error = "empty result"
            except Exception as e:
                last_error = e

            if attempt < self.MAX_RETRIES:
                delay = self.BASE_BACKOFF * (2 ** (attempt - 1))
                delay += random.uniform(0, 0.3)   
                logger.info(
                    " Retry %d/%d для %s через %.2f сек",
                    attempt, self.MAX_RETRIES, url, delay,
                )
                await asyncio.sleep(delay)

        if isinstance(last_error, Exception):
            raise last_error
        return None
    
    async def _ensure_session(self):
        """Сессии с User-Agent"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(
                total=30,
                connect=10,
                sock_read=10,
            )
            headers = {"User-Agent": self.user_agent}
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
            )
            logger.debug(f"Сессия создана с UA: {self.user_agent}")

    async def _process_one(self, url: str, current_depth: int):
        """Обработать страницу с соблюдением вежливости"""
        domain = urlparse(url).netloc

        if self.respect_robots:
            await self.robots_parser.fetch_robots(url)
            if not self.robots_parser.can_fetch(url, self.user_agent):
                logger.info(f" Заблокировано robots.txt: {url}")
                self.queue.mark_failed(url, "blocked by robots.txt")
                self._blocked_by_robots += 1
                return

            robots_delay = self.robots_parser.get_crawl_delay(url)
            if robots_delay > 0:
                logger.debug(f" Crawl-delay={robots_delay} для {domain}")
                await asyncio.sleep(robots_delay)

        waited = await self.rate_limiter.acquire(domain)
        if waited > 0.01:
            self._rate_limited += 1

        #  Доп задержка 
        if self.min_delay > 0:
            extra = self.min_delay + random.uniform(0, self.jitter)
            await asyncio.sleep(extra)

        fetch_start = time.time()

        try:
            async with self.semaphores._global_sem:
                async with self.semaphores.get_domain_semaphore(domain):
                    result = await self._fetch_with_backoff(url)
        except Exception as e:
            logger.warning(f" Ошибка загрузки {url}: {e}")
            self.queue.mark_failed(url, str(e))
            return

        fetch_elapsed = time.time() - fetch_start
        self._response_times.append(fetch_elapsed)

        if result is None:
            self.queue.mark_failed(url, "fetch returned None")
            return

        self.queue.mark_processed(url, result)
        logger.info(f"good [{current_depth}] {url} — {result['links_count']} ссылок, {fetch_elapsed:.2f} сек")

        next_depth = current_depth + 1
        if next_depth > self.max_depth:
            return

        for link in result["links"]:
            norm = normalize_url(link)

            if (norm in self.queue.processed
                    or norm in self.queue.failed
                    or self.depth.get(norm, self.max_depth + 1) <= next_depth):
                continue

            if self._same_domain_only and not self._same_domain(norm):
                continue

            if not self._should_visit(norm):
                continue

            if self.queue.add_url(norm, priority=0):
                self.depth[norm] = next_depth

    def stats(self) -> dict:
        """Статистика по краулеру"""

        avg_response = (
            sum(self._response_times) / len(self._response_times)
            if self._response_times
            else 0.0
        )

        # Скорость
        elapsed = time.time() - self._start_time if self._start_time else 0
        processed = len(self.queue.processed)
        rps = processed / elapsed if elapsed > 0 else 0.0

        robots_stats = self.robots_parser.stats() if self.robots_parser else {}

        return {
            "processed": processed,
            "failed": len(self.queue.failed),
            "in_queue": self.queue.size(),
            "blocked_by_robots": self._blocked_by_robots,
            "rate_limiter": self.rate_limiter.stats(),
            "avg_response_time": round(avg_response, 3),
            "requests_per_second": round(rps, 2),
            "elapsed": round(elapsed, 2),
            "robots": robots_stats,
        }

    def log_stats(self):
        s = self.stats()
        logger.info(
            " processed=%d, failed=%d, in_queue=%d, "
            "blocked=%d, rate_limited=%d, avg=%.2fs, rps=%.2f",
            s["processed"], s["failed"], s["in_queue"],
            s["blocked_by_robots"], s["rate_limited_times"],
            s["avg_response_time"], s["requests_per_second"],
        )

async def demo():
    """Демонстрация"""
    print()
    print("ВЕЖЛИВЫЙ ОБХОД")


    crawler = PoliteCrawler(
        max_concurrent=5,
        max_depth=1,
        requests_per_second=2.0,      
        per_domain_limit=True,
        respect_robots=True,
        min_delay=0.2,                
        jitter=0.3,                   
        user_agent="MyPoliteBot/1.0",
    )

    try:
        results = await crawler.crawl(
            start_urls=["https://www.python.org"],
            max_pages=8,
            max_depth=1,
            same_domain_only=True,
            progress_every=3,
        )
    finally:
        await crawler.close()

    print()
    print(f"РЕЗУЛЬТАТ: обработано {len(results)} страниц")
 
    for url, data in list(results.items())[:10]:
        print(f"\n {url}")
        print(f"   Title: {data['title']}")
        print(f"   URLs: {data['links_count']}")

    print()
    print("СТАТИСТИКА")
    s = crawler.stats()
    for key, value in s.items():
        print(f"  {key}: {value}")    

async def test_rate_limiter_domains():
    """RateLimiter: разные домены."""
    print()
    print("ТЕСТ RateLimiter — РАЗНЫЕ ДОМЕНЫ")

    limiter = RateLimiter(requests_per_second=2.0, per_domain=True)
    start = time.time()
    for i in range(3):
        await limiter.acquire(f"domain{i}.com")
    elapsed1 = time.time() - start
    print(f"\nper_domain=True: {elapsed1:.2f} сек ")

    # per_domain=False — ждём глобально
    limiter2 = RateLimiter(requests_per_second=2.0, per_domain=False)
    start = time.time()
    for i in range(3):
        await limiter2.acquire(f"domain{i}.com")
    elapsed2 = time.time() - start
    print(f"\nper_domain=False: {elapsed2:.2f} сек")

async def test_blocked():
    """Тест блокировки robots.txt."""
    print()
    print("ТЕСТ БЛОКИРОВКИ ROBOTS.TXT")

    crawler = PoliteCrawler(
        max_depth=0,
        respect_robots=True,
        user_agent="TestBot/1.0",
    )
    try:
        results = await crawler.crawl(
            start_urls=[
                "https://www.python.org",
                "https://www.python.org/webstats/",   
            ],
            max_pages=5,
            max_depth=0,
            same_domain_only=True,
            progress_every=100,
        )
    finally:
        await crawler.close()

    print(f"\n Обработано: {len(results)}")
    print(f" Заблокировано: {crawler._blocked_by_robots}")
    print(f" Упало: {len(crawler.queue.failed)}")

    for url, reason in crawler.queue.failed.items():
        print(f"   {url}: {reason}")    

async def test_robots():
    """Тест RobotsParser"""
    print()
    print("ТЕСТ RobotsParser")

    crawler = PoliteCrawler(respect_robots=True, user_agent="test-bot")
    parser = crawler.robots_parser

    rules = await parser.fetch_robots("https://www.python.org")
    print(f"\n rules: {rules}")

    for url in [
        "https://www.python.org/",
        "https://www.python.org/about/",
        "https://www.python.org/jobs/",
    ]:
        result = parser.can_fetch(url)
        icon = "correct" if result else "incorrect"
        print(f"  {icon} {url}")

    await crawler.close()

async def test_rate_limiter_burst():
    print()
    print("ТЕСТ RateLimiter — BURST")

    limiter = RateLimiter(requests_per_second=2.0, burst=3, per_domain=True)

    timestamps = []
    async def task(i):
        await limiter.acquire("example.com")
        timestamps.append(time.time())

    start = time.time()
    await asyncio.gather(*[task(i) for i in range(6)])
    elapsed = time.time() - start

    timestamps.sort()
    intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps) - 1)]

    print(f"  6 запросов, burst=3, rate=2 RPS")
    print(f"  Всего: {elapsed:.2f} сек")
    print(f"  Интервалы: {[round(x, 2) for x in intervals]}")

    assert 1.2 <= elapsed <= 2.0, f"Ожидалось 1.5, получено {elapsed:.2f}"
    print(" Burst работает, далее — ограничение по rate")    

async def test_rate_limiter_concurrent():
    print()
    print("ТЕСТ RateLimiter — КОНКУРЕНТНОСТЬ")

    limiter = RateLimiter(requests_per_second=5.0, burst=1, per_domain=True)

    timestamps = []
    async def task(i):
        await limiter.acquire("example.com")
        timestamps.append(time.time())

    start = time.time()
    await asyncio.gather(*[task(i) for i in range(10)])
    elapsed = time.time() - start

    timestamps.sort()
    intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps) - 1)]
    min_interval = min(intervals) if intervals else 0

    print(f"  10 запросов, burst=1, rate=5 RPS")
    print(f"  Всего: {elapsed:.2f} сек)")
    print(f"  Минимальный интервал: {min_interval:.3f} сек")

    assert min_interval >= 0.15, f"Есть пачка: min interval = {min_interval:.3f}"
    print(" Пачек нет, интервалы соблюдаются")

async def main():
    await test_rate_limiter()
    await test_rate_limiter_domains()
    await test_robots()
    await test_blocked()
    await test_rate_limiter_burst()
    await test_rate_limiter_concurrent()
    await demo()


if __name__ == "__main__":
    asyncio.run(main())