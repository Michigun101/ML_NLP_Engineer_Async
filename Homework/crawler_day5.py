"""Day 5: Обработка ошибок и автоматические повторы."""

import asyncio
import logging
import random
import time
from urllib.parse import urlparse
import aiohttp   
import json
from crawler_day4 import PoliteCrawler
from crawler_day3 import normalize_url

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

# ============================================================
# ИСКЛЮЧЕНИЯ
# ============================================================


class CrawlerError(Exception):
    """Базовое исключение краулера"""
    pass


class TransientError(CrawlerError):
    """Временная ошибка — можно повторить"""
    pass


class PermanentError(CrawlerError):
    """Постоянная ошибка — не повторять"""
    pass


class NetworkError(CrawlerError):
    """Сетевая ошибка — можно повторить"""
    pass


class ParseError(CrawlerError):
    """Ошибка парсинга — не повторять"""
    pass



class RetryStrategy:
    """Выполнение с повторами"""

    def __init__(
        self,
        max_retries: int = 3, # максимальное количество попыток
        backoff_factor: float = 2.0,
        base_delay: float = 0.5, # базовая задержка для 2 попытки
        jitter: float = 0.3,
        retry_on: list = None,
        per_type_delays: dict = None
    ):
        if max_retries < 1:
            raise ValueError("max_retries должен быть >= 1")
        if backoff_factor < 1:
            raise ValueError("backoff_factor должен быть >= 1")

        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.base_delay = base_delay
        self.jitter = jitter

        if retry_on is None:
            retry_on = [TransientError, NetworkError]
        self.retry_on = tuple(retry_on)  

        self.per_type_delays = per_type_delays or {} 

        self.stats = {
            "attempts": 0,          # всего попыток
            "successes": 0,         # успешных выполнений
            "failures": 0,          # провалившихся
            "retries": 0,           # сколько раз повторяли
            "by_error_type": {},    
            "permanent_urls": [],   # список (url, error) 
            "total_retry_time": 0.0,  # суммарное время в backoff
        }

    async def execute_with_retry(
        self,
        coro,
        *args,
        timeout_factor: float = None,   
        **kwargs,
    ):
        """Выполнить корутину с повторами"""
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            self.stats["attempts"] += 1

            try:
                if timeout_factor is not None:
                    timeout = timeout_factor * attempt
                    logger.debug(
                        "Попытка %d/%d с таймаутом %.1f сек",
                        attempt, self.max_retries, timeout,
                    )
                    result = await asyncio.wait_for(
                        coro(*args, **kwargs),
                        timeout=timeout,
                    )
                else:
                    result = await coro(*args, **kwargs)

                self.stats["successes"] += 1

                if attempt > 1:
                    logger.info(
                        " Успех после %d попыток: %s",
                        attempt, args[0] if args else "?",
                    )

                return result


            except asyncio.TimeoutError as e:
                timeout_err = TransientError(
                    f"Timeout {timeout}s на попытке {attempt}: "
                    f"{args[0] if args else '?'}"
                )
                last_error = timeout_err
                self._record_error(timeout_err)

                if attempt < self.max_retries:
                    base = self.base_delay  
                    delay = base * (self.backoff_factor ** (attempt - 1))
                    delay += random.uniform(0, self.jitter)

                    self.stats["retries"] += 1
                    self.stats["total_retry_time"] += delay

                    logger.info(
                        " Retry %d/%d для %s через %.2f сек (Timeout)",
                        attempt, self.max_retries,
                        args[0] if args else "?",
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    self.stats["failures"] += 1
                    logger.warning(
                        " Все %d попыток исчерпаны: %s",
                        self.max_retries, timeout_err,
                    )
                    raise timeout_err

            except PermanentError as e:
                self.stats["failures"] += 1
                self._record_error(e)
                self.stats["permanent_urls"].append(
                    (args[0] if args else "?", str(e))
                )
                logger.warning(" Permanent error: %s", e)
                raise

            except self.retry_on as e:
                last_error = e
                self._record_error(e)

                if attempt < self.max_retries:
                    base = self.per_type_delays.get(type(e), self.base_delay)
                    delay = base * (self.backoff_factor ** (attempt - 1))
                    delay += random.uniform(0, self.jitter)

                    self.stats["retries"] += 1
                    self.stats["total_retry_time"] += delay

                    logger.info(
                        " Retry %d/%d для %s через %.2f сек (%s)",
                        attempt, self.max_retries,
                        args[0] if args else "?",
                        delay, type(e).__name__,
                    )
                    await asyncio.sleep(delay)
                else:
                    self.stats["failures"] += 1
                    logger.warning(
                        " Все %d попыток исчерпаны: %s",
                        self.max_retries, e,
                    )
                    raise

            except CrawlerError as e:
                self.stats["failures"] += 1
                self._record_error(e)
                raise

        if last_error:
            raise last_error

    def _record_error(self, exc: Exception):
        """Запись ошибки в статистику по типу"""
        name = type(exc).__name__
        self.stats["by_error_type"][name] = (self.stats["by_error_type"].get(name, 0) + 1)

class CircuitBreaker:
    """Circuit breaker для защиты от «мёртвых» доменов"""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
    ):
        if failure_threshold < 1:
            raise ValueError("failure_threshold должен быть >= 1")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout должен быть > 0")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self._failures: dict[str, int] = {}       # domain - счётчик ошибок
        self._opened_at: dict[str, float] = {}    # domain - время открытия

    def is_open(self, domain: str) -> bool:
        """Открыт ли circuit для домена"""
        if domain not in self._opened_at:
            return False

        elapsed = time.time() - self._opened_at[domain]
        if elapsed >= self.recovery_timeout:
            # Время вышло — пробуем снова
            del self._opened_at[domain]
            self._failures.pop(domain, None)
            logger.info(f" Circuit HALF_OPEN для {domain}")
            return False

        return True

    def record_failure(self, domain: str):
        """Фиксация ошибки"""
        self._failures[domain] = self._failures.get(domain, 0) + 1

        if self._failures[domain] >= self.failure_threshold:
            self._opened_at[domain] = time.time()
            logger.warning(f" Circuit OPEN для {domain} ({self._failures[domain]} ошибок подряд)")

    def record_success(self, domain: str):
        """Сбрасываем счетчик ошибок при успехе"""
        if domain in self._failures:
            del self._failures[domain]
        if domain in self._opened_at:
            del self._opened_at[domain]

    def stats(self) -> dict:
        """Статистика circuit breaker"""
        return {
            "domains_with_failures": len(self._failures),
            "domains_open": len(self._opened_at),
            "failures": dict(self._failures),
            "open_until": {
                d: round(t + self.recovery_timeout - time.time(), 2)
                for d, t in self._opened_at.items()
            },
        }    



class ReliableCrawler(PoliteCrawler):
    """Краулер с полной обработкой ошибок"""

    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 2,
        requests_per_second: float = 2.0,
        respect_robots: bool = True,
        user_agent: str = "ReliableBot/1.0",
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        base_delay: float = 0.5,
        circuit_threshold: int = 5,
        circuit_timeout: float = 30.0,
    ):
        super().__init__(
            max_concurrent=max_concurrent,
            max_depth=max_depth,
            requests_per_second=requests_per_second,
            per_domain_limit=True,
            respect_robots=respect_robots,
            min_delay=0.0,
            jitter=0.0,
            user_agent=user_agent,
        )

        self.retry_strategy = RetryStrategy(
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            base_delay=base_delay,
            jitter=0.3,
            per_type_delays={
                TransientError: 0.5,   
                NetworkError: 2.0,     
            }
        )

        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_threshold,
            recovery_timeout=circuit_timeout,
        )

        self._blocked_by_circuit = 0
        self.BASE_TIMEOUT = 30.0

    async def fetch_url(self, url: str) -> str | None:
        """Загрузка с классификацией ошибок по HTTP-кодам"""
        await self._ensure_session()

        async with self._semaphore:
            logger.info(f" Загрузка: {url}", )
            start = time.time()

            try:
                async with self._session.get(url) as response:
                    status = response.status

                    if status == 429:
                        raise TransientError(f"429 Too Many Requests: {url}")
                        
                    if status in (500, 502, 503, 504):
                        raise TransientError(f"{status} Server Error: {url}")
                        
                    if status in (401, 403):
                        raise PermanentError(f"{status} Forbidden/Unauthorized: {url}")
                        
                    if status == 404:
                        raise PermanentError(f"404 Not Found: {url}")
                        
                    # Прочие 4xx/5xx
                    response.raise_for_status()

                    text = await response.text()
                    elapsed = time.time() - start
                    logger.info(" Успех: %s (%.2f сек, %d символов)",url, elapsed, len(text))
                    return text

            except asyncio.TimeoutError as e:
                raise TransientError(f"Timeout: {url}") from e
            except aiohttp.ClientResponseError as e:
                raise PermanentError(f"HTTP {e.status}: {url}") from e
            except aiohttp.ClientError as e:
                raise NetworkError(f"Network: {url} — {e}") from e

    async def _process_one(self, url: str, current_depth: int):
        """Обработка одной страницы"""
        domain = urlparse(url).netloc

        # Проверка circuit breaker
        if self.circuit_breaker.is_open(domain):
            logger.warning(f" Circuit открыт, пропуск: {url}")
            self.queue.mark_failed(url, "circuit breaker open")
            self._blocked_by_circuit += 1
            return

        if self.respect_robots:
            await self.robots_parser.fetch_robots(url)
            if not self.robots_parser.can_fetch(url, self.user_agent):
                logger.info(f" Заблокировано robots.txt: {url}")
                self.queue.mark_failed(url, "blocked by robots.txt")
                self._blocked_by_robots += 1
                return

        await self.rate_limiter.acquire(domain)

        # Загрузка с retry
        start = time.time()

        try:
            async with self.semaphores._global_sem:
                async with self.semaphores.get_domain_semaphore(domain):
                    result = await self.retry_strategy.execute_with_retry(
                        self.fetch_and_parse,
                        url,
                        timeout_factor=self.BASE_TIMEOUT   
                    )

            # Успех — сбрасываем circuit
            self.circuit_breaker.record_success(domain)
            elapsed = time.time() - start
            self._response_times.append(elapsed)

        except PermanentError as e:
            self.circuit_breaker.record_failure(domain)
            self.queue.mark_failed(url, f"PermanentError: {e}")
            logger.warning(f" Permanent: {url}")
            return

        except (TransientError, NetworkError) as e:
            self.circuit_breaker.record_failure(domain)
            self.queue.mark_failed(url, f"All retries failed: {e}")
            logger.warning(f" Все retry исчерпаны: {url}")
            return

        except Exception as e:
            self.circuit_breaker.record_failure(domain)
            self.queue.mark_failed(url, f"Unexpected: {e}")
            logger.exception(f" Неожиданная ошибка: {url}", )
            return

        if result is None:
            self.queue.mark_failed(url, "fetch returned None")
            return

        self.queue.mark_processed(url, result)
        logger.info(
            " [%d] %s — %d ссылок, %.2f сек",
            current_depth, url, result["links_count"],
            time.time() - start,
        )

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
        """Статистика краулера"""
        base = super().stats()

        retry_stats = dict(self.retry_strategy.stats)

        circuit_stats = self.circuit_breaker.stats()

        return {
            **base,
            "blocked_by_circuit": self._blocked_by_circuit,
            "retry": retry_stats,
            "circuit": circuit_stats,
        }              
###########################################     
async def demo():
    """Демонстрация ReliableCrawler."""
    print()
    print("ОБХОД С RETRY И CIRCUIT BREAKER")

    crawler = ReliableCrawler(
        max_concurrent=5,
        max_depth=1,
        requests_per_second=2.0,
        respect_robots=True,
        user_agent="ReliableBot/1.0",
        max_retries=3,
        backoff_factor=2.0,
        base_delay=0.5,
        circuit_threshold=3,
        circuit_timeout=10.0,
    )

    urls = [
        "https://www.python.org",           #  200
        "https://httpbin.org/status/404",    #  PermanentError
        "https://httpbin.org/status/500",    #  TransientError - retry
        "https://httpbin.org/status/503",    #  TransientError - retry
        "https://httpbin.org/status/429",    #  TransientError - retry
    ]

    try:
        results = await crawler.crawl(
            start_urls=urls,
            max_pages=10,
            max_depth=0,        
            same_domain_only=False,
            progress_every=100,
        )
    finally:
        await crawler.close()

    print()
    print(f"РЕЗУЛЬТАТ: обработано {len(results)} страниц")

    for url, data in results.items():
        print(f"\n {url}")
        print(f"   Title: {data['title']}")

    print()
    print("СТАТИСТИКА")

    s = crawler.stats()
    print(f"\n Основное:")
    print(f"   processed:           {s['processed']}")
    print(f"   failed:              {s['failed']}")
    print(f"   blocked_by_robots:   {s['blocked_by_robots']}")
    print(f"   blocked_by_circuit:  {s['blocked_by_circuit']}")
    print(f"   rps:                 {s['requests_per_second']}")

    print(f"\n Retry:")
    for key, value in s["retry"].items():
        if isinstance(value, (int, float)):
            print(f"   {key}: {value}")
    print(f"   by_error_type: {s['retry']['by_error_type']}")
    print(f"   permanent_urls: {len(s['retry']['permanent_urls'])}")
    for url, err in s["retry"]["permanent_urls"][:5]:
        print(f"       {url}: {err[:60]}")

    print(f"\n Circuit breaker:")
    for key, value in s["circuit"].items():
        print(f"   {key}: {value}")

    report = {
        "crawler_stats": crawler.stats(),
        "failed_urls": dict(crawler.queue.failed),
        "permanent_urls": crawler.retry_strategy.stats["permanent_urls"],
        "circuit_stats": crawler.circuit_breaker.stats(),
    }

    with open("error_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"\n Отчёт сохранён в error_report.json")
    

async def test_per_type_backoff():
    """Разные backoff для разных типов ошибок"""
    print()

    print("ТЕСТ: разные backoff для разных типов")

    attempts = []

    async def flaky(url):
        attempts.append(url)
        if len(attempts) < 3:
            if len(attempts) == 1:
                raise TransientError("transient")
            else:
                raise NetworkError("network")
        return "OK"

    strategy = RetryStrategy(
        max_retries=5,
        backoff_factor=2.0,
        jitter=0.0,
        per_type_delays={
            TransientError: 0.5,
            NetworkError: 2.0,
        },
    )

    start = time.time()
    await strategy.execute_with_retry(flaky, "url")
    elapsed = time.time() - start

    print(f"\nПопыток: {len(attempts)}")
    print(f"Время: {elapsed:.2f} сек")
    print(f"Stats: total_retry_time={strategy.stats['total_retry_time']:.2f}")

async def test_retry_strategy():
    """Тест RetryStrategy."""
    print()
    print("ТЕСТ RetryStrategy")

    attempts = {"count": 0}

    async def flaky_function(url: str):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise TransientError(f"Временная ошибка #{attempts['count']}")
        return f"OK: {url}"

    strategy = RetryStrategy(max_retries=5, base_delay=0.3, jitter=0.1)

    start = time.time()
    result = await strategy.execute_with_retry(
        flaky_function, "https://example.com"
    )
    elapsed = time.time() - start

    print(f"\n Результат: {result}")
    print(f"   Попыток: {attempts['count']}")
    print(f"   Время: {elapsed:.2f} сек")
    print(f"   Статистика: {strategy.stats}")


    print("\nPermanent — не повторяем ")
    attempts["count"] = 0

    async def permanent_function(url: str):
        attempts["count"] += 1
        raise PermanentError(f"404 Not Found: {url}")

    strategy2 = RetryStrategy(max_retries=5)
    try:
        await strategy2.execute_with_retry(
            permanent_function, "https://example.com/missing"
        )
    except PermanentError as e:
        print(f" Permanent: {e}")
        print(f"   Попыток: {attempts['count']}")
        print(f"   Статистика: {strategy2.stats}")

async def test_circuit_breaker():
    """Тест CircuitBreaker."""
    print()
    print("ТЕСТ CircuitBreaker")

    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=2.0)

    print(f"\nДо ошибок: is_open('example.com') = {cb.is_open('example.com')}")

    cb.record_failure("example.com")
    cb.record_failure("example.com")
    print(f"После 2 ошибок: is_open = {cb.is_open('example.com')} (должно False)")

    cb.record_failure("example.com")
    print(f"После 3 ошибок: is_open = {cb.is_open('example.com')} (должно True)")

    print(f"\n Ждём {cb.recovery_timeout} сек...")
    await asyncio.sleep(cb.recovery_timeout + 0.1)

    print(f"После таймаута: is_open = {cb.is_open('example.com')} (должно False)")

    print(f"\nStats: {cb.stats()}")

async def test_timeout():
    """Тест повторов при таймауте"""
    print()
    print("ТЕСТ: повторы при таймауте")

    attempts = {"count": 0}

    async def slow_function(url: str):
        attempts["count"] += 1
        if attempts["count"] < 3:
            await asyncio.sleep(10)   
        return "OK"

    strategy = RetryStrategy(max_retries=5, base_delay=0.3, jitter=0.1)

    async def timeout_wrapper(url: str):
        try:
            return await asyncio.wait_for(slow_function(url), timeout=0.5)
        except asyncio.TimeoutError:
            raise TransientError(f"Timeout: {url}")

    start = time.time()
    result = await strategy.execute_with_retry(timeout_wrapper, "url")
    elapsed = time.time() - start

    print(f"\n Результат: {result}")
    print(f"   Попыток: {attempts['count']}")
    print(f"   Время: {elapsed:.2f} сек")
    print(f"   by_error_type: {strategy.stats['by_error_type']}")


async def main():
    await test_retry_strategy()
    await test_per_type_backoff()
    await test_circuit_breaker()
    await test_timeout()
    await demo()


if __name__ == "__main__":
    asyncio.run(main())