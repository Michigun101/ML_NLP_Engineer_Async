import asyncio
import logging
import time

import aiohttp


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

class AsyncCrawler:
    """Асинхронный краулер"""

    def __init__(self, max_concurrent: int = 10):
        if not isinstance(max_concurrent, int) or max_concurrent <= 0:
            raise ValueError(
                f"max_concurrent должен быть положительным целым, "
                f"получено {max_concurrent}"
            )

        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self):
        """Создать сессию, если она ещё не создана."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(
                total=30,        # таймаут на запрос в сек
                connect=10,      # на подключение
                sock_read=10,    # на чтение ответа
            )
            self._session = aiohttp.ClientSession(timeout=timeout)
            logger.debug("Сессия создана")

    async def fetch_url(self, url: str) -> str | None:
        """Загрузить одну страницу"""
        await self._ensure_session()

        async with self._semaphore:
            logger.info(f" Загрузка: {url}")
            start = time.time()

            try:
                async with self._session.get(url) as response:
                    response.raise_for_status() # выдаст исключения, если ошибки 400-е или 500-е
                    text = await response.text()
                    elapsed = time.time() - start
                    logger.info(f" Успех: {url} ({elapsed:.2f} сек, {len(text)} символов)")
                    return text

            except asyncio.TimeoutError:
                logger.warning(f" Таймаут: {url}")
                return None

            except aiohttp.ClientResponseError as e: # 400-е или 500-е
                logger.warning(f" HTTP {e.status}: {url}")
                return None

            except aiohttp.ClientError as e: # остальные
                logger.warning(f" Сеть: {url} — {e}")
                return None

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """Параллельная загрузка URL"""
        if not isinstance(urls, list):
            raise ValueError(f"urls должен быть списком, получено {type(urls).__name__}")

        tasks = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*tasks)

        return {
            url: content for url, content in zip(urls, results) if content is not None
        }

    async def close(self):
        """Закрытие сессии"""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("Сессия закрыта")
            self._session = None


SHARED_URLS = [
    "https://example.com",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/html",
    "https://httpbin.org/json",
]


async def demo():
    """Демонстрация"""

    crawler = AsyncCrawler(max_concurrent=5)
    print()
    print("ПАРАЛЛЕЛЬНАЯ ЗАГРУЗКА")
    print()

    start = time.time()
    results = await crawler.fetch_urls(SHARED_URLS)
    elapsed = time.time() - start

    await crawler.close()

    print()
    print(f" Успешно: {len(results)}/{len(SHARED_URLS)}")
    print(f"  Время: {elapsed:.2f} сек")

async def demo_sequential():
    """Последовательная загрузка"""

    print()
    print("ПОСЛЕДОВАТЕЛЬНАЯ ЗАГРУЗКА")
    print()

    crawler = AsyncCrawler(max_concurrent=5)

    start = time.time()
    results = {}
    for url in SHARED_URLS:
        text = await crawler.fetch_url(url)   #  ждём каждый
        if text is not None:
            results[url] = text
    elapsed = time.time() - start

    await crawler.close()

    print()
    print(f" Успешно: {len(results)}/{len(SHARED_URLS)}")
    print(f"  Время: {elapsed:.2f} сек")


async def main():
    await demo()
    await demo_sequential()

if __name__ == "__main__":
    asyncio.run(main())