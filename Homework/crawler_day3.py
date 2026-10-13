import asyncio
import logging
import time
from collections import defaultdict
from urllib.parse import urlparse

from crawler_day2 import CrawlerWithParser


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
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

class CrawlerQueue:
    '''Очередь URL с приоритетами.'''

    def __init__(self):
        # {priority: [url]}
        self._queues: dict[int, list[str]] = defaultdict(list)

        self._queued: set[str] = set()     # очередь url
        self.processed: dict[str, dict] = {}  
        self.failed: dict[str, str] = {}      
        self.visited_urls: set[str] = set()
        self._total_added = 0

    def size(self) -> int:
        '''Общее количество URL в очереди'''
        return sum(len(q) for q in self._queues.values())

    def is_empty(self) -> bool:
        return self.size() == 0

    def add_url(self, url: str, priority: int = 0) -> bool:
        '''Добавление URL с приоритетом'''
        url = normalize_url(url) 

        if url in self.visited_urls:   
            return False

        self._queues[priority].append(url)
        self._queued.add(url)
        self.visited_urls.add(url)
        self._total_added += 1
        return True

    async def get_next(self) -> str | None:
        '''Получение следующего URL'''
        if self.is_empty():
            return None

        max_priority = max(self._queues.keys())

        url = self._queues[max_priority].pop(0)

        if not self._queues[max_priority]:
            del self._queues[max_priority]

        self._queued.discard(url)

        return url

    def mark_processed(self, url: str, result: dict):
        '''Пометка URL как обработанного'''
        url = normalize_url(url)
        self.processed[url] = result
        self._queued.discard(url)   

    def mark_failed(self, url: str, error, status_code: int | None = None,
                error_type: str | None = None):
        url = normalize_url(url)
        if isinstance(error, Exception):
            info = {
                "message": str(error),
                "error_type": error_type or type(error).__name__,
                "status_code": status_code if status_code is not None else getattr(error, "status_code", None),
            }
        else:
            info = {
                "message": str(error),
                "error_type": error_type or "UnknownError",
                "status_code": status_code,
            }
        self.failed[url] = info
        self._queued.discard(url)

    def get_stats(self) -> dict:
        '''Статистика очереди'''
        return {
            'in_queue': self.size(),
            'processed': len(self.processed),
            'failed': len(self.failed),
            'total_added': self._total_added,
            'by_priority': {p: len(q) for p, q in self._queues.items()},
        }

class SemaphoreManager:
    '''Управление семафорами'''

    def __init__(self, global_limit: int = 10, per_domain_limit: int = 5):
        if global_limit <= 0 or per_domain_limit <= 0:
            raise ValueError('Лимиты должны быть положительными')

        self.global_limit = global_limit
        self.per_domain_limit = per_domain_limit

        self._global_sem = asyncio.Semaphore(global_limit)
        self._domain_sems: dict[str, asyncio.Semaphore] = {}

    def get_domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        '''Получение или создание семафора для домена'''
        if domain not in self._domain_sems:
            self._domain_sems[domain] = asyncio.Semaphore(self.per_domain_limit)
        return self._domain_sems[domain]

    def stats(self) -> dict:
        '''Статистика менеджера'''
        return {
            'global_limit': self.global_limit,
            'per_domain_limit': self.per_domain_limit,
            'domains_tracked': len(self._domain_sems),
        }

class AdvancedCrawler(CrawlerWithParser):
    '''Обогащение класса'''

    def __init__(self, max_concurrent=10, max_depth=2,
                 global_limit=10, per_domain_limit=5):
        super().__init__(max_concurrent)
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.queue = CrawlerQueue()
        self.semaphores = SemaphoreManager(global_limit, per_domain_limit)

        self.depth: dict[str, int] = {}         # url -> глубина
        self.base_domains: set[str] = set()     # домены стартовых URL
        self._start_time = 0.0                  

        self._same_domain_only = False
        self._exclude_patterns: list[str] = []
        self._include_patterns: list[str] = []

    async def crawl(self, start_urls: list[str], max_pages: int = 100,
                max_depth: int = None,
                same_domain_only: bool = False,
                exclude_patterns: list[str] = None,
                include_patterns: list[str] = None,
                progress_every: int = 5) -> dict[str, dict]:
        
        if not isinstance(start_urls, list) or not start_urls:
            raise ValueError('start_urls должен быть непустым списком')

        if max_depth is not None:
            self.max_depth = max_depth
        self._same_domain_only = same_domain_only
        self._exclude_patterns = exclude_patterns or []
        self._include_patterns = include_patterns or []
        self.max_pages = max_pages

        self.base_domains = { urlparse(url).netloc for url in start_urls }

        # Стартовые URL идут в  очередь
        for url in start_urls:
            if self.queue.add_url(url, priority=10):
                self.depth[url] = 0

        self._start_time = time.time()

        logger.info(f' Старт: {len(start_urls)} URL, max_pages={max_pages}, '
                    f'max_depth={self.max_depth}, same_domain={same_domain_only}')

        processed_count = 0
        counter_lock = asyncio.Lock()

        async def worker(worker_id: int):
            nonlocal processed_count
            idle_rounds = 0

            while True:
                # Проверка лимита + резервирование слота — атомарно
                async with counter_lock:
                    if processed_count >= max_pages:
                        return
                    processed_count += 1        
                    my_slot = processed_count

                url = await self.queue.get_next()

                if url is None:
                    # нечего обрабатывать, возвращаем слот обратно
                    async with counter_lock:
                        processed_count -= 1
                    idle_rounds += 1
                    if idle_rounds > 50:
                        return
                    await asyncio.sleep(0.1)
                    continue

                idle_rounds = 0

                if url in self.queue.processed:
                    async with counter_lock:
                        processed_count -= 1    # слот не нужен
                    continue

                current_depth = self.depth.get(url, 0)
                if current_depth > self.max_depth:
                    async with counter_lock:
                        processed_count -= 1
                    continue

                is_start = current_depth == 0
                if not is_start and not self._should_visit(url):
                    async with counter_lock:
                        processed_count -= 1
                    continue

                # Обрабатываем — слот уже зарезервирован
                try:
                    await self._process_one(url, current_depth)
                except Exception as e:
                    logger.exception(f" Необработанная ошибка в _process_one({url}): {e}")
                    self.queue.mark_failed(url, f"unhandled: {e}")

                # Прогресс
                if my_slot % progress_every == 0:
                    self._log_progress(my_slot)

        workers = [
            asyncio.create_task(worker(i))
            for i in range(self.max_concurrent)
        ]
        await asyncio.gather(*workers)

        return self.queue.processed 

    async def _process_one(self, url: str, current_depth: int):
        '''Обработка одной страницы'''
        domain = urlparse(url).netloc

        # пробуем с двойным семафором
        try:
            async with self.semaphores._global_sem:
                async with self.semaphores.get_domain_semaphore(domain):
                    result = await self.fetch_and_parse(url)
        except Exception as e:
            logger.warning(f' Ошибка загрузки {url}: {e}')
            self.queue.mark_failed(url, str(e))
            return

        if result is None:
            self.queue.mark_failed(url, 'fetch returned None')
            return

        self.queue.mark_processed(url, result)
        logger.info(f" [{current_depth}] {url} — {result['links_count']} ссылок, {result['text_length']} символов")

        # Добавляем найденные ссылки
        next_depth = current_depth + 1
        if next_depth > self.max_depth:
            return  

        for link in result['links']:
            norm = normalize_url(link)
            if (norm in self.queue.processed
                    or norm in self.queue.failed
                    or self.depth.get(norm, self.max_depth + 1) <= next_depth):
                continue

            # корректность домена
            if self._same_domain_only and not self._same_domain(norm):
                continue

            if not self._should_visit(norm):
                continue

            # Добавляем в очередь если прошел все проверки
            if self.queue.add_url(norm, priority=0):
                self.depth[norm] = next_depth


    def _same_domain(self, url: str) -> bool:
        return urlparse(url).netloc in self.base_domains

    def _should_visit(self, url: str) -> bool:
        '''Фильтры URL'''

        if self._same_domain_only and not self._same_domain(url):
            return False

        if self._exclude_patterns:
            for pattern in self._exclude_patterns:
                if pattern in url:
                    return False

        if self._include_patterns:
            if not any(p in url for p in self._include_patterns):
                return False

        return True
    
    def _log_progress(self, processed: int):
        '''Лог прогресса'''
        elapsed = time.time() - self._start_time
        speed = processed / elapsed if elapsed > 0 else 0
        stats = self.queue.get_stats()

        remaining = self.max_pages - processed
        eta = remaining / speed if speed > 0 else 0

        logger.info(
            " %d обработано, в очереди: %d, ошибок: %d, %.2f стр/сек, ETA: %.1f сек",
            processed, stats["in_queue"], stats["failed"], speed, eta,
        )


async def demo():
    """Демонстрация"""
    print()
    
    print("BFS-ОБХОД САЙТА")

    crawler = AdvancedCrawler(
        max_concurrent=10,
        max_depth=1,
        global_limit=10,
        per_domain_limit=5,
    )

    try:
        results = await crawler.crawl(
            start_urls=["https://www.python.org"],
            max_pages=10,
            max_depth=1,
            same_domain_only=True,
            exclude_patterns=["/logout", "/signup"],
            progress_every=3,
        )
    finally:
        await crawler.close()

    print()
    print(f"РЕЗУЛЬТАТ: обработано {len(results)} страниц")

    for url, data in list(results.items())[:10]:
        depth = crawler.depth.get(url, "?")
        print(f"\n[{depth}] {url}")
        print(f"     Title: {data['title']}")
        print(f"     Ссылок: {data['links_count']}, текста: {data['text_length']}")

def test_normalization():
    """Тест нормализации URL"""
    print()
    print("ТЕСТ НОРМАЛИЗАЦИИ URL")

    cases = [
        ("https://www.python.org/", "https://www.python.org"),
        ("https://www.python.org", "https://www.python.org"),
        ("https://WWW.PYTHON.ORG/about/", "https://www.python.org/about"),
        ("https://example.com:443/path", "https://example.com/path"),
        ("https://example.com/path#section", "https://example.com/path"),
    ]

    for original, expected in cases:
        result = normalize_url(original)
        status = "correct" if result == expected else "incorrect"
        print(f"{status} {original:50s} → {result}")
        if result != expected:
            print(f"    Ожидалось: {expected}")


def test_queue_dedup():
    """Тест очереди"""
    print()
    print("ТЕСТ ДЕДУПЛИКАЦИИ")

    q = CrawlerQueue()

    urls = [
        "https://python.org",
        "https://python.org/",
        "https://python.org/#anchor",
        "https://PYTHON.ORG/",
    ]

    added = 0
    for url in urls:
        if q.add_url(url):
            added += 1

    print(f"\nПопыток добавить: {len(urls)}")
    print(f"Реально добавлено: {added}")
    print(f"Ожидалось: 1")
    print(f"{'correct' if added == 1 else 'incorrect'} Дедупликация {'работает' if added == 1 else 'НЕ работает'}")

async def test_depth():
    """Тест ограничения глубины"""
    print()
    print("ТЕСТ ОГРАНИЧЕНИЯ ГЛУБИНЫ")

    for max_depth in (0, 1):
        crawler = AdvancedCrawler(max_depth=max_depth)
        try:
            results = await crawler.crawl(
                start_urls=["https://www.python.org"],
                max_pages=5,
                max_depth=max_depth,
                same_domain_only=True,
                progress_every=100,   
            )
            depths = sorted({crawler.depth.get(u, 0) for u in results})
        finally:
            await crawler.close()

        print(f"\n max_depth={max_depth}: обработано {len(results)}")
        print(f"   Глубины в результатах: {depths}")

        if max_depth == 0:
            print("   Ожидание: только глубина 0 (стартовый URL)")
        else:
            print("   Ожидание: глубины 0 и 1")    

async def test_filters():
    """Тест фильтров URL."""
    print()
    print("ТЕСТ ФИЛЬТРОВ")

    crawler = AdvancedCrawler(max_depth=1)
    try:
        results = await crawler.crawl(
            start_urls=["https://www.python.org"],
            max_pages=5,
            max_depth=1,
            same_domain_only=True,
            exclude_patterns=["/jobs/", "/downloads/"],
            progress_every=100,
        )
    finally:
        await crawler.close()

    bad = [u for u in results if "/jobs/" in u or "/downloads/" in u]
    print(f"\n exclude ['/jobs/', '/downloads/']")
    print(f"   Обработано: {len(results)}")
    print(f"   Нарушений: {len(bad)} (должно быть 0)")
    if bad:
        for u in bad:
            print(f"      bad {u}")

    crawler = AdvancedCrawler(max_depth=1)
    try:
        results = await crawler.crawl(
            start_urls=["https://www.python.org"],
            max_pages=5,
            max_depth=1,
            same_domain_only=True,
            include_patterns=["/about", "/community"],
            progress_every=100,
        )
    finally:
        await crawler.close()

    print(f"\n include ['/about', '/community']")
    print(f"   Обработано: {len(results)}")
    for u in results:
        print(f"      {u}")

    bad = [u for u in results if "/about" not in u and "/community" not in u and u != "https://www.python.org"]
    print(f"   Нарушений: {len(bad)} (кроме стартового)")            

async def test_duplicates():
    """Тест отсутствия дубликатов"""
    print()
    print("ТЕСТ ОТСУТСТВИЯ ДУБЛИКАТОВ")

    crawler = AdvancedCrawler(max_depth=1)
    try:
        results = await crawler.crawl(
            start_urls=["https://www.python.org"],
            max_pages=10,
            max_depth=1,
            same_domain_only=True,
            progress_every=100,
        )
    finally:
        await crawler.close()

    urls = list(results.keys())
    unique_urls = set(urls)

    print(f"\nОбработано URL: {len(urls)}")
    print(f"Уникальных: {len(unique_urls)}")

    if len(urls) == len(unique_urls):
        print(" Дубликатов нет")
    else:
        print(" Есть дубликаты!")

    print(f"\nvisited_urls в очереди: {len(crawler.queue.visited_urls)}")
    print(f"processed: {len(crawler.queue.processed)}")
    print(f"failed: {len(crawler.queue.failed)}")   


async def test_parallelism():
    """Проверить, что несколько страниц обрабатываются одновременно."""
    print()
    print("ТЕСТ ПАРАЛЛЕЛИЗМА")

    crawler = AdvancedCrawler(
        max_concurrent=5,
        global_limit=5,
        per_domain_limit=5,
        max_depth=1,
    )

    active = 0
    max_active = 0

    original_process = crawler._process_one
    async def tracking_process(url, depth):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.3)          # имитируем сетевую задержку
            return await original_process(url, depth)
        finally:
            active -= 1

    crawler._process_one = tracking_process

    try:
        await crawler.crawl(
            start_urls=["https://www.python.org"],
            max_pages=8,
            max_depth=1,
            same_domain_only=True,
            progress_every=100,
        )
    finally:
        await crawler.close()

    print(f"  Максимум одновременных задач: {max_active}")
    assert max_active > 1, f"Параллелизма нет: max_active={max_active}"
    print(" Параллелизм работает")     

async def test_max_pages_strict():
    """max_pages не должен превышаться даже при параллельных воркерах"""
    print()
    print("ТЕСТ: строгое соблюдение max_pages")

    crawler = AdvancedCrawler(
        max_concurrent=5,
        global_limit=5,
        per_domain_limit=5,
        max_depth=1,
    )

    # 5 стартовых URL, max_pages=1
    urls = [
        "https://www.python.org",
        "https://www.python.org/about",
        "https://www.python.org/community",
        "https://www.python.org/psf",
        "https://www.python.org/jobs",
    ]

    try:
        results = await crawler.crawl(
            start_urls=urls,
            max_pages=1,
            max_depth=0,
            same_domain_only=True,
            progress_every=100,
        )
    finally:
        await crawler.close()

    print(f"  Обработано: {len(results)} (ожидалось 1)")
    assert len(results) == 1, f"max_pages нарушен: обработано {len(results)}"

    print(" max_pages соблюдается строго")

async def main():
    test_normalization()
    test_queue_dedup()

    await demo()        
    await test_depth()
    await test_filters()
    await test_duplicates()
    await test_parallelism()
    await test_max_pages_strict()
    

if __name__ == '__main__':
    asyncio.run(main())