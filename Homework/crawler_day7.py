
import requests
import argparse
import asyncio
import json
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse

import aiofiles
import yaml
from crawler_day1 import AsyncCrawler
from crawler_day6 import (
    DataStorage,
    JSONStorage,
    CSVStorage,
    SQLiteStorage,
    StoringCrawler,
)

logger = logging.getLogger(__name__)

class SitemapParser:
    """Парсер sitemap.xml с поддержкой sitemap index"""

    def __init__(self, crawler, max_depth: int = 3):
        if max_depth < 1:
            raise ValueError("max_depth должен быть >= 1")
        self.crawler = crawler
        self.max_depth = max_depth
        self._visited: set[str] = set()

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        """Загрузка sitemap"""
        self._visited.clear()
        return await self._fetch_recursive(sitemap_url, depth=0)

    async def _fetch_recursive(self, url: str, depth: int) -> list[str]:
        """Рекурсивная загрузка"""
        if depth > self.max_depth:
            logger.warning(f"  Превышена глубина sitemap: {url}")
            return []

        if url in self._visited:
            logger.debug(f"  Уже видели: {url}" )
            return []

        self._visited.add(url)

        try:
            text = await self.crawler.fetch_url(url)
        except Exception as e:
            logger.warning(f" Ошибка загрузки sitemap {url}: {e}")
            return []

        if not text:
            return []

        return await self._parse_xml(text, url, depth)

    async def _parse_xml(self, text: str, url: str, depth: int) -> list[str]:
        """Парсинг XML"""
        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            logger.warning(f" Ошибка парсинга XML {url}: {e}")
            return []

        # Убираем namespace http://...
        self._strip_namespace(root)

        tag = root.tag.lower()   

        if tag == "urlset":
            return self._extract_urls(root)
        elif tag == "sitemapindex":
            return await self._process_index(root, depth)
        else:
            logger.warning(f"  Неизвестный тип sitemap: {tag}")
            return []

    def _strip_namespace(self, elem):
        """Убираем namespace из тегов"""
        for e in elem.iter():
            if "}" in e.tag:
                e.tag = e.tag.split("}", 1)[1]

    def _extract_urls(self, root) -> list[str]:
        """Извлекаем <loc> из urlset"""
        urls = []
        for url_elem in root.findall("url"):
            loc = url_elem.find("loc")
            if loc is not None and loc.text:
                urls.append(loc.text.strip())
        logger.info(f" Извлечено {len(urls)} URL из sitemap")
        return urls

    async def _process_index(self, root, depth: int) -> list[str]:
        """Рекурсивно обрабатываем sitemap index"""
        results = []
        sitemaps = root.findall("sitemap")

        logger.info(f" Sitemap index: {len(sitemaps)} вложенных")

        for sitemap in sitemaps:
            loc = sitemap.find("loc")
            if loc is not None and loc.text:
                sub_urls = await self._fetch_recursive(loc.text.strip(),depth=depth + 1)
                results.extend(sub_urls)

        return results

class CrawlerStats:
    """Статистики краулера"""

    def __init__(self):
        self.start_time = time.time()
        self.end_time: float | None = None
        self.total_processed = 0
        self.successful = 0
        self.failed = 0
        self.status_codes: Counter = Counter()      
        self.domains: Counter = Counter()           
        self.error_types: Counter = Counter()       

    def record(
        self,
        url: str,
        status_code: int,
        success: bool,
        error_type: str | None = None,
    ):
        """Фиксация обработки одной страницы"""
        self.total_processed += 1

        if success:
            self.successful += 1
        else:
            self.failed += 1
            if error_type:
                self.error_types[error_type] += 1

        if status_code:
            self.status_codes[status_code] += 1

        domain = urlparse(url).netloc
        if domain:
            self.domains[domain] += 1

    def finish(self):
        """Фиксация конца работы"""
        self.end_time = time.time()

    def to_dict(self) -> dict:
        """Сериализовать в dict"""
        elapsed = (self.end_time or time.time()) - self.start_time
        avg_rps = self.total_processed / elapsed if elapsed > 0 else 0

        return {
            "total_pages": self.total_processed,
            "successful": self.successful,
            "failed": self.failed,
            "elapsed_seconds": round(elapsed, 2),
            "avg_rps": round(avg_rps, 2),
            "status_codes": dict(self.status_codes),
            "top_domains": self.domains.most_common(10),
            "error_types": dict(self.error_types),
        }

    def __str__(self) -> str:
        d = self.to_dict()
        return (
            f"CrawlerStats("
            f"total={d['total_pages']}, "
            f"ok={d['successful']}, "
            f"fail={d['failed']}, "
            f"elapsed={d['elapsed_seconds']}s, "
            f"rps={d['avg_rps']})"
        )

class StatsExporter:
    """Экспорт статистики в JSON и HTML"""

    @staticmethod
    async def export_to_json(stats: CrawlerStats, filename: str):
        """Запись статистики в JSON"""
        data = stats.to_dict()

        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, ensure_ascii=False, indent=2))

        logger.info(f" JSON-отчёт сохранён: {filename}")
        return filename

    @staticmethod
    async def export_to_html(stats: CrawlerStats, filename: str):
        """Создать HTML-отчёт"""
        d = stats.to_dict()

        # Формируем строки таблиц
        status_rows = "".join(
            f"<tr><td>{code}</td><td>{count}</td></tr>"
            for code, count in sorted(d["status_codes"].items())
        )
        domain_rows = "".join(
            f"<tr><td>{dom}</td><td>{cnt}</td></tr>"
            for dom, cnt in d["top_domains"]
        )
        error_rows = "".join(
            f"<tr><td>{err}</td><td>{cnt}</td></tr>"
            for err, cnt in d["error_types"].items()
        )

        html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Crawler Report</title>
    <style>
        body {{
            font-family: -apple-system, Segoe UI, sans-serif;
            max-width: 900px;
            margin: 40px auto;
            padding: 0 20px;
            color: #333;
        }}
        h1 {{ border-bottom: 2px solid #0066cc; padding-bottom: 10px; }}
        h2 {{ color: #0066cc; margin-top: 30px; }}
        .summary {{
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
        }}
        .card {{
            background: #f5f7fa;
            padding: 15px 25px;
            border-radius: 8px;
            min-width: 150px;
        }}
        .card .num {{
            font-size: 2em;
            font-weight: bold;
            color: #0066cc;
        }}
        .card .label {{ color: #666; font-size: 0.9em; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}
        th, td {{
            text-align: left;
            padding: 8px 12px;
            border-bottom: 1px solid #ddd;
        }}
        th {{ background: #f5f7fa; }}
        .timestamp {{ color: #888; font-size: 0.85em; }}
    </style>
</head>
<body>
    <h1> Crawler Report</h1>
    <p class="timestamp">Сгенерировано: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>

    <h2> Общая статистика</h2>
    <div class="summary">
        <div class="card">
            <div class="num">{d['total_pages']}</div>
            <div class="label">Всего страниц</div>
        </div>
        <div class="card">
            <div class="num" style="color: #28a745;">{d['successful']}</div>
            <div class="label">Успешных</div>
        </div>
        <div class="card">
            <div class="num" style="color: #dc3545;">{d['failed']}</div>
            <div class="label">Упавших</div>
        </div>
        <div class="card">
            <div class="num">{d['elapsed_seconds']}s</div>
            <div class="label">Время работы</div>
        </div>
        <div class="card">
            <div class="num">{d['avg_rps']}</div>
            <div class="label">Страниц/сек</div>
        </div>
    </div>

    <h2> HTTP-коды</h2>
    <table>
        <tr><th>Код</th><th>Количество</th></tr>
        {status_rows if status_rows else "<tr><td colspan='2'>Нет данных</td></tr>"}
    </table>

    <h2> Топ доменов</h2>
    <table>
        <tr><th>Домен</th><th>Страниц</th></tr>
        {domain_rows if domain_rows else "<tr><td colspan='2'>Нет данных</td></tr>"}
    </table>

    <h2> Типы ошибок</h2>
    <table>
        <tr><th>Тип</th><th>Количество</th></tr>
        {error_rows if error_rows else "<tr><td colspan='2'>Нет ошибок 🎉</td></tr>"}
    </table>
</body>
</html>
"""

        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            await f.write(html)

        logger.info(f" HTML-отчёт сохранён: {filename}")
        return filename

class CrawlerConfig:
    """Конфигурация краулера"""

    DEFAULTS = {
        "start_urls": [],
        "max_pages": 100,
        "max_depth": 2,
        "max_concurrent": 10,
        "requests_per_second": 2.0,
        "respect_robots": True,
        "user_agent": "Day7Bot/1.0",
        "min_delay": 0.0,
        "jitter": 0.0,
        "max_retries": 3,
        "backoff_factor": 2.0,
        "circuit_threshold": 5,
        "circuit_timeout": 30.0,
        "same_domain_only": False,
        "exclude_patterns": [],
        "include_patterns": [],
        "storage": {
            "type": "json",
            "path": "results.jsonl",
        },
        "output": {
            "json": "report.json",
            "html": "report.html",
        },
        "log_file": "crawler.log",
        "log_level": "INFO",
    }

    def __init__(self, data: dict):
        self.data = {**self.DEFAULTS, **data}

    @classmethod
    def from_yaml(cls, path: str) -> "CrawlerConfig":
        """Загрузка конфига из YAML-файла"""
        if not os.path.exists(path):
            raise FileNotFoundError(f"Конфиг не найден: {path}")

        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        logger.info(f"  Конфиг загружен: {path}" )
        return cls(data)

    @classmethod
    def from_dict(cls, data: dict) -> "CrawlerConfig":
        """Создать конфиг из словаря."""
        return cls(data)

    def get(self, key: str, default=None):
        """Получить значение по ключу"""
        return self.data.get(key, default)

    def __getitem__(self, key: str):
        return self.data[key]

    def to_dict(self) -> dict:
        return dict(self.data)

    def __repr__(self) -> str:
        return f"CrawlerConfig(keys={list(self.data.keys())})"

class FinalCrawler(StoringCrawler):
    """Финальный краулер"""

    def __init__(self, config: CrawlerConfig, storage: DataStorage):
        super().__init__(
            storage=storage,
            max_concurrent=config["max_concurrent"],
            max_depth=config["max_depth"],
            requests_per_second=config["requests_per_second"],
            respect_robots=config["respect_robots"],
            user_agent=config["user_agent"],
            max_retries=config["max_retries"],
            backoff_factor=config["backoff_factor"],
            circuit_threshold=config["circuit_threshold"],
            circuit_timeout=config["circuit_timeout"],
            min_delay=config["min_delay"],   
            jitter=config["jitter"],         
        )

        self.config = config
        self.stats = CrawlerStats()
        self.sitemap_parser = SitemapParser(self, max_depth=3)

    async def crawl(self, **overrides) -> dict:
        """Запуск краулинга из конфига"""
        start_urls = overrides.get("start_urls") or self.config["start_urls"]
        max_pages = overrides.get("max_pages", self.config["max_pages"])
        max_depth = overrides.get("max_depth", self.config["max_depth"])
        
        same_domain_only = overrides.get("same_domain_only", self.config["same_domain_only"])
        exclude_patterns = overrides.get("exclude_patterns", self.config["exclude_patterns"])
        include_patterns = overrides.get("include_patterns", self.config["include_patterns"])

        if not start_urls:
            raise ValueError("start_urls не заданы (ни в конфиге, ни в overrides)")

        logger.info(
            " Старт FinalCrawler: %d URL, max_pages=%d, max_depth=%d",
            len(start_urls), max_pages, max_depth,
        )

        results = await super().crawl(
            start_urls=start_urls,
            max_pages=max_pages,
            max_depth=max_depth,
            same_domain_only=same_domain_only,
            exclude_patterns=exclude_patterns,
            include_patterns=include_patterns,
        )

        self._collect_stats(results)

        self.stats.finish()

        return results

    def _collect_stats(self, results: dict):
        """Перенести данные из родителя в CrawlerStats."""
        for url, data in results.items():
            self.stats.record(
                url=url,
                status_code=200,
                success=True,
            )

        for url, error in self.queue.failed.items():
            error_type = "UnknownError"
            error_str = str(error)
            for known in ("PermanentError", "TransientError", "NetworkError"):
                if known in error_str:
                    error_type = known
                    break
            if "circuit" in error_str.lower():
                error_type = "CircuitOpen"
            if "robots" in error_str.lower():
                error_type = "RobotsBlocked"

            self.stats.record(
                url=url,
                status_code=0,
                success=False,
                error_type=error_type,
            )

    def get_stats(self) -> dict:
        return self.stats.to_dict()

    async def export_to_json(self, filename: str | None = None) -> str:
        """Экспорт статистики в JSON"""
        filename = filename or self.config["output"]["json"]
        return await StatsExporter.export_to_json(self.stats, filename)

    async def export_to_html_report(self, filename: str | None = None) -> str:
        """Экспорт статистики в HTML"""
        filename = filename or self.config["output"]["html"]
        return await StatsExporter.export_to_html(self.stats, filename)

    async def crawl_sitemap(self, sitemap_url: str) -> dict:
        """Краулить URL из sitemap"""
        logger.info(f" Загрузка sitemap: {sitemap_url}")
        urls = await self.sitemap_parser.fetch_sitemap(sitemap_url)

        if not urls:
            logger.warning("Sitemap пуст — краулинг не запущен")
            return {}

        logger.info(f" Получено {len(urls)} URL из sitemap")

        return await self.crawl(start_urls=urls)

    @classmethod
    def from_config(cls, config_path: str) -> "FinalCrawler":
        """Создание краулер из YAML-файла"""
        config = CrawlerConfig.from_yaml(config_path)
        storage = _create_storage(config["storage"])
        return cls(config=config, storage=storage)

    async def close(self):
        try:
            self.stats.finish()
        except Exception:
            pass
        await super().close()


def _create_storage(storage_cfg: dict) -> DataStorage:
    """Фабрика хранилища по конфигу"""
    stype = storage_cfg.get("type", "json").lower()
    path = storage_cfg.get("path", "results.jsonl")

    if stype == "json":
        return JSONStorage(path)
    elif stype == "csv":
        return CSVStorage(path)
    elif stype == "sqlite":
        batch_size = storage_cfg.get("batch_size", 10)
        return SQLiteStorage(path, batch_size=batch_size)
    else:
        raise ValueError(f"Неизвестный тип storage: {stype}")    


def setup_logging(log_file: str = "crawler.log", level: str = "INFO"):
    """Настройка логирования в файл"""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)   

    # Убираем старые хендлеры
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,   # 10 MB
        backupCount=5,                
        encoding="utf-8",
    )
    file_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(getattr(logging, level.upper(), logging.INFO))
    console.setFormatter(formatter)
    root.addHandler(console)

    logging.getLogger(__name__).info(
        " Логирование: файл=%s, уровень=%s", log_file, level
    )

def parse_args() -> argparse.Namespace:
    """Разобрать аргументы командной строки"""
    parser = argparse.ArgumentParser(
        description="Advanced async crawler (Day 7)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--urls", nargs="+",
        help="Стартовые URL",
    )
    parser.add_argument(
        "--sitemap",
        help="URL sitemap.xml",
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="Путь к YAML-конфигу",
    )
    parser.add_argument(
        "--max-pages", type=int,
        help="Максимум страниц",
    )
    parser.add_argument(
        "--max-depth", type=int,
        help="Максимальная глубина",
    )
    parser.add_argument(
        "--output",
        help="Файл JSON-отчёта",
    )
    parser.add_argument(
        "--html",
        help="Файл HTML-отчёта",
    )
    parser.add_argument(
        "--rate-limit", type=float,
        help="Запросов в секунду",
    )
    parser.add_argument(
        "--no-robots", action="store_true",
        help="Не соблюдать robots.txt",
    )
    parser.add_argument(
        "--log-file", default="crawler_day7.log",
        help="Файл логов",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Уровень логирования",
    )

    return parser.parse_args()


async def run_from_cli():
    """Запуск краулера из командной строки"""
    args = parse_args()

    setup_logging(args.log_file, args.log_level)

    try:
        config = CrawlerConfig.from_yaml(args.config)
    except FileNotFoundError:
        logger.warning(
            "Конфиг %s не найден — использую defaults", args.config
        )
        config = CrawlerConfig.from_dict({})

    overrides = {}
    if args.urls:
        overrides["start_urls"] = args.urls
    if args.max_pages:
        overrides["max_pages"] = args.max_pages
    if args.max_depth is not None:
        overrides["max_depth"] = args.max_depth
    if args.rate_limit:
        overrides["requests_per_second"] = args.rate_limit
    if args.no_robots:
        overrides["respect_robots"] = False

    for key, value in overrides.items():
        config.data[key] = value

    storage = _create_storage(config["storage"])
    crawler = FinalCrawler(config=config, storage=storage)

    try:
        if args.sitemap:
            results = await crawler.crawl_sitemap(args.sitemap)
        else:
            results = await crawler.crawl()
    finally:
        await crawler.close()

    json_path = await crawler.export_to_json(args.output)
    html_path = await crawler.export_to_html_report(args.html)

    s = crawler.get_stats()
    print()
    print("ИТОГИ")
    print(f"  Обработано:  {s['total_pages']}")
    print(f"  Успешно:     {s['successful']}")
    print(f"  Ошибок:      {s['failed']}")
    print(f"  Время:       {s['elapsed_seconds']} сек")
    print(f"  Скорость:    {s['avg_rps']} стр/сек")
    print(f"  JSON-отчёт:  {json_path}")
    print(f"  HTML-отчёт:  {html_path}")

##############################################################3

async def test_sitemap():
    """Тест SitemapParser."""
    print()
    print("ТЕСТ SitemapParser")

    crawler = AsyncCrawler()
    parser = SitemapParser(crawler, max_depth=2)

    print("\n1. Синтетический urlset ")
    urlset_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <url><loc>https://example.com/</loc></url>
        <url><loc>https://example.com/about</loc></url>
        <url><loc>https://example.com/contact</loc></url>
    </urlset>"""

    urls = await parser._parse_xml(urlset_xml, "test_urlset", 0)
    print(f"Найдено: {len(urls)}")
    for u in urls:
        print(f"   {u}")

    print("\n2. Синтетический sitemapindex ")
    index_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <sitemap><loc>https://example.com/sitemap-1.xml</loc></sitemap>
        <sitemap><loc>https://example.com/sitemap-2.xml</loc></sitemap>
    </sitemapindex>"""

    async def fake_fetch(url):
        if "sitemap-1" in url:
            return """<?xml version="1.0"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>https://a.com/page1</loc></url>
                <url><loc>https://a.com/page2</loc></url>
            </urlset>"""
        elif "sitemap-2" in url:
            return """<?xml version="1.0"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>https://b.com/page3</loc></url>
            </urlset>"""
        return None

    parser.crawler.fetch_url = fake_fetch
    parser._visited.clear()

    urls = await parser._parse_xml(index_xml, "test_index", 0)
    print(f"Найдено: {len(urls)}")
    for u in urls:
        print(f"   {u}")

    await crawler.close()



async def test_stats():
    """Тест CrawlerStats"""
    print()
    print("ТЕСТ CrawlerStats")

    stats = CrawlerStats()

    stats.record("https://python.org/", 200, success=True)
    stats.record("https://python.org/about", 200, success=True)
    stats.record("https://python.org/jobs", 404, success=False, error_type="PermanentError")
    stats.record("https://google.com/", 200, success=True)
    stats.record("https://google.com/maps", 500, success=False, error_type="TransientError")
    stats.record("https://example.com/", 200, success=True)

    stats.finish()

    print(f"\n{stats}")
    print()
    print("Детали:")
    for key, value in stats.to_dict().items():
        print(f"  {key}: {value}")

async def test_export():
    """Тест экспорта."""
    print()

    print("ТЕСТ StatsExporter")

    stats = CrawlerStats()
    stats.record("https://python.org/", 200, success=True)
    stats.record("https://python.org/about", 200, success=True)
    stats.record("https://python.org/jobs", 404, success=False, error_type="PermanentError")
    stats.record("https://google.com/", 200, success=True)
    stats.record("https://google.com/maps", 500, success=False, error_type="TransientError")
    stats.record("https://example.com/", 200, success=True)
    stats.finish()


    json_path = await StatsExporter.export_to_json(stats, "report.json")
    html_path = await StatsExporter.export_to_html(stats, "report.html")

    print(f"\n JSON: {json_path}")
    print(f" HTML: {html_path}")

    async with aiofiles.open(json_path, encoding="utf-8") as f:
        content = await f.read()
        data = json.loads(content)
        print(f"\nJSON total_pages: {data['total_pages']}")
        print(f"JSON successful:  {data['successful']}")

async def test_config():
    """Тест CrawlerConfig."""
    print()
    print("ТЕСТ CrawlerConfig")

    config = CrawlerConfig.from_yaml("config.yaml")
    print(f"\n Загружено: {config}")
    print(f"   start_urls: {config['start_urls']}")
    print(f"   max_pages:  {config['max_pages']}")
    print(f"   storage:    {config['storage']}")

    config2 = CrawlerConfig.from_dict({"max_pages": 42})
    print(f"\n Из dict: max_pages={config2['max_pages']}, "
          f"user_agent={config2['user_agent']}")

    assert config2["max_pages"] == 42
    assert config2["user_agent"] == "Day7Bot/1.0"   
    print("\n Слияние DEFAULTS работает")

async def test_final_crawler():
    """Тест FinalCrawler."""
    print()
    print("ТЕСТ FinalCrawler")


    crawler = FinalCrawler.from_config("config.yaml")
    print(f" Создан: {crawler}")

    results = await crawler.crawl(max_pages=3)

    await crawler.close()

    print(f"\n Результат: {len(results)} страниц")
    print(f" Stats: {crawler.stats}")

    s = crawler.get_stats()
    print(f"\n  total:      {s['total_pages']}")
    print(f"  successful: {s['successful']}")
    print(f"  failed:     {s['failed']}")
    print(f"  elapsed:    {s['elapsed_seconds']}s")
    print(f"  rps:        {s['avg_rps']}")
    print(f"  domains:    {s['top_domains']}")

    json_path = await crawler.export_to_json()
    html_path = await crawler.export_to_html_report()
    print(f"\n JSON: {json_path}")
    print(f" HTML: {html_path}")

async def test_performance():
    """Тест производительности"""
    print()
    print("ТЕСТ ПРОИЗВОДИТЕЛЬНОСТИ")

    urls = [f"https://httpbin.org/delay/1" for _ in range(5)]

    print("\nAsync")
    crawler = FinalCrawler.from_config("config.yaml")

    start = time.time()
    await crawler.crawl(
        start_urls=urls,
        max_pages=5,
        max_depth=0,
        same_domain_only=True,
    )
    elapsed_async = time.time() - start
    await crawler.close()

    print(f"\n  Async: {elapsed_async:.2f} сек для 5 URL")

    print("\nSync")
    try:
        start = time.time()
        for url in urls:
            try:
                requests.get(url, timeout=10)
            except Exception as e:
                print(f"   Ошибка: {e}")
        elapsed_sync = time.time() - start
        print(f"\n  Sync:  {elapsed_sync:.2f} сек для 5 URL")

        speedup = elapsed_sync / elapsed_async if elapsed_async > 0 else 0
        print(f"\n Ускорение: {speedup:.2f}x")
    except ImportError:
        print("   requests не установлен — пропускаем")    

async def test_config_delay():
    print()
    print("ТЕСТ: min_delay/jitter из конфига")

    config = CrawlerConfig.from_dict({
        "start_urls": ["https://example.com"],
        "min_delay": 0.3,
        "jitter": 0.1,
        "respect_robots": False,
    })

    storage = _create_storage(config["storage"])
    crawler = FinalCrawler(config=config, storage=storage)

    print(f"  crawler.min_delay = {crawler.min_delay}")
    print(f"  crawler.jitter    = {crawler.jitter}")
    assert crawler.min_delay == 0.3, f"min_delay не пробросился: {crawler.min_delay}"
    assert crawler.jitter == 0.1, f"jitter не пробросился: {crawler.jitter}"

    await crawler.close()
    print(" Параметры доходят из конфига")

async def main():
    setup_logging("crawler_day7.log", "INFO")

    await test_sitemap()
    await test_stats()
    await test_export()
    await test_config()
    await test_final_crawler()
    await test_performance()
    await test_config_delay()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        asyncio.run(run_from_cli())
    else:
        asyncio.run(main())   