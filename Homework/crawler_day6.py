import asyncio
import csv
import io
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime

import aiofiles
import aiosqlite

from crawler_day5 import ReliableCrawler


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

class DataStorage(ABC):
    """Абстрактное хранилище данных"""

    @abstractmethod
    async def save(self, data: dict) -> None:
        """Сохранить одну запись"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Закрыть хранилище"""
        pass

    def stats(self) -> dict:
        """Базовая статистика"""
        return {
            "saved": 0,
            "errors": 0,
        }

class JSONStorage(DataStorage):
    """Хранилище в формате JSON"""

    def __init__(self, path: str = "results.jsonl", indent: int = None):
        if not path:
            raise ValueError("path не может быть пустым")

        self.path = path
        self._file = None
        self._saved = 0
        self._errors = 0
        self.indent = indent

    async def _ensure_open(self):
        """Открыть файл, если ещё не открыт"""
        if self._file is None:
            self._file = await aiofiles.open(self.path, "a", encoding="utf-8")
            logger.debug(f"Открыт файл: {self.path}" )

    async def save(self, data: dict) -> None:
        """Добавить одну запись"""
        if not isinstance(data, dict):
            raise ValueError(f"data должен быть dict, получено {type(data).__name__}")

        await self._ensure_open()

        try:
            line = json.dumps(data, ensure_ascii=False, indent=self.indent)
            await self._file.write(line + "\n")
            await self._file.flush()   
            self._saved += 1
        except Exception as e:
            self._errors += 1
            logger.error(f"Ошибка записи в JSON: {e}")
            raise

    async def close(self) -> None:
        """Закрыть файл"""
        if self._file is not None:
            await self._file.close()
            self._file = None
            logger.info(f"JSONStorage закрыт ({self._saved} записей)")

    def stats(self) -> dict:
        return {
            "type": "JSONStorage",
            "path": self.path,
            "saved": self._saved,
            "errors": self._errors,
        }    

class CSVStorage(DataStorage):
    """Хранилище в формате CSV"""

    def __init__(self, path: str = "results.csv", encoding: str = "utf-8"):
        if not path:
            raise ValueError("path не может быть пустым")

        self.path = path
        self.encoding = encoding
        self._file = None
        self._writer = None
        self._headers: list[str] | None = None
        self._saved = 0
        self._errors = 0

    def _flatten(self, data: dict) -> dict:
        """Сериализовать сложные поля в строки"""
        result = {}
        for key, value in data.items():
            if isinstance(value, (list, dict)):
                result[key] = json.dumps(value, ensure_ascii=False)
            elif value is None:
                result[key] = ""
            else:
                result[key] = value
        return result

    async def _ensure_open(self, headers: list[str]):
        """Открыть файл и создать writer при первой записи"""
        if self._file is None:
            file_exists = os.path.exists(self.path)

            need_headers = not file_exists or os.path.getsize(self.path) == 0

            self._file = await aiofiles.open(
                self.path, "a", encoding=self.encoding, newline=""
            )

            self._buffer = io.StringIO()
            self._writer = csv.DictWriter(
                self._buffer,
                fieldnames=headers,
                extrasaction="ignore",   
            )

            if need_headers:
                self._writer.writeheader()
                await self._file.write(self._buffer.getvalue())
                await self._file.flush()
                self._buffer.seek(0)
                self._buffer.truncate(0)

            self._headers = headers

    async def save(self, data: dict) -> None:
        """Добавить одну запись"""
        if not isinstance(data, dict):
            raise ValueError(f"data должен быть dict, получено {type(data).__name__}")

        flat = self._flatten(data)

        if self._headers is None:
            self._headers = list(flat.keys())

        await self._ensure_open(self._headers)

        try:
            self._writer.writerow(flat)
            await self._file.write(self._buffer.getvalue())
            await self._file.flush()

            self._buffer.seek(0)
            self._buffer.truncate(0)

            self._saved += 1
        except Exception as e:
            self._errors += 1
            logger.error(f"Ошибка записи в CSV: {e}")
            raise

    async def close(self) -> None:
        if self._file is not None:
            await self._file.close()
            self._file = None
            self._writer = None
            logger.info(f"CSVStorage закрыт ({self._saved} записей)" )

    def stats(self) -> dict:
        return {
            "type": "CSVStorage",
            "path": self.path,
            "saved": self._saved,
            "errors": self._errors,
            "headers": self._headers,
        }

class SQLiteStorage(DataStorage):
    """Хранилище в SQLite с batch-вставками"""

    def __init__(
        self,
        path: str = "crawler.db",
        batch_size: int = 10,
    ):
        if not path:
            raise ValueError("path не может быть пустым")
        if batch_size < 1:
            raise ValueError("batch_size должен быть >= 1")

        self.path = path
        self.batch_size = batch_size
        self._conn: aiosqlite.Connection | None = None
        self._buffer: list[tuple] = []
        self._saved = 0
        self._errors = 0
        self._duplicates = 0

    async def init_db(self):
        """Создать таблицы и индексы."""
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.path)

        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                text TEXT,
                links TEXT,
                metadata TEXT,
                crawled_at TEXT,
                status_code INTEGER,
                content_type TEXT
            )
        """)
        await self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_url ON pages(url)"
        )
        await self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_crawled_at ON pages(crawled_at)"
        )
        await self._conn.commit()
        self._initialized = True
        logger.debug("SQLite схема инициализирована: %s", self.path)

    async def _ensure_open(self):
        """Открыть соединение и создать схему."""
        if self._conn is None:
            await self.init_db()

    def _prepare_row(self, data: dict) -> tuple:
        """Преобразовать dict в tuple для SQL"""
        return (
            data.get("url", ""),
            data.get("title"),
            data.get("text"),
            json.dumps(data.get("links", []), ensure_ascii=False),
            json.dumps(data.get("metadata", {}), ensure_ascii=False),
            data.get("crawled_at"),
            data.get("status_code"),
            data.get("content_type"),
        )

    async def save(self, data: dict) -> None:
        """Добавить запись в буфер"""
        if not isinstance(data, dict):
            raise ValueError(f"data должен быть dict, получено {type(data).__name__}")

        await self._ensure_open()

        self._buffer.append(self._prepare_row(data))

        if len(self._buffer) >= self.batch_size:
            await self._flush()

    async def _flush(self):
        """Записать буфер в БД одним batch"""
        if not self._buffer:
            return

        try:
            cursor = await self._conn.executemany(
                """
                INSERT OR IGNORE INTO pages
                    (url, title, text, links, metadata,
                     crawled_at, status_code, content_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._buffer,
            )

            inserted = cursor.rowcount
            duplicates = len(self._buffer) - inserted

            self._saved += inserted
            self._duplicates += duplicates

            await self._conn.commit()
            logger.debug(f"Flush: +{inserted} записей ({duplicates} дублей)")

        except Exception as e:
            self._errors += 1
            logger.error(f"Ошибка flush в SQLite: {e}")
            raise

        finally:
            self._buffer = []

    async def close(self) -> None:
        """Финальный flush и закрытие"""
        if self._conn is not None:
            await self._flush()
            await self._conn.close()
            self._conn = None
            logger.info(
                "SQLite закрыт (%d записей, %d дублей)",
                self._saved, self._duplicates,
            )

    async def read_all(self, limit: int = 100) -> list[dict]:
        """Прочитать все записи"""
        await self._ensure_open()

        cursor = await self._conn.execute(
            "SELECT url, title, status_code, crawled_at FROM pages LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()

        return [
            {
                "url": row[0],
                "title": row[1],
                "status_code": row[2],
                "crawled_at": row[3],
            }
            for row in rows
        ]

    def stats(self) -> dict:
        return {
            "type": "SQLiteStorage",
            "path": self.path,
            "saved": self._saved,
            "duplicates": self._duplicates,
            "errors": self._errors,
            "buffer_size": len(self._buffer),
        }

class StoringCrawler(ReliableCrawler):
    """ReliableCrawler с автоматическим сохранением данных"""

    def __init__(
        self,
        storage: DataStorage,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.storage = storage

    async def _process_one(self, url: str, current_depth: int):
        await super()._process_one(url, current_depth)

        if url in self.queue.processed:
            result = self.queue.processed[url]

            result["crawled_at"] = datetime.now().isoformat()
            result["status_code"] = 200
            result["content_type"] = "text/html"

            try:
                await self.storage.save(result)
                logger.debug(" Сохранено: %s", url)
            except Exception as e:
                logger.error(" Ошибка сохранения %s: %s", url, e)

    async def close(self):
        try:
            await self.storage.close()
        except Exception as e:
            logger.error(f"Ошибка закрытия storage: {e}")

        await super().close()

# ============================================================
# ТЕСТЫ
# ============================================================
async def demo():
    """Демонстрация"""
    print()
    print("ДЕМО: КРАУЛИНГ С СОХРАНЕНИЕМ В JSON / CSV / SQLITE")

    for path in ["crawl_results.jsonl", "crawl_results.csv", "crawl_results.db"]:
        if os.path.exists(path):
            os.remove(path)

    start_urls = [
        "https://www.python.org",
        "https://httpbin.org/html",
    ]

    for storage_name, storage in [
        ("JSON", JSONStorage("crawl_results.jsonl")),
        ("CSV", CSVStorage("crawl_results.csv")),
        ("SQLite", SQLiteStorage("crawl_results.db", batch_size=5)),
    ]:
        print(f" Хранилище: {storage_name}")


        crawler = StoringCrawler(
            storage=storage,
            max_concurrent=5,
            max_depth=0,          
            requests_per_second=2.0,
            respect_robots=True,
            user_agent="Day6Bot/1.0",
            max_retries=2,
            circuit_threshold=3,
            circuit_timeout=10.0,
        )

        try:
            results = await crawler.crawl(
                start_urls=start_urls,
                max_pages=5,
                max_depth=0,
                same_domain_only=False,
                progress_every=10,
            )
        finally:
            await crawler.close()

        print(f"\n Обработано: {len(results)}")
        print(f" Storage stats: {storage.stats()}")

    print()

    print("ПРОВЕРКА СОХРАНЁННЫХ ДАННЫХ")

    print("\n JSONL (первые 2 строки):")
    async with aiofiles.open("crawl_results.jsonl", encoding="utf-8") as f:
        count = 0
        async for line in f:
            if count >= 2:
                break
            data = json.loads(line)
            print(f"   {data['url']} — {data['title']}")
            count += 1

    print("\n CSV (первые 2 строки):")
    async with aiofiles.open("crawl_results.csv", encoding="utf-8") as f:
        lines = (await f.read()).splitlines()
        for line in lines[:3]:  
            print(f"   {line[:100]}")

    print("\n SQLite (все записи):")
    sqlite_check = SQLiteStorage("crawl_results.db")
    records = await sqlite_check.read_all()
    for r in records:
        print(f"   {r['url']} — {r['title']} ({r['status_code']})")
    await sqlite_check.close()

async def test_json_storage():
    """Тест JSONStorage."""
    print()
    print("ТЕСТ JSONStorage")

    if os.path.exists("test_output.jsonl"):
        os.remove("test_output.jsonl")

    storage = JSONStorage("test_output.jsonl")

    await storage.save({"url": "https://a.com", "title": "A", "links": ["x"]})
    await storage.save({"url": "https://b.com", "title": "Б", "links": []})
    await storage.save({"url": "https://c.com", "title": "C", "links": ["y", "z"]})

    await storage.close()

    async with aiofiles.open("test_output.jsonl", encoding="utf-8") as f:
        async for line in f:
            print("  ", line.strip())

    print(f"\n Stats: {storage.stats()}")


async def test_csv_storage():
    """Тест CSVStorage."""
    print()
    print("ТЕСТ CSVStorage")

    if os.path.exists("test_output.csv"):
        os.remove("test_output.csv")

    storage = CSVStorage("test_output.csv")

    await storage.save({
        "url": "https://a.com",
        "title": "A",
        "links": ["x", "y"],
    })
    await storage.save({
        "url": "https://b.com",
        "title": "Б",
        "links": ["z"],
    })

    await storage.close()

    print("\n Содержимое файла:")
    async with aiofiles.open("test_output.csv", encoding="utf-8") as f:
        content = await f.read()
    print(content)

    print(f" Stats: {storage.stats()}")

async def test_sqlite_storage():
    """Тест SQLiteStorage."""
    print()

    print("ТЕСТ SQLiteStorage")


    if os.path.exists("test_output.db"):
        os.remove("test_output.db")

    storage = SQLiteStorage("test_output.db", batch_size=3)

    for i in range(5):
        await storage.save({
            "url": f"https://example.com/page{i}",
            "title": f"Page {i}",
            "text": f"Content {i}",
            "links": [f"/link{i}"],
            "metadata": {"lang": "ru"},
            "crawled_at": datetime.now().isoformat(),
            "status_code": 200,
            "content_type": "text/html",
        })

    await storage.save({
        "url": "https://example.com/page0",
        "title": "Duplicate",
        "text": "...",
        "links": [],
        "metadata": {},
        "crawled_at": datetime.now().isoformat(),
        "status_code": 200,
        "content_type": "text/html",
    })

    await storage.close()

    print(f"\n Stats: {storage.stats()}")

    storage2 = SQLiteStorage("test_output.db")
    records = await storage2.read_all()
    print(f"\n Прочитано {len(records)} записей:")
    for r in records:
        print(f"   {r['url']} — {r['title']} ({r['status_code']})")
    await storage2.close()

async def test_save_error():
    print()
    print("ТЕСТ: обработка ошибок сохранения")

    class BrokenStorage(DataStorage):
        def __init__(self):
            self.save_calls = 0
            self.save_errors = 0

        async def save(self, data):
            self.save_calls += 1
            self.save_errors += 1
            raise IOError("Disk is full!")

        async def close(self):
            pass

    storage = BrokenStorage()

    crawler = StoringCrawler(
        storage=storage,
        max_concurrent=2,
        max_depth=0,
        requests_per_second=2.0,
        respect_robots=False,   
        max_retries=1,
    )

    try:
        results = await crawler.crawl(
            start_urls=["https://example.com"],
            max_pages=2,
            max_depth=0,
            progress_every=100,
        )
    finally:
        await crawler.close()

    print(f"\n Краулер обработал: {len(results)}")
    print(f" Ошибок сохранения (в логах)")
    print(f" Краулер НЕ упал, несмотря на ошибки storage")

    assert storage.save_errors > 0, "Ошибки должны были произойти!"
    assert len(results) > 0, "Краулер должен был обработать хотя бы одну страницу!"
    print(f"  Краулер НЕ упал, несмотря на {storage.save_errors} ошибок storage")

async def main():
    await test_json_storage()
    await test_csv_storage()
    await test_sqlite_storage()
    await test_save_error()      
    await demo()


if __name__ == "__main__":
    asyncio.run(main())