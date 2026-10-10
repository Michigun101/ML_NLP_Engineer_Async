import asyncio
import logging
import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawler_day1 import AsyncCrawler   


logger = logging.getLogger(__name__)

class HTMLParser:
    """Парсер HTML: извлекает ссылки, текст, метаданные."""

    def __init__(self, parser: str = "lxml"):
        self.parser = parser

    async def parse_html(self, html: str, url: str) -> dict:
        """парсинг HTML"""
        result = {
            "url": url,
            "title": None,
            "text": "",
            "text_length": 0,
            "links": [],
            "links_count": 0,
            "images": [],
            "images_count": 0,
            "headings": [],
            "tables": [],              
            "tables_count": 0,         
            "lists": [],               
            "lists_count": 0,          
            "metadata": {},
        }

        if not html or not isinstance(html, str):
            logger.warning(f"Пустой HTML для {url}")
            return result

        try:
            soup = BeautifulSoup(html, self.parser)
        except Exception as e:
            logger.warning(f"Ошибка парсинга для {url}: {e}")
            return result

        result["title"] = self._extract_title(soup)
        result["text"] = self.extract_text(soup)
        result["text_length"] = len(result["text"])
        result["links"] = self.extract_links(soup, url)
        result["links_count"] = len(result["links"])
        result["images"] = self.extract_images(soup, url)
        result["images_count"] = len(result["images"])
        result["headings"] = self.extract_headings(soup)
        result["tables"] = self.extract_tables(soup)              
        result["tables_count"] = len(result["tables"])            
        result["lists"] = self.extract_lists(soup)                
        result["lists_count"] = len(result["lists"])              
        result["metadata"] = self.extract_metadata(soup)

        return result

    def _extract_title(self, soup: BeautifulSoup) -> str | None:
        if soup.title and soup.title.string:
            return soup.title.string.strip()
        return None

    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        """Извлечение текста"""
        target = soup

        if selector:
            target = soup.select_one(selector)
            if target is None:
                logger.warning(f"Селектор не найден: {selector}")
                return ""
        else:
            target = soup.body if soup.body else soup

        for tag in target(["script", "style"]):
            tag.decompose()

        return target.get_text(separator=" ", strip=True)

    def extract_links(self, soup: BeautifulSoup, base_url: str, same_domain_only=False) -> list[str]:
        """Извлечение всех ссылок"""
        links = []
        seen = set()  
        base_domain = urlparse(base_url).netloc

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()

            if href.startswith(("#", "mailto:", "javascript:", "tel:")):
                continue

            absolute = urljoin(base_url, href)

            parsed = urlparse(absolute)
            if parsed.scheme not in ("http", "https"):
                continue

            if same_domain_only:
                if urlparse(absolute).netloc != base_domain:
                    continue

            if absolute not in seen:
                seen.add(absolute)
                links.append(absolute)

        return links

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        """Извлечение всех изображений"""
        images = []

        for img in soup.find_all("img"):
            src = img.get("src", "").strip()
            if not src:
                continue

            absolute = urljoin(base_url, src)
            images.append({
                "src": absolute,
                "alt": img.get("alt", "").strip(),
            })

        return images

    def extract_headings(self, soup: BeautifulSoup) -> list[dict]:
        """Извлечение всех заголовков"""
        headings = []

        for level in ("h1", "h2", "h3"):
            for h in soup.find_all(level):
                text = h.get_text(strip=True)
                if text:
                    headings.append({
                        "level": level,
                        "text": text,
                    })

        return headings

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        """Извлечение мета-тегов (title, description, keywords)"""
        meta = {
            "title": None,
            "description": None,
            "keywords": None,
        }

        if soup.title and soup.title.string:
            meta["title"] = soup.title.string.strip()

        for name in ("description", "keywords"):
            tag = soup.find("meta", attrs={"name": name})
            if tag and tag.get("content"):
                meta[name] = tag["content"].strip()

        return meta

    def extract_tables(self, soup: BeautifulSoup) -> list[dict]:
        """Извлечь все таблицы с данными."""
        tables = []

        for table in soup.find_all("table"):
            rows = []
            headers = []

            for th in table.find_all("th"):
                headers.append(th.get_text(strip=True))

            for tr in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if cells:
                    rows.append(cells)

            tables.append({
                "headers": headers,
                "rows": rows,
                "rows_count": len(rows),
            })

        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        """Извлечь все списки ul и ol."""
        lists = []

        for list_type in ("ul", "ol"):
            for lst in soup.find_all(list_type):
                items = [li.get_text(strip=True) for li in lst.find_all("li", recursive=False)]
                if items:
                    lists.append({
                        "type": list_type,
                        "items": items,
                        "items_count": len(items),
                    })

        return lists

class CrawlerWithParser(AsyncCrawler):
    """Расширение AsyncCrawler с парсингом HTML"""

    def __init__(self, max_concurrent: int = 10):
        super().__init__(max_concurrent)
        self.parser = HTMLParser()

    async def fetch_and_parse(self, url: str) -> dict | None:
        """Загрузить страницу и распарсить"""
        html = await self.fetch_url(url)
        if html is None:
            return None
        return await self.parser.parse_html(html, url)

async def demo():
    """Демонстрация парсинга"""
    urls = [
        "https://example.com",
        "https://httpbin.org/html",
        "https://www.python.org",
    ]

    print()
    print("ПАРСИНГ HTML")

    crawler = CrawlerWithParser(max_concurrent=5)

    start = time.time()
    tasks = [crawler.fetch_and_parse(url) for url in urls]
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start

    await crawler.close()

    for url, result in zip(urls, results):
        if result is None:
            print(f"\n Не удалось: {url}")
            continue

        print(f"\n URL: {result['url']}")
        print(f" Title: {result['title']}")
        print(f" Длина текста: {result['text_length']}")
        print(f" Ссылок: {result['links_count']}")
        print(f" Изображений: {result['images_count']}")
        print(f" Заголовков: {len(result['headings'])}")
        print(f" Таблиц: {result['tables_count']}")
        print(f" Списков: {result['lists_count']}")

        if result["headings"]:
            print(f"   Первые заголовки:")
            for h in result["headings"][:3]:
                print(f"      [{h['level']}] {h['text'][:60]}")

        if result["metadata"].get("description"):
            desc = result["metadata"]["description"]
            print(f" Description: {desc[:80]}")

        if result["tables"]:
            t = result["tables"][0]
            print(f"   Первая таблица: {t['rows_count']} строк")
            if t["headers"]:
                print(f"      Заголовки: {t['headers'][:5]}")

        if result["lists"]:
            lst = result["lists"][0]
            print(f"   Первый список ({lst['type']}): {lst['items_count']} элементов")
            print(f"      {lst['items'][:3]}")

    print()
    print(f"  Общее время: {elapsed:.2f} сек")

async def demo_broken_html():
    """Тест обработки битого HTML"""
    print()
    print("ТЕСТ БИТОГО HTML")

    parser = HTMLParser()

    # 1. Пустая строка
    result = await parser.parse_html("", "https://example.com")
    print(f"\n Пустой HTML:")
    print(f"   Title: {result['title']}")
    print(f"   Links: {result['links_count']}")
    print(f"   Text length: {result['text_length']}")

    # 2. Невалидный HTML (незакрытые теги)
    broken = "<html><body><p>Не закрыт<p>Ещё один<div>текст"
    result = await parser.parse_html(broken, "https://example.com")
    print(f"\n Битый HTML (незакрытые теги):")
    print(f"   Text: {result['text'][:80]}")
    print(f"   (не упало)")  

    # 3. Не строка
    result = await parser.parse_html(None, "https://example.com")
    print(f"\n None:")
    print(f"   Title: {result['title']}")
    print(f"   (не упало)")    


async def main():
    await demo()
    await demo_broken_html()


if __name__ == "__main__":
    asyncio.run(main())    