# Async Crawler (Days 1-7)

Полнофункциональный асинхронный краулер на Python. Полный цикл: скачивание → парсинг → обход → сохранение → отчёты.

## Возможности

### Ядро
- **Async HTTP** через `aiohttp` (Day 1)
- **HTML-парсинг** через `BeautifulSoup` (Day 2)
- **BFS-обход** с фильтрами и глубиной (Day 3)

### Вежливость и надёжность
- **Rate limiting** (запросов/сек, per-domain) (Day 4)
- **robots.txt** — собственный парсер с кэшем (Day 4)
- **Retry** с экспоненциальным backoff (Day 5)
- **Circuit breaker** — защита от «мёртвых» доменов (Day 5)

### Данные
- **Хранилище**: JSON (JSONL), CSV, SQLite (Day 6)
- **Батч-вставки** и индексы в SQLite
- **Дедупликация** через `UNIQUE url`

### Финальные штрихи (Day 7)
- **Sitemap.xml** — urlset и sitemapindex с рекурсией
- **Статистика**: страниц, коды, домены, типы ошибок, rps
- **Отчёты**: JSON и HTML с таблицами и карточками
- **Конфиг YAML** + **CLI** через `argparse`
- **Логирование** в файл с ротацией

## Установка

```bash
pip install aiohttp aiofiles aiosqlite beautifulsoup4 lxml pyyaml