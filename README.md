# Advanced Async Crawler

Учебный асинхронный веб-краулер, последовательно собранный за семь этапов. Он поддерживает параллельную загрузку, HTML-парсинг, обход ссылок, robots.txt, rate limiting, повторы, Circuit Breaker, JSON/CSV/SQLite и финальный CLI.

## Возможности

- `aiohttp.ClientSession`, connection pooling и настраиваемые таймауты;
- очередь URL с приоритетами, глубиной, фильтрами и дедупликацией;
- извлечение текста, ссылок, metadata, изображений, заголовков, таблиц и списков;
- глобальные и доменные семафоры, rate limiting, jitter и Crawl-delay;
- соблюдение robots.txt и настраиваемый User-Agent;
- классификация ошибок, exponential backoff и Circuit Breaker;
- асинхронное сохранение в JSON Lines, CSV и SQLite;
- sitemap.xml и рекурсивные sitemap index;
- статистика, JSON/HTML-отчёты, прогресс и ротация логов;
- JSON-конфигурация и CLI.

## Установка

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Быстрый запуск

```bash
python crawler.py --urls https://example.com --max-pages 100 \
  --max-depth 2 --rate-limit 2 --respect-robots \
  --output results.jsonl --log-file logs/crawler.log
```

При использовании JSON-конфигурации:

```bash
python crawler.py --config config.example.json
```

CLI создаёт файл данных, `<output>.stats.json` и `<output>.report.html`.

## Python API

```python
import asyncio
from crawler import AdvancedCrawler

async def main():
    crawler = AdvancedCrawler.from_config("config.example.json")
    try:
        results = await crawler.crawl()
        print(crawler.get_stats())
        crawler.export_to_json("stats.json")
        crawler.export_to_html_report("report.html")
    finally:
        await crawler.close()

asyncio.run(main())
```

`AsyncCrawler` остаётся доступен для низкоуровневого использования. Основные методы: `fetch_url`, `fetch_urls`, `fetch_and_parse`, `crawl`, `close`.

## Конфигурация

Поддерживается JSON. Полный пример находится в `config.example.json`.

- `start_urls` — начальные URL;
- `max_pages` — лимит страниц;
- `crawler` — `max_concurrent`, `max_depth`, `rate_limit` или `requests_per_second`, таймауты, robots.txt и retry-настройки;
- `filters` — `same_domain_only`, `include_patterns`, `exclude_patterns`;
- `sitemaps` — sitemap.xml или sitemap index;
- `storage` — `json`, `jsonl`, `csv` или `sqlite`;
- `logging` — файл, уровень, размер и число архивов.

## Хранилища

```python
from crawler import AsyncCrawler
from storage import JSONStorage, CSVStorage, SQLiteStorage

storage = SQLiteStorage("crawler.db", batch_size=100)
crawler = AsyncCrawler(storage=storage)
```

Каждая запись содержит `url`, `title`, `text`, `links`, `metadata`, `crawled_at`, `status_code` и `content_type`.

## Тесты и производительность

```bash
python -m unittest discover -v
python benchmark.py
```

`benchmark.py` сравнивает асинхронную и последовательную обработку 100, 500 и 1000 страниц и измеряет пиковое потребление памяти.

## Структура

- `crawler.py` — AsyncCrawler, AdvancedCrawler и CLI;
- `parser.py` — HTMLParser;
- `sitemap_parser.py` — sitemap и sitemap index;
- `crawler_stats.py` — статистика и отчёты;
- `storage.py` — JSON, CSV и SQLite;
- `retry_strategy.py`, `circuit_breaker.py` — отказоустойчивость;
- `rate_limiter.py`, `robots_parser.py`, `queue_manager.py`, `semaphore_manager.py` — управление обходом.
