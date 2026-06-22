import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from crawler import AsyncCrawler
from storage import (
    CSVStorage,
    DataStorage,
    JSONStorage,
    SQLiteStorage,
    normalize_crawl_data,
)


def sample_page(index: int = 1) -> dict:
    return {
        "url": f"https://example.com/{index}",
        "title": f"Title, \"{index}\"",
        "text": f"Line one\nLine two {index}",
        "links": [f"https://example.com/{index}/next"],
        "metadata": {"description": f"Page {index}"},
        "crawled_at": datetime(2026, 6, 21, tzinfo=timezone.utc),
        "status_code": 200,
        "content_type": "text/html; charset=utf-8",
    }


class StorageFormatTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary_directory.name)

    async def asyncTearDown(self):
        self.temporary_directory.cleanup()

    async def test_json_lines_storage_and_integrity(self):
        path = self.directory / "pages.jsonl"
        storage = JSONStorage(str(path), buffer_size=2)
        await storage.save(sample_page(1))
        await storage.save(sample_page(2))

        rows = await storage.read_all()
        await storage.close()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["metadata"], {"description": "Page 1"})
        self.assertEqual(rows[1]["status_code"], 200)
        self.assertEqual(len(path.read_text(encoding="utf-8").splitlines()), 2)

    async def test_pretty_json_storage(self):
        path = self.directory / "pretty.json"
        storage = JSONStorage(
            str(path),
            json_lines=False,
            indent=2,
        )
        await storage.save(sample_page())
        await storage.close()

        content = path.read_text(encoding="utf-8")
        self.assertIn("\n  {", content)
        self.assertIn('"title": "Title, \\"1\\""', content)

    async def test_csv_storage_handles_special_characters(self):
        path = self.directory / "pages.csv"
        storage = CSVStorage(
            str(path),
            encoding="utf-8",
            buffer_size=1,
        )
        await storage.save(sample_page())

        rows = await storage.read_all()
        await storage.close()

        self.assertEqual(rows[0]["title"], 'Title, "1"')
        self.assertEqual(rows[0]["text"], "Line one\nLine two 1")
        self.assertEqual(rows[0]["links"], ["https://example.com/1/next"])

    async def test_sqlite_batch_storage_and_index(self):
        path = self.directory / "crawler.db"
        storage = SQLiteStorage(str(path), batch_size=10)
        await storage.save_many([sample_page(1), sample_page(2)])

        rows = await storage.read_all()
        cursor = await storage._connection.execute(
            "PRAGMA index_list('pages')"
        )
        indexes = await cursor.fetchall()
        await cursor.close()
        await storage.close()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["links"], ["https://example.com/1/next"])
        self.assertIn("idx_pages_url", {row[1] for row in indexes})
        self.assertEqual(storage.get_stats()["saved"], 2)

    def test_standard_data_is_completed(self):
        normalized = normalize_crawl_data({"url": "https://example.com"})

        self.assertEqual(
            set(normalized),
            {
                "url",
                "title",
                "text",
                "links",
                "metadata",
                "crawled_at",
                "status_code",
                "content_type",
            },
        )
        self.assertIsInstance(normalized["crawled_at"], datetime)


class FlakyStorage(DataStorage):
    def __init__(self, failures_before_success: int):
        super().__init__()
        self.failures_before_success = failures_before_success
        self.attempts = 0
        self.items = []
        self.closed = False

    async def save(self, data: dict) -> None:
        self.attempts += 1
        if self.attempts <= self.failures_before_success:
            raise OSError("temporary storage failure")
        self.items.append(data)
        self.saved_count += 1

    async def close(self) -> None:
        self.closed = True


class CrawlerStorageTests(unittest.IsolatedAsyncioTestCase):
    def make_crawler(self, storage):
        crawler = AsyncCrawler(
            storage=storage,
            storage_retries=2,
            storage_retry_delay=0,
        )

        async def fake_fetch_and_parse(url):
            data = sample_page()
            data["url"] = url
            return data

        crawler.fetch_and_parse = fake_fetch_and_parse
        return crawler

    async def test_crawler_saves_every_processed_page(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = JSONStorage(
                str(Path(directory) / "results.jsonl"),
                buffer_size=1,
            )
            crawler = self.make_crawler(storage)
            results = await crawler.crawl(
                ["https://example.com"],
                max_pages=1,
            )
            await crawler.close()
            rows = await storage.read_all()

        self.assertEqual(len(results), 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["url"], "https://example.com")

    async def test_storage_write_is_retried(self):
        storage = FlakyStorage(failures_before_success=2)
        crawler = self.make_crawler(storage)

        results = await crawler.crawl(
            ["https://example.com"],
            max_pages=1,
        )
        await crawler.close()

        self.assertEqual(len(results), 1)
        self.assertEqual(storage.attempts, 3)
        self.assertEqual(len(storage.items), 1)
        self.assertEqual(crawler.storage_errors, {})

    async def test_storage_failure_does_not_stop_crawl(self):
        storage = FlakyStorage(failures_before_success=100)
        crawler = self.make_crawler(storage)

        results = await crawler.crawl(
            ["https://example.com"],
            max_pages=1,
        )
        await crawler.close()

        self.assertEqual(len(results), 1)
        self.assertIn("https://example.com", crawler.storage_errors)
        self.assertEqual(storage.attempts, 3)


if __name__ == "__main__":
    unittest.main()
