import asyncio
import unittest

from crawler import AsyncCrawler
from queue_manager import CrawlerQueue
from semaphore_manager import SemaphoreManager


def page(url, links=None):
    return {
        "url": url,
        "title": url,
        "text": "content",
        "links": links or [],
        "metadata": {},
        "images": [],
        "headings": {"h1": [], "h2": [], "h3": []},
        "tables": [],
        "lists": [],
    }


class Day3QueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_priority_deduplication_and_fragments(self):
        queue = CrawlerQueue()
        self.assertTrue(
            await queue.add_url("https://example.com/low", priority=10)
        )
        self.assertTrue(
            await queue.add_url("https://example.com/high", priority=1)
        )
        self.assertTrue(
            await queue.add_url(
                "https://example.com/page#first",
                priority=5,
            )
        )
        self.assertFalse(
            await queue.add_url(
                "https://example.com/page#second",
                priority=5,
            )
        )

        self.assertEqual((await queue.get_next()).url, "https://example.com/high")
        self.assertEqual((await queue.get_next()).url, "https://example.com/page")
        self.assertEqual((await queue.get_next()).url, "https://example.com/low")
        self.assertIsNone(await queue.get_next())


class Day3SemaphoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_global_and_per_domain_limits(self):
        manager = SemaphoreManager(global_limit=2, per_domain_limit=1)
        active = 0
        max_active = 0
        active_by_domain = {}
        max_by_domain = {}
        lock = asyncio.Lock()

        async def worker(url, domain):
            nonlocal active, max_active
            await manager.acquire(url)
            try:
                async with lock:
                    active += 1
                    active_by_domain[domain] = active_by_domain.get(domain, 0) + 1
                    max_active = max(max_active, active)
                    max_by_domain[domain] = max(
                        max_by_domain.get(domain, 0),
                        active_by_domain[domain],
                    )
                await asyncio.sleep(0.02)
            finally:
                async with lock:
                    active -= 1
                    active_by_domain[domain] -= 1
                await manager.release(url)

        await asyncio.gather(
            worker("https://one.example/a", "one.example"),
            worker("https://one.example/b", "one.example"),
            worker("https://two.example/a", "two.example"),
            worker("https://two.example/b", "two.example"),
        )

        self.assertLessEqual(max_active, 2)
        self.assertTrue(all(value <= 1 for value in max_by_domain.values()))
        self.assertEqual(manager.active_tasks, 0)


class Day3CrawlerTests(unittest.IsolatedAsyncioTestCase):
    def make_crawler(self, pages, max_depth=2, max_concurrent=3):
        crawler = AsyncCrawler(
            max_concurrent=max_concurrent,
            max_depth=max_depth,
        )
        active = 0
        max_active = 0

        async def fake_fetch_and_parse(url):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.02)
            active -= 1
            return pages.get(url, page(url))

        crawler.fetch_and_parse = fake_fetch_and_parse
        return crawler, lambda: max_active

    async def test_crawl_is_parallel_and_deduplicates_links(self):
        root = "https://example.com"
        pages = {
            root: page(
                root,
                [
                    f"{root}/a",
                    f"{root}/b",
                    f"{root}/a#fragment",
                    f"{root}/c",
                ],
            ),
            f"{root}/a": page(f"{root}/a"),
            f"{root}/b": page(f"{root}/b"),
            f"{root}/c": page(f"{root}/c"),
        }
        crawler, get_max_active = self.make_crawler(pages)

        results = await crawler.crawl([root], max_pages=10)

        self.assertEqual(set(results), set(pages))
        self.assertEqual(len(crawler.visited_urls), 4)
        self.assertGreater(get_max_active(), 1)

    async def test_depth_and_url_filters(self):
        root = "https://example.com/docs"
        pages = {
            root: page(
                root,
                [
                    f"{root}/a",
                    "https://example.com/private/docs",
                    "https://example.com/other",
                ],
            ),
            f"{root}/a": page(
                f"{root}/a",
                [f"{root}/deep"],
            ),
        }
        crawler, _ = self.make_crawler(pages, max_depth=1)

        results = await crawler.crawl(
            [root],
            max_pages=10,
            include_patterns=["docs"],
            exclude_patterns=["private"],
        )

        self.assertEqual(set(results), {root, f"{root}/a"})
        self.assertNotIn(f"{root}/deep", crawler.visited_urls)

    async def test_multiple_start_domains_are_allowed(self):
        first = "https://one.example"
        second = "https://two.example"
        crawler, _ = self.make_crawler(
            {
                first: page(first),
                second: page(second),
            }
        )

        results = await crawler.crawl(
            [first, second],
            same_domain_only=True,
        )

        self.assertEqual(set(results), {first, second})

    async def test_state_is_reset_between_crawls(self):
        first = "https://example.com/first"
        second = "https://example.com/second"
        pages = {
            first: page(first),
            second: page(second),
        }
        crawler, _ = self.make_crawler(pages)

        await crawler.crawl([first])
        results = await crawler.crawl([second])

        self.assertEqual(set(results), {second})
        self.assertEqual(crawler.visited_urls, {second})

    async def test_max_pages_is_not_exceeded(self):
        root = "https://example.com"
        pages = {
            root: page(
                root,
                [f"{root}/{index}" for index in range(10)],
            )
        }
        crawler, _ = self.make_crawler(pages, max_concurrent=5)

        results = await crawler.crawl([root], max_pages=4)

        self.assertEqual(len(results), 4)
        self.assertEqual(len(crawler.visited_urls), 4)


if __name__ == "__main__":
    unittest.main()
