import asyncio
import time
import unittest

import aiohttp
from aiohttp import web

from crawler import AsyncCrawler
from rate_limiter import RateLimiter
from robots_parser import RobotsParser


class RateLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_domain_rate_limit(self):
        limiter = RateLimiter(
            requests_per_second=20,
            per_domain=True,
        )

        started = time.monotonic()
        await limiter.acquire("example.com")
        await limiter.acquire("example.com")
        await limiter.acquire("example.com")
        elapsed = time.monotonic() - started

        self.assertGreaterEqual(elapsed, 0.09)

    async def test_different_domains_have_independent_limits(self):
        limiter = RateLimiter(
            requests_per_second=10,
            per_domain=True,
        )
        await limiter.acquire("one.example")
        await limiter.acquire("two.example")

        started = time.monotonic()
        await asyncio.gather(
            limiter.acquire("one.example"),
            limiter.acquire("two.example"),
        )
        elapsed = time.monotonic() - started

        self.assertGreaterEqual(elapsed, 0.08)
        self.assertLess(elapsed, 0.17)

    async def test_global_limit_serializes_different_domains(self):
        limiter = RateLimiter(
            requests_per_second=10,
            per_domain=False,
        )
        await limiter.acquire("one.example")

        started = time.monotonic()
        await asyncio.gather(
            limiter.acquire("one.example"),
            limiter.acquire("two.example"),
        )
        elapsed = time.monotonic() - started

        self.assertGreaterEqual(elapsed, 0.18)
        self.assertEqual(limiter.get_stats()["request_count"], 3)

    async def test_minimum_delay_and_stats(self):
        limiter = RateLimiter(
            requests_per_second=1000,
            min_delay=0.05,
        )
        await limiter.acquire("example.com")
        await limiter.acquire("example.com")

        stats = limiter.get_stats()
        self.assertGreaterEqual(stats["average_delay"], 0.045)
        self.assertGreaterEqual(stats["total_wait_time"], 0.045)


class Day4CrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.robots_hits = 0
        self.private_hits = 0
        self.retry_hits = 0
        self.rate_limited_hits = 0
        self.last_user_agent = None

        app = web.Application()
        app.router.add_get("/robots.txt", self._robots)
        app.router.add_get("/private", self._private)
        app.router.add_get("/allowed", self._allowed)
        app.router.add_get("/retry", self._retry)
        app.router.add_get("/rate-limited", self._rate_limited)

        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()

        port = self.site._server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def _robots(self, request):
        self.robots_hits += 1
        return web.Response(
            text=(
                "User-agent: TestBot\n"
                "Disallow: /private\n"
                "Crawl-delay: 1\n"
                "\n"
                "User-agent: *\n"
                "Disallow: /blocked\n"
            )
        )

    async def _private(self, request):
        self.private_hits += 1
        return web.Response(text="private")

    async def _allowed(self, request):
        self.last_user_agent = request.headers.get("User-Agent")
        return web.Response(text="allowed")

    async def _retry(self, request):
        self.retry_hits += 1
        if self.retry_hits < 3:
            return web.Response(status=500)
        return web.Response(text="recovered")

    async def _rate_limited(self, request):
        self.rate_limited_hits += 1
        if self.rate_limited_hits == 1:
            return web.Response(
                status=429,
                headers={"Retry-After": "0"},
            )
        return web.Response(text="available")

    async def test_robots_parser_honors_user_agent_and_cache(self):
        parser = RobotsParser()
        await parser.fetch_robots(
            self.base_url,
            user_agent="TestBot",
        )
        async with aiohttp.ClientSession() as session:
            await parser.fetch_robots(
                self.base_url,
                session,
                "TestBot",
            )

        self.assertEqual(self.robots_hits, 1)
        self.assertFalse(
            parser.can_fetch(f"{self.base_url}/private", "TestBot")
        )
        self.assertTrue(
            parser.can_fetch(f"{self.base_url}/allowed", "TestBot")
        )
        self.assertFalse(
            parser.can_fetch(f"{self.base_url}/blocked", "OtherBot")
        )
        self.assertEqual(
            parser.get_crawl_delay("TestBot", self.base_url),
            1.0,
        )

    async def test_crawler_blocks_robots_and_uses_configured_user_agent(self):
        crawler = AsyncCrawler(
            requests_per_second=1000,
            respect_robots=True,
            min_delay=0,
            jitter=0,
            user_agent="TestBot",
            max_retries=0,
        )
        try:
            blocked = await crawler.fetch_url(f"{self.base_url}/private")
            allowed = await crawler.fetch_url(f"{self.base_url}/allowed")
        finally:
            await crawler.close()

        self.assertEqual(blocked, "")
        self.assertEqual(allowed, "allowed")
        self.assertEqual(self.private_hits, 0)
        self.assertEqual(crawler.blocked_by_robots, 1)
        self.assertEqual(self.last_user_agent, "TestBot")

    async def test_crawler_applies_crawl_delay(self):
        crawler = AsyncCrawler(
            requests_per_second=1000,
            respect_robots=True,
            user_agent="TestBot",
            max_retries=0,
        )
        try:
            await crawler.fetch_url(f"{self.base_url}/allowed?request=1")
            started = time.monotonic()
            await crawler.fetch_url(f"{self.base_url}/allowed?request=2")
            elapsed = time.monotonic() - started
        finally:
            await crawler.close()

        self.assertGreaterEqual(elapsed, 0.9)

    async def test_exponential_backoff_recovers_from_server_errors(self):
        crawler = AsyncCrawler(
            requests_per_second=1000,
            respect_robots=False,
            user_agent="TestBot",
            max_retries=2,
            backoff_base=0.02,
        )
        try:
            started = time.monotonic()
            result = await crawler.fetch_url(f"{self.base_url}/retry")
            elapsed = time.monotonic() - started
        finally:
            await crawler.close()

        self.assertEqual(result, "recovered")
        self.assertEqual(self.retry_hits, 3)
        self.assertGreaterEqual(elapsed, 0.05)
        self.assertEqual(crawler.get_rate_stats()["request_count"], 3)

    async def test_429_retry_is_counted(self):
        crawler = AsyncCrawler(
            requests_per_second=1000,
            respect_robots=False,
            max_retries=1,
            backoff_base=0.01,
        )
        try:
            result = await crawler.fetch_url(
                f"{self.base_url}/rate-limited"
            )
        finally:
            await crawler.close()

        self.assertEqual(result, "available")
        self.assertEqual(crawler.rate_limited_count, 1)


if __name__ == "__main__":
    unittest.main()
