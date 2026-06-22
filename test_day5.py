import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path

from aiohttp import web

from crawler import AsyncCrawler
from retry_strategy import (
    NetworkError,
    ParseError,
    PermanentError,
    RetryStrategy,
    TransientError,
)


class RetryStrategyTests(unittest.IsolatedAsyncioTestCase):
    async def test_transient_error_retries_with_exponential_backoff(self):
        delays = []
        calls = 0

        async def fake_sleep(delay):
            delays.append(delay)

        async def operation():
            nonlocal calls
            calls += 1
            if calls < 3:
                raise TransientError("temporary", url="https://example.com")
            return "ok"

        strategy = RetryStrategy(
            max_retries=3,
            backoff_factor=0.5,
            sleep_func=fake_sleep,
        )

        result = await strategy.execute_with_retry(operation)

        self.assertEqual(result, "ok")
        self.assertEqual(calls, 3)
        self.assertEqual(delays, [0.5, 1.0])
        self.assertEqual(strategy.get_stats()["successful_retries"], 1)

    async def test_permanent_error_is_not_retried(self):
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise PermanentError(
                "not found",
                url="https://example.com/missing",
                status=404,
            )

        strategy = RetryStrategy(max_retries=5)

        with self.assertRaises(PermanentError):
            await strategy.execute_with_retry(operation)

        stats = strategy.get_stats()
        self.assertEqual(calls, 1)
        self.assertEqual(
            stats["permanent_error_urls"],
            ["https://example.com/missing"],
        )

    async def test_error_types_can_have_different_limits_and_backoff(self):
        delays = []
        calls = 0

        async def fake_sleep(delay):
            delays.append(delay)

        async def operation():
            nonlocal calls
            calls += 1
            raise NetworkError("dns", url="https://example.com")

        strategy = RetryStrategy(
            max_retries=4,
            retry_limits={NetworkError: 1},
            backoff_factors={NetworkError: 0.75},
            sleep_func=fake_sleep,
        )

        with self.assertRaises(NetworkError):
            await strategy.execute_with_retry(operation)

        self.assertEqual(calls, 2)
        self.assertEqual(delays, [0.75])

    async def test_retry_types_are_configurable(self):
        calls = 0

        async def no_sleep(delay):
            return None

        async def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ParseError("broken document")
            return "parsed"

        strategy = RetryStrategy(
            max_retries=1,
            retry_on=[ParseError],
            sleep_func=no_sleep,
        )

        self.assertEqual(
            await strategy.execute_with_retry(operation),
            "parsed",
        )


class Day5CrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service_hits = 0
        self.not_found_hits = 0
        self.slow_hits = 0

        app = web.Application()
        app.router.add_get("/service", self._service)
        app.router.add_get("/missing", self._missing)
        app.router.add_get("/slow", self._slow)

        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()

        port = self.site._server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def _service(self, request):
        self.service_hits += 1
        if self.service_hits < 3:
            return web.Response(status=503)
        return web.Response(text="service restored")

    async def _missing(self, request):
        self.not_found_hits += 1
        return web.Response(status=404)

    async def _slow(self, request):
        self.slow_hits += 1
        await asyncio.sleep(0.06)
        return web.Response(text="slow success")

    def make_crawler(self, **kwargs):
        return AsyncCrawler(
            requests_per_second=1000,
            respect_robots=False,
            min_delay=0,
            jitter=0,
            **kwargs,
        )

    async def test_503_is_retried_and_success_is_recorded(self):
        crawler = self.make_crawler(
            max_retries=2,
            backoff_base=0.01,
        )
        try:
            result = await crawler.fetch_url(f"{self.base_url}/service")
        finally:
            await crawler.close()

        stats = crawler.get_error_stats()
        self.assertEqual(result, "service restored")
        self.assertEqual(self.service_hits, 3)
        self.assertEqual(stats["errors_by_type"]["TransientError"], 2)
        self.assertEqual(stats["successful_retries"], 1)
        self.assertGreater(stats["average_retry_wait"], 0)

    async def test_404_is_permanent_and_not_retried(self):
        url = f"{self.base_url}/missing"
        crawler = self.make_crawler(
            max_retries=3,
            backoff_base=0.01,
        )
        try:
            result = await crawler.fetch_url(url)
        finally:
            await crawler.close()

        self.assertEqual(result, "")
        self.assertEqual(self.not_found_hits, 1)
        self.assertEqual(
            crawler.error_details[url]["type"],
            "PermanentError",
        )
        self.assertIn(
            url,
            crawler.get_error_stats()["permanent_error_urls"],
        )

    async def test_timeout_grows_and_second_attempt_succeeds(self):
        crawler = self.make_crawler(
            max_retries=1,
            backoff_base=0.001,
            connect_timeout=0.03,
            read_timeout=0.03,
            total_timeout=0.03,
            timeout_growth=5.0,
        )
        try:
            result = await crawler.fetch_url(f"{self.base_url}/slow")
        finally:
            await crawler.close()

        self.assertEqual(result, "slow success")
        self.assertGreaterEqual(self.slow_hits, 1)
        stats = crawler.get_error_stats()
        self.assertEqual(stats["retry_attempts"], 1)
        self.assertEqual(stats["successful_retries"], 1)

    async def test_error_report_is_saved(self):
        url = f"{self.base_url}/missing"
        crawler = self.make_crawler(max_retries=0)
        try:
            await crawler.fetch_url(url)
            with tempfile.TemporaryDirectory() as directory:
                report_path = Path(directory) / "errors.json"
                await crawler.save_error_report(str(report_path))
                report = json.loads(report_path.read_text(encoding="utf-8"))
        finally:
            await crawler.close()

        self.assertIn(url, report["errors"])
        self.assertIn(url, report["statistics"]["permanent_error_urls"])


if __name__ == "__main__":
    unittest.main()
