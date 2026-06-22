import asyncio
import unittest

from aiohttp import web

from circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
)
from crawler import AsyncCrawler


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds: float):
        self.value += seconds


class CircuitBreakerStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_opens_blocks_and_recovers(self):
        clock = FakeClock()
        breaker = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout=10,
            failure_window=60,
            clock=clock,
        )

        await breaker.record_failure("example.com")
        await breaker.record_failure("example.com")
        self.assertEqual(
            await breaker.get_state("example.com"),
            CircuitState.OPEN,
        )

        with self.assertRaises(CircuitOpenError):
            await breaker.before_request("example.com")

        clock.advance(10)
        await breaker.before_request("example.com")
        self.assertEqual(
            await breaker.get_state("example.com"),
            CircuitState.HALF_OPEN,
        )

        await breaker.record_success("example.com")
        self.assertEqual(
            await breaker.get_state("example.com"),
            CircuitState.CLOSED,
        )
        self.assertEqual(breaker.get_stats()["recovered"], 1)

    async def test_half_open_failure_reopens_circuit(self):
        clock = FakeClock()
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_timeout=5,
            clock=clock,
        )
        await breaker.record_failure("example.com")
        clock.advance(5)
        await breaker.before_request("example.com")

        await breaker.record_failure("example.com")

        self.assertEqual(
            await breaker.get_state("example.com"),
            CircuitState.OPEN,
        )
        self.assertEqual(breaker.get_stats()["opened"], 2)

    async def test_only_one_half_open_probe_is_allowed(self):
        clock = FakeClock()
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_timeout=5,
            clock=clock,
        )
        await breaker.record_failure("example.com")
        clock.advance(5)

        await breaker.before_request("example.com")
        with self.assertRaises(CircuitOpenError):
            await breaker.before_request("example.com")

    async def test_old_failures_leave_the_window(self):
        clock = FakeClock()
        breaker = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout=5,
            failure_window=3,
            clock=clock,
        )
        await breaker.record_failure("example.com")
        clock.advance(4)
        await breaker.record_failure("example.com")

        self.assertEqual(
            await breaker.get_state("example.com"),
            CircuitState.CLOSED,
        )


class CircuitBreakerCrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.hits = 0
        self.healthy = False

        app = web.Application()
        app.router.add_get("/service", self._service)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        port = self.site._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/service"

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def _service(self, request):
        self.hits += 1
        if not self.healthy:
            return web.Response(status=503)
        return web.Response(text="healthy")

    async def test_crawler_blocks_domain_and_automatically_recovers(self):
        crawler = AsyncCrawler(
            requests_per_second=1000,
            respect_robots=False,
            max_retries=0,
            circuit_breaker_enabled=True,
            circuit_failure_threshold=2,
            circuit_recovery_timeout=0.05,
            circuit_failure_window=10,
        )
        try:
            self.assertEqual(await crawler.fetch_url(self.url), "")
            self.assertEqual(await crawler.fetch_url(self.url), "")
            self.assertEqual(await crawler.fetch_url(self.url), "")
            self.assertEqual(self.hits, 2)
            self.assertEqual(
                crawler.error_details[self.url]["type"],
                "CircuitOpenError",
            )

            await asyncio.sleep(0.06)
            self.healthy = True
            self.assertEqual(await crawler.fetch_url(self.url), "healthy")
        finally:
            await crawler.close()

        stats = crawler.get_circuit_breaker_stats()
        self.assertEqual(stats["opened"], 1)
        self.assertEqual(stats["blocked_requests"], 1)
        self.assertEqual(stats["recovered"], 1)
        domain = self.url.split("/")[2]
        self.assertEqual(stats["domains"][domain]["state"], "closed")


if __name__ == "__main__":
    unittest.main()
