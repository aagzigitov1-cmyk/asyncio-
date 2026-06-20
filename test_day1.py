import asyncio
import time
import unittest

from aiohttp import ClientTimeout, web

from crawler import AsyncCrawler


class Day1CrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        app = web.Application()
        app.router.add_get("/ok", self._ok)
        app.router.add_get("/missing", self._missing)
        app.router.add_get("/slow", self._slow)

        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()

        port = self.site._server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"

        self.crawler = AsyncCrawler(max_concurrent=5)
        self.crawler.respect_robots = False
        self.crawler.rate_limiter.rps = 1000
        self.crawler.rate_limiter.min_delay = 0
        self.crawler.rate_limiter.jitter = 0

    async def asyncTearDown(self):
        await self.crawler.close()
        await self.runner.cleanup()

    async def _ok(self, request):
        return web.Response(text="valid page")

    async def _missing(self, request):
        return web.Response(status=404)

    async def _slow(self, request):
        await asyncio.sleep(0.15)
        return web.Response(text="slow page")

    async def test_fetch_valid_url(self):
        html = await self.crawler.fetch_url(f"{self.base_url}/ok")
        self.assertEqual(html, "valid page")

    async def test_http_error_does_not_crash(self):
        url = f"{self.base_url}/missing"
        html = await self.crawler.fetch_url(url)
        self.assertEqual(html, "")
        self.assertEqual(self.crawler.failed_urls[url], "HTTP 404")

    async def test_client_error_does_not_crash(self):
        url = "not-a-valid-url"
        html = await self.crawler.fetch_url(url)
        self.assertEqual(html, "")
        self.assertIn("InvalidURL", self.crawler.failed_urls[url])

    async def test_timeout_does_not_crash(self):
        url = f"{self.base_url}/slow"
        self.crawler.timeout = ClientTimeout(total=0.03)
        html = await self.crawler.fetch_url(url)
        self.assertEqual(html, "")
        self.assertEqual(self.crawler.failed_urls[url], "Timeout")

    async def test_parallel_fetch_is_faster_than_sequential(self):
        urls = [f"{self.base_url}/slow?request={index}" for index in range(3)]

        started = time.perf_counter()
        for url in urls:
            await self.crawler.fetch_url(url)
        sequential_time = time.perf_counter() - started

        started = time.perf_counter()
        results = await self.crawler.fetch_urls(urls)
        parallel_time = time.perf_counter() - started

        self.assertTrue(all(results.values()))
        self.assertLess(parallel_time, sequential_time * 0.75)


if __name__ == "__main__":
    unittest.main()
