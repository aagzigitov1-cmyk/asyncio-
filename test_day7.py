import json
import logging
import tempfile
import unittest
from pathlib import Path

from aiohttp import web

from benchmark import run_scalability_benchmark
from crawler import AdvancedCrawler, _build_cli_parser
from crawler_stats import CrawlerStats
from logging_setup import configure_logging
from sitemap_parser import SitemapParser


class SitemapTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        app = web.Application()
        app.router.add_get("/sitemap.xml", self._index)
        app.router.add_get("/one.xml", self._one)
        app.router.add_get("/two.xml", self._two)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        port = self.site._server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def _index(self, request):
        return web.Response(
            content_type="application/xml",
            text=(
                "<?xml version='1.0'?>"
                "<sitemapindex xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
                f"<sitemap><loc>{self.base_url}/one.xml</loc></sitemap>"
                f"<sitemap><loc>{self.base_url}/two.xml</loc></sitemap>"
                "</sitemapindex>"
            ),
        )

    async def _one(self, request):
        return web.Response(
            content_type="application/xml",
            text=(
                "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
                f"<url><loc>{self.base_url}/page/1</loc></url>"
                f"<url><loc>{self.base_url}/page/2</loc></url>"
                "</urlset>"
            ),
        )

    async def _two(self, request):
        return web.Response(
            content_type="application/xml",
            text=(
                "<urlset>"
                f"<url><loc>{self.base_url}/page/2</loc></url>"
                f"<url><loc>{self.base_url}/page/3</loc></url>"
                "</urlset>"
            ),
        )

    async def test_recursive_sitemap_index_and_deduplication(self):
        urls = await SitemapParser().fetch_sitemap(
            f"{self.base_url}/sitemap.xml"
        )

        self.assertEqual(
            urls,
            [
                f"{self.base_url}/page/1",
                f"{self.base_url}/page/2",
                f"{self.base_url}/page/3",
            ],
        )

    async def test_advanced_crawler_uses_sitemap_as_source(self):
        crawler = AdvancedCrawler(
            sitemap_urls=[f"{self.base_url}/sitemap.xml"],
            max_pages=3,
            show_progress=False,
        )

        async def fake_fetch_and_parse(url):
            return {
                "url": url,
                "title": url,
                "text": "page",
                "links": [],
                "metadata": {},
                "status_code": 200,
                "content_type": "text/html",
            }

        crawler.fetch_and_parse = fake_fetch_and_parse
        try:
            results = await crawler.crawl()
        finally:
            await crawler.close()

        self.assertEqual(len(results), 3)
        self.assertEqual(crawler.get_stats()["successful"], 3)


class StatsAndReportsTests(unittest.TestCase):
    def test_stats_progress_and_exports(self):
        stats = CrawlerStats()
        stats.start()
        stats.record_success("https://one.example/a", 200)
        stats.record_success("https://one.example/b", 201)
        stats.record_failure("https://two.example/missing", 404)
        stats.finish()

        snapshot = stats.get_stats()
        progress = stats.progress(
            max_pages=10,
            active_tasks=2,
            queued=5,
        )
        self.assertEqual(snapshot["total_pages"], 3)
        self.assertEqual(snapshot["status_codes"]["404"], 1)
        self.assertEqual(snapshot["top_domains"][0]["domain"], "one.example")
        self.assertEqual(progress["percentage"], 30.0)
        self.assertEqual(progress["active_tasks"], 2)

        with tempfile.TemporaryDirectory() as directory:
            json_path = Path(directory) / "stats.json"
            html_path = Path(directory) / "report.html"
            stats.export_to_json(str(json_path))
            stats.export_to_html_report(str(html_path))

            exported = json.loads(json_path.read_text(encoding="utf-8"))
            report = html_path.read_text(encoding="utf-8")

        self.assertEqual(exported["successful"], 2)
        self.assertIn("Async crawler report", report)
        self.assertIn("one.example", report)


class ConfigurationAndCLITests(unittest.IsolatedAsyncioTestCase):
    async def test_advanced_crawler_from_json_config(self):
        with tempfile.TemporaryDirectory() as directory:
            result_path = Path(directory) / "results.jsonl"
            config_path = Path(directory) / "crawler.json"
            config_path.write_text(
                json.dumps(
                    {
                        "start_urls": ["https://example.com"],
                        "max_pages": 1,
                        "show_progress": False,
                        "crawler": {
                            "max_concurrent": 3,
                            "max_depth": 1,
                            "rate_limit": 1000,
                            "respect_robots": False,
                        },
                        "filters": {"same_domain_only": True},
                        "storage": {
                            "format": "jsonl",
                            "path": str(result_path),
                            "options": {"buffer_size": 1},
                        },
                    }
                ),
                encoding="utf-8",
            )
            crawler = AdvancedCrawler.from_config(str(config_path))

            async def fake_fetch_and_parse(url):
                return {
                    "url": url,
                    "title": "Configured",
                    "text": "content",
                    "links": [],
                    "metadata": {},
                    "status_code": 200,
                    "content_type": "text/html",
                }

            crawler.fetch_and_parse = fake_fetch_and_parse
            await crawler.crawl()
            await crawler.close()
            stored = await crawler.storage.read_all()

        self.assertEqual(crawler.max_concurrent, 3)
        self.assertEqual(crawler.max_depth, 1)
        self.assertEqual(len(stored), 1)

    async def test_realtime_progress_callback(self):
        events = []
        crawler = AdvancedCrawler(
            start_urls=["https://example.com/1", "https://example.com/2"],
            max_pages=2,
            show_progress=False,
        )
        crawler.progress_callback = events.append

        async def fake_fetch_and_parse(url):
            return {
                "url": url,
                "title": url,
                "text": "content",
                "links": [],
                "metadata": {},
                "status_code": 200,
                "content_type": "text/html",
            }

        crawler.fetch_and_parse = fake_fetch_and_parse
        try:
            await crawler.crawl()
        finally:
            await crawler.close()

        self.assertTrue(events)
        self.assertEqual(events[-1]["completed"], 2)
        self.assertIn("eta_seconds", events[-1])

    def test_cli_arguments(self):
        args = _build_cli_parser().parse_args(
            [
                "--urls",
                "https://example.com",
                "--max-pages",
                "25",
                "--max-depth",
                "3",
                "--output",
                "results.csv",
                "--respect-robots",
                "--rate-limit",
                "2.5",
            ]
        )

        self.assertEqual(args.urls, ["https://example.com"])
        self.assertEqual(args.max_pages, 25)
        self.assertEqual(args.max_depth, 3)
        self.assertTrue(args.respect_robots)
        self.assertEqual(args.rate_limit, 2.5)


class LoggingAndPerformanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_rotating_file_logging(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "crawler.log"
            handlers = configure_logging(
                log_file=str(log_path),
                level="DEBUG",
                max_bytes=120,
                backup_count=2,
            )
            logger = logging.getLogger("rotation-test")
            for index in range(20):
                logger.info("message %s %s", index, "x" * 40)
            for handler in handlers:
                handler.flush()

            files = list(Path(directory).glob("crawler.log*"))
            configure_logging(level="INFO")

        self.assertGreaterEqual(len(files), 2)

    async def test_small_scalability_benchmark(self):
        results = await run_scalability_benchmark(
            page_counts=(20, 50),
            simulated_latency=0.001,
            max_concurrent=10,
        )

        self.assertEqual([item["pages"] for item in results], [20, 50])
        self.assertTrue(all(item["speedup"] > 1 for item in results))
        self.assertTrue(all(item["peak_memory_mb"] > 0 for item in results))


if __name__ == "__main__":
    unittest.main()
