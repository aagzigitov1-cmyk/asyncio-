import unittest
from unittest.mock import AsyncMock, patch

from bs4 import BeautifulSoup

from crawler import AsyncCrawler
from parser import HTMLParser


HTML = """
<html>
  <head>
    <title>Example title</title>
    <meta name="description" content="Example description">
    <meta name="keywords" content="asyncio,crawler">
  </head>
  <body>
    <h1>Main heading</h1>
    <h2>Section</h2>
    <a href="/relative">Relative</a>
    <a href="https://external.example/page">External</a>
    <a href="mailto:user@example.com">Email</a>
    <a href="http:missing-host">Invalid</a>
    <img src="/image.png" alt="Example image">
    <table><tr><th>Name</th></tr><tr><td>Value</td></tr></table>
    <ul><li>First</li><li>Second</li></ul>
  </body>
</html>
"""


class Day2ParserTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.parser = HTMLParser()

    async def test_parse_valid_html(self):
        result = await self.parser.parse_html(
            HTML,
            "https://example.com/base/page",
        )

        self.assertEqual(result["title"], "Example title")
        self.assertEqual(
            result["metadata"]["description"],
            "Example description",
        )
        self.assertIn("Main heading", result["text"])
        self.assertEqual(result["headings"]["h1"], ["Main heading"])
        self.assertEqual(
            result["images"],
            [
                {
                    "src": "https://example.com/image.png",
                    "alt": "Example image",
                }
            ],
        )
        self.assertEqual(result["tables"], [[["Name"], ["Value"]]])
        self.assertEqual(result["lists"], [["First", "Second"]])

    async def test_relative_links_are_absolute_and_invalid_links_are_filtered(self):
        result = await self.parser.parse_html(
            HTML,
            "https://example.com/base/page",
        )

        self.assertEqual(
            result["links"],
            [
                "https://example.com/relative",
                "https://external.example/page",
            ],
        )

    async def test_broken_html_returns_available_data(self):
        result = await self.parser.parse_html(
            "<html><title>Broken<h1>Still readable<a href='/page'>Page",
            "https://example.com",
        )

        self.assertTrue(result["text"])
        self.assertEqual(result["links"], ["https://example.com/page"])

    async def test_extractor_error_keeps_partial_result(self):
        with patch.object(
            self.parser,
            "extract_images",
            side_effect=ValueError("bad image"),
        ):
            result = await self.parser.parse_html(
                HTML,
                "https://example.com",
            )

        self.assertEqual(result["images"], [])
        self.assertEqual(result["title"], "Example title")
        self.assertTrue(result["text"])
        self.assertTrue(result["links"])

    async def test_fetch_and_parse_integration(self):
        crawler = AsyncCrawler()
        crawler.fetch_url = AsyncMock(return_value=HTML)

        result = await crawler.fetch_and_parse("https://example.com")

        crawler.fetch_url.assert_awaited_once_with("https://example.com")
        self.assertEqual(result["title"], "Example title")
        self.assertEqual(result["url"], "https://example.com")

    def test_extract_text_with_selector(self):
        soup = BeautifulSoup(
            "<main><p>First</p><p>Second</p></main><footer>Ignore</footer>",
            "lxml",
        )

        self.assertEqual(
            self.parser.extract_text(soup, "main p"),
            "First Second",
        )


if __name__ == "__main__":
    unittest.main()
