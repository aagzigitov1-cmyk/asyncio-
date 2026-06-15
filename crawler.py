import asyncio
import logging
from parser import HTMLParser
import aiohttp


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent

        self.semaphore = asyncio.Semaphore(max_concurrent)

        self.timeout = aiohttp.ClientTimeout(
            total=30,
            connect=10,
            sock_read=20,
        )

        self.session: aiohttp.ClientSession | None = None

        self.parser = HTMLParser()
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=self.timeout
            )

        return self.session

    async def fetch_url(self, url: str) -> str:
        async with self.semaphore:
            logging.info(f"▶️ Start: {url}")

            try:
                session = await self._get_session()

                async with session.get(url) as response:
                    response.raise_for_status()

                    html = await response.text()

                    logging.info(f"✅ Success: {url}")

                    return html

            except aiohttp.ClientResponseError as e:
                logging.error(
                    f"🚫 HTTP error | {url} | status={e.status}"
                )

            except aiohttp.ClientError as e:
                logging.error(
                    f"❌ Client error | {url} | {e}"
                )

            except asyncio.TimeoutError:
                logging.error(
                    f"⏰ Timeout | {url}"
                )

            except Exception as e:
                logging.error(
                    f"⚠️ Unknown error | {url} | {e}"
                )

            return ""

    async def fetch_urls(
        self,
        urls: list[str]
    ) -> dict[str, str]:

        tasks = [
            self.fetch_url(url)
            for url in urls
        ]

        results = await asyncio.gather(
        *tasks,
        return_exceptions=True
)

        return {
            url: result
            for url, result in zip(urls, results)
            if result
        }


    async def fetch_and_parse(
        self,
        url: str
    ) -> dict:

        html = await self.fetch_url(url)

        if not html:
            return {
                "url": url,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {},
                "images": [],
                "headings": {
                    "h1": [],
                    "h2": [],
                    "h3": [],
                },
                "tables": [],
                "lists": [],
            }

        return await self.parser.parse_html(
            html,
            url
        )

    async def fetch_and_parse_many(
        self,
        urls: list[str]
    ) -> list[dict]:

        tasks = [
            self.fetch_and_parse(url)
            for url in urls
        ]

        return await asyncio.gather(
            *tasks
        )

    async def close(self):
        if self.session is not None:
            await self.session.close()