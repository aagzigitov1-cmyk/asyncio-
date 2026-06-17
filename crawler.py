import asyncio
import logging
from parser import HTMLParser
import aiohttp
import time
from urllib.parse import urlparse
from queue_manager import CrawlerQueue
from semaphore_manager import SemaphoreManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


class AsyncCrawler:
    def __init__(
            self,
            max_concurrent: int = 10,
            max_depth: int = 2,
            per_domain_limit: int = 2,
        ):
        self.max_concurrent = max_concurrent


        self.timeout = aiohttp.ClientTimeout(
            total=30,
            connect=10,
            sock_read=20,
        )

        self.session: aiohttp.ClientSession | None = None

        self.parser = HTMLParser()
        self.max_depth = max_depth

        self.queue = CrawlerQueue()

        self.semaphore_manager = SemaphoreManager(
            global_limit=max_concurrent,
            per_domain_limit=per_domain_limit,
        )

        self.visited_urls: set[str] = set()

        self.failed_urls: dict[str, str] = {}

        self.processed_urls: dict[str, dict] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=self.timeout
            )

        return self.session


    def _is_same_domain(
        self,
        url: str,
        root_domain: str,
    ) -> bool:

        return (
            urlparse(url).netloc
            == root_domain
        )


    def _matches_patterns(
        self,
        url: str,
        include_patterns: list[str] | None,
        exclude_patterns: list[str] | None,
    ) -> bool:

        if include_patterns:

            if not any(
                pattern in url
                for pattern in include_patterns
            ):
                return False

        if exclude_patterns:

            if any(
                pattern in url
                for pattern in exclude_patterns
            ):
                return False

        return True


    def _should_visit(
        self,
        url: str,
        depth: int,
        root_domain: str,
        same_domain_only: bool,
        include_patterns: list[str] | None,
        exclude_patterns: list[str] | None,
    ) -> bool:

        if depth > self.max_depth:
            return False


        if same_domain_only:

            if not self._is_same_domain(
                url,
                root_domain,
            ):
                return False

        if not self._matches_patterns(
            url,
            include_patterns,
            exclude_patterns,
        ):
            return False

        return True



    async def fetch_url(
        self,
        url: str,
    ) -> str:

        await self.semaphore_manager.acquire(
            url
        )

        try:

            logging.info(
                f"▶️ Start: {url}"
            )

            try:

                session = await self._get_session()

                async with session.get(
                    url
                ) as response:

                    response.raise_for_status()

                    html = await response.text()

                    logging.info(
                        f"✅ Success: {url}"
                    )

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

        finally:

            self.semaphore_manager.release(
                url
            )

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
            return_exceptions=True,
        )

        return {
            url: result
            for url, result in zip(urls, results)
            if isinstance(result, str) and result
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


    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = True,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> dict[str, dict]:

        if not start_urls:
            return {}

        start_time = time.perf_counter()

        root_domain = urlparse(
            start_urls[0]
        ).netloc

        for url in start_urls:

            await self.queue.add_url(
                url=url,
                depth=0,
            )

        idle_rounds = 0

        while len(self.processed_urls) < max_pages:

            try:
                item = await asyncio.wait_for(
                    self.queue.get_next(),
                    timeout=5
                )

            except asyncio.TimeoutError:

                if self.semaphore_manager.active_tasks == 0:
                    break

                idle_rounds += 1

                if idle_rounds > 3:
                    break

                continue

            idle_rounds = 0

            if not item or not item.url:
                continue

            if item.url in self.visited_urls:
                continue

            if not self._should_visit(
                url=item.url,
                depth=item.depth,
                root_domain=root_domain,
                same_domain_only=same_domain_only,
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns,
            ):
                continue

            self.visited_urls.add(item.url)


            try:

                result = await self.fetch_and_parse(
                    item.url
                )

                if not result or (
                    not result.get("title")
                    and not result.get("text")
                ):

                    self.failed_urls[
                        item.url
                    ] = "Empty result"

                    self.queue.mark_failed(
                        item.url,
                        "Empty result",
                    )

                    continue

                self.processed_urls[
                    item.url
                ] = result

                self.queue.mark_processed(
                    item.url
                )

                next_depth = item.depth + 1

                if next_depth <= self.max_depth:

                    for link in result.get("links", []):

                        await self.queue.add_url(
                            url=link,
                            depth=next_depth,
                        )
                stats = self.queue.get_stats()

                elapsed = max(
                    time.perf_counter() - start_time,
                    0.001,
                )

                speed = (
                    stats["processed"]
                    / elapsed
                )

                logging.info(
                    f"📄 Processed={stats['processed']} "
                    f"| Queue={stats['queued']} "
                    f"| Errors={stats['failed']} "
                    f"| Speed={speed:.2f} pages/sec"
                )

            except Exception as e:

                self.failed_urls[
                    item.url
                ] = str(e)

                self.queue.mark_failed(
                    item.url,
                    str(e),
                )

        return self.processed_urls


    async def close(self):
        if self.session is not None:
            await self.session.close()