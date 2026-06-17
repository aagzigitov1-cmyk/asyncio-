import asyncio
import logging
from parser import HTMLParser
import aiohttp
import time
from urllib.parse import urlparse
from queue_manager import CrawlerQueue
from semaphore_manager import SemaphoreManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
import random


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


        self.rate_limiter = RateLimiter(
            requests_per_second=2.0,
            per_domain=True,
            min_delay=0.5,
            jitter=0.2,
        )

        # =========================
        # USER AGENT SYSTEM
        # =========================
  

        self.user_agents = [
            "MyCrawler/1.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        ]

        self.robots = RobotsParser()
        self.respect_robots = True

        self.blocked_by_robots = 0
        self.rate_limited_count = 0








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

    def _get_user_agent(self) -> str:
        return random.choice(self.user_agents)

    async def fetch_url(self, url: str) -> str:
        domain = urlparse(url).netloc
        session = await self._get_session()

        # =========================
        # 1. ROBOTS.TXT
        # =========================
        if self.respect_robots:
            rules = await self.robots.fetch_robots(
                f"https://{domain}",
                session,
            )

            if not self.robots.can_fetch(url, rules):
                logging.warning(f"🚫 robots blocked: {url}")
                self.blocked_by_robots += 1
                self.failed_urls[url] = "Blocked by robots.txt"
                return ""

            delay = self.robots.get_delay(rules)
            if delay:
                await asyncio.sleep(delay)

        # =========================
        # 2. RATE LIMITER
        # =========================
        await self.rate_limiter.acquire(domain)

        # =========================
        # 3. SEMAPHORE
        # =========================
        await self.semaphore_manager.acquire(url)

        try:
            logging.info(f"▶️ Start: {url}")

            headers = {
                "User-Agent": self._get_user_agent()
            }

            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                html = await response.text()

                logging.info(f"✅ Success: {url}")
                return html

        except aiohttp.ClientResponseError as e:

            if e.status == 429:
                self.rate_limited_count += 1
                await asyncio.sleep(2)  # простой backoff

            logging.error(f"❌ HTTP error {url} | {e.status}")
            return ""

        finally:
            await self.semaphore_manager.release(url)

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
            url: result if isinstance(result, str) else ""
            for url, result in zip(urls, results)
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
                    timeout=5,
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

                self.visited_urls.add(
                    item.url
                )

                self.processed_urls[
                    item.url
                ] = result

                self.queue.mark_processed(
                    item.url
                )

                next_depth = item.depth + 1

                if next_depth <= self.max_depth:

                    for link in result.get(
                        "links",
                        [],
                    ):
                        await self.queue.add_url(
                            url=link,
                            depth=next_depth,
                        )

                stats = self.queue.get_stats()

                elapsed = max(
                    time.perf_counter()
                    - start_time,
                    0.001,
                )

                speed = (
                    len(self.processed_urls)
                    / elapsed
                )

                logging.info(
                    f"📄 Processed={stats['processed']} "
                    f"| Queue={stats['queued']} "
                    f"| Errors={stats['failed']} "
                    f"| Speed={speed:.2f} pages/sec"
                )

                logging.info(
                    f"📊 Robots blocked: "
                    f"{self.blocked_by_robots} "
                    f"| Active: "
                    f"{self.semaphore_manager.active_tasks}"
                )

            except Exception as e:

                self.failed_urls[
                    item.url
                ] = str(e)

                self.queue.mark_failed(
                    item.url,
                    str(e),
                )

                logging.error(
                    f"❌ Crawl error | "
                    f"{item.url} | {e}"
                )

        return self.processed_urls


    async def close(self):
        if self.session is not None:
            await self.session.close()