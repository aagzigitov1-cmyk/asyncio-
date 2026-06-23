import asyncio
import inspect
import json
import logging
from datetime import datetime, timezone
from parser import HTMLParser
import aiohttp
import aiofiles
import time
from urllib.parse import urlparse
from queue_manager import CrawlerQueue
from semaphore_manager import SemaphoreManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
from retry_strategy import (
    CrawlerError,
    NetworkError,
    PermanentError,
    RetryStrategy,
    TransientError,
)
from storage import DataStorage
from circuit_breaker import CircuitBreaker
from crawler_stats import CrawlerStats
from sitemap_parser import SitemapParser
from crawler_config import create_storage, load_config, storage_from_output
from logging_setup import configure_logging
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
            requests_per_second: float = 1.0,
            rate_limit_per_domain: bool = True,
            respect_robots: bool = True,
            min_delay: float = 0.0,
            jitter: float = 0.0,
            user_agent: str = "MyCrawler/1.0",
            rotate_user_agents: bool = False,
            max_retries: int = 2,
            backoff_base: float = 1.0,
            connect_timeout: float = 10.0,
            read_timeout: float = 20.0,
            total_timeout: float = 30.0,
            timeout_growth: float = 1.5,
            storage: DataStorage | None = None,
            storage_retries: int = 2,
            storage_retry_delay: float = 0.1,
            circuit_breaker_enabled: bool = True,
            circuit_failure_threshold: int = 5,
            circuit_recovery_timeout: float = 30.0,
            circuit_failure_window: float = 60.0,
        ):
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if backoff_base < 0:
            raise ValueError("backoff_base cannot be negative")
        if min(connect_timeout, read_timeout, total_timeout) <= 0:
            raise ValueError("timeouts must be greater than zero")
        if timeout_growth < 1:
            raise ValueError("timeout_growth must be at least 1")
        if storage_retries < 0 or storage_retry_delay < 0:
            raise ValueError("storage retry settings cannot be negative")

        self.max_concurrent = max_concurrent


        self.timeout = aiohttp.ClientTimeout(
            total=total_timeout,
            connect=connect_timeout,
            sock_read=read_timeout,
        )
        self.timeout_growth = timeout_growth

        self.session: aiohttp.ClientSession | None = None
        self.storage = storage
        self.storage_retries = storage_retries
        self.storage_retry_delay = storage_retry_delay
        self.storage_errors: dict[str, str] = {}
        self.response_metadata: dict[str, dict] = {}
        self.stats_collector = None
        self.progress_callback = None

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
            requests_per_second=requests_per_second,
            per_domain=rate_limit_per_domain,
            min_delay=min_delay,
            jitter=jitter,
        )

        # =========================
        # USER AGENT SYSTEM
        # =========================
  

        self.user_agent = user_agent
        self.rotate_user_agents = rotate_user_agents
        self.user_agents = [
            user_agent,
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        ]

        self.robots = RobotsParser()
        self.respect_robots = respect_robots
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.retry_strategy = RetryStrategy(
            max_retries=max_retries,
            backoff_factor=backoff_base,
            retry_on=[TransientError, NetworkError],
            retry_limits={
                TransientError: max_retries,
                NetworkError: min(max_retries, 2),
            },
            backoff_factors={
                TransientError: backoff_base,
                NetworkError: backoff_base * 1.5,
            },
        )
        self.error_details: dict[str, dict] = {}
        self.circuit_breaker = (
            CircuitBreaker(
                failure_threshold=circuit_failure_threshold,
                recovery_timeout=circuit_recovery_timeout,
                failure_window=circuit_failure_window,
            )
            if circuit_breaker_enabled
            else None
        )

        self.blocked_by_robots = 0
        self.rate_limited_count = 0








    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=self.timeout
            )

        return self.session


    def _is_same_domain(
        self,
        url: str,
        root_domains: set[str],
    ) -> bool:

        return urlparse(url).netloc.lower() in root_domains


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
        root_domains: set[str],
        same_domain_only: bool,
        include_patterns: list[str] | None,
        exclude_patterns: list[str] | None,
    ) -> bool:

        if depth > self.max_depth:
            return False


        if same_domain_only:

            if not self._is_same_domain(
                url,
                root_domains,
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
        if self.rotate_user_agents:
            return random.choice(self.user_agents)
        return self.user_agent

    def get_rate_stats(self) -> dict[str, float | int]:
        stats = self.rate_limiter.get_stats()
        stats["blocked_by_robots"] = self.blocked_by_robots
        stats["rate_limited_responses"] = self.rate_limited_count
        return stats

    def get_error_stats(self) -> dict:
        return self.retry_strategy.get_stats()

    def get_circuit_breaker_stats(self) -> dict:
        if self.circuit_breaker is None:
            return {
                "enabled": False,
                "opened": 0,
                "blocked_requests": 0,
                "recovered": 0,
                "domains": {},
            }
        return {
            "enabled": True,
            **self.circuit_breaker.get_stats(),
        }

    async def save_error_report(self, path: str) -> None:
        report = {
            "statistics": self.get_error_stats(),
            "errors": self.error_details,
            "history": self.retry_strategy.history,
        }
        async with aiofiles.open(path, "w", encoding="utf-8") as file:
            await file.write(
                json.dumps(report, ensure_ascii=False, indent=2)
            )

    def get_storage_stats(self) -> dict:
        stats = (
            self.storage.get_stats()
            if self.storage is not None
            else {"saved": 0, "errors": 0}
        )
        return {
            **stats,
            "failed_urls": dict(self.storage_errors),
        }

    async def _save_result(self, data: dict) -> bool:
        if self.storage is None:
            return True

        url = data.get("url", "unknown")
        for attempt in range(self.storage_retries + 1):
            try:
                await self.storage.save(data)
                self.storage_errors.pop(url, None)
                return True
            except Exception as error:
                self.storage.error_count += 1
                logging.error(
                    f"💾 Storage error | {url} "
                    f"| attempt={attempt + 1} | {error}"
                )
                if attempt < self.storage_retries:
                    await asyncio.sleep(
                        self.storage_retry_delay * (2 ** attempt)
                    )
                else:
                    self.storage_errors[url] = str(error)
        return False

    async def fetch_url(self, url: str) -> str:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        session = await self._get_session()
        user_agent = self._get_user_agent()

        # =========================
        # 1. ROBOTS.TXT
        # =========================
        if self.respect_robots:
            await self.robots.fetch_robots(
                base_url,
                session,
                user_agent,
                fetcher=self._fetch_robots_once,
            )

            if not self.robots.can_fetch(url, user_agent):
                logging.warning(f"🚫 robots blocked: {url}")
                self.blocked_by_robots += 1
                self.failed_urls[url] = "Blocked by robots.txt"
                return ""

            crawl_delay = self.robots.get_crawl_delay(
                user_agent,
                base_url,
            )
            self.rate_limiter.set_domain_delay(domain, crawl_delay)
        else:
            self.rate_limiter.set_domain_delay(domain, 0.0)

        try:
            html = await self.retry_strategy.execute_with_retry(
                self._fetch_once,
                url,
                user_agent,
                domain,
            )
            self.failed_urls.pop(url, None)
            self.error_details.pop(url, None)
            return html
        except CrawlerError as error:
            self.failed_urls[url] = str(error)
            self.error_details[url] = {
                "type": type(error).__name__,
                "message": str(error),
                "status": error.status,
                "attempts": getattr(error, "attempts", 1),
            }
            return ""

    async def _fetch_robots_once(
        self,
        robots_url: str,
        user_agent: str,
    ) -> tuple[int, str]:
        domain = urlparse(robots_url).netloc.lower()
        await self.rate_limiter.acquire(domain)
        await self.semaphore_manager.acquire(robots_url)
        try:
            session = await self._get_session()
            async with session.get(
                robots_url,
                headers={"User-Agent": user_agent},
            ) as response:
                return response.status, await response.text()
        finally:
            await self.semaphore_manager.release(robots_url)

    async def _fetch_once(
        self,
        url: str,
        user_agent: str,
        domain: str,
    ) -> str:
        attempt = self.retry_strategy.current_attempt
        timeout_multiplier = self.timeout_growth ** attempt

        def scaled(value):
            return (
                value * timeout_multiplier
                if value is not None
                else None
            )

        request_timeout = aiohttp.ClientTimeout(
            total=scaled(self.timeout.total),
            connect=scaled(self.timeout.connect),
            sock_read=scaled(self.timeout.sock_read),
        )

        if self.circuit_breaker is not None:
            await self.circuit_breaker.before_request(
                domain,
                url=url,
            )

        await self.rate_limiter.acquire(domain)
        await self.semaphore_manager.acquire(url)

        try:
            logging.info(
                f"▶️ Start: {url} | attempt={attempt + 1}"
            )
            session = await self._get_session()
            async with session.get(
                url,
                headers={"User-Agent": user_agent},
                timeout=request_timeout,
            ) as response:
                status = response.status

                if status >= 400:
                    retry_after = None
                    if status == 429:
                        self.rate_limited_count += 1
                        try:
                            retry_after = float(
                                response.headers.get("Retry-After")
                            )
                        except (TypeError, ValueError):
                            pass

                    message = f"HTTP {status}: {url}"
                    if status in (401, 403, 404):
                        raise PermanentError(
                            message,
                            url=url,
                            status=status,
                        )
                    if status == 429 or status >= 500:
                        raise TransientError(
                            message,
                            url=url,
                            status=status,
                            retry_after=retry_after,
                        )
                    raise PermanentError(
                        message,
                        url=url,
                        status=status,
                    )

                html = await response.text()
                self.response_metadata[url] = {
                    "status_code": status,
                    "content_type": response.headers.get(
                        "Content-Type",
                        "",
                    ),
                }
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_success(domain)
                logging.info(
                    f"✅ Success: {url} | attempt={attempt + 1}"
                )
                return html

        except (TransientError, NetworkError):
            if self.circuit_breaker is not None:
                await self.circuit_breaker.record_failure(domain)
            raise
        except asyncio.TimeoutError as error:
            classified_error = TransientError(
                f"Timeout: {url}",
                url=url,
            )
            if self.circuit_breaker is not None:
                await self.circuit_breaker.record_failure(domain)
            raise classified_error from error
        except CrawlerError:
            raise
        except aiohttp.ClientError as error:
            classified_error = NetworkError(
                f"Network error: {url} "
                f"| {type(error).__name__}: {error}",
                url=url,
            )
            if self.circuit_breaker is not None:
                await self.circuit_breaker.record_failure(domain)
            raise classified_error from error
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
        response_metadata = self.response_metadata.get(url, {})
        status_code = response_metadata.get("status_code", 0)

        if not html and not (200 <= status_code < 400):
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
                "crawled_at": datetime.now(timezone.utc),
                "status_code": 0,
                "content_type": "",
            }

        result = await self.parser.parse_html(
            html,
            url
        )
        result.update(
            {
                "crawled_at": datetime.now(timezone.utc),
                "status_code": response_metadata.get("status_code", 200),
                "content_type": response_metadata.get("content_type", ""),
            }
        )
        return result

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

        self.queue = CrawlerQueue()
        self.visited_urls.clear()
        self.failed_urls.clear()
        self.processed_urls.clear()
        self.storage_errors.clear()
        self.response_metadata.clear()

        start_time = time.perf_counter()

        root_domains = {
            urlparse(url).netloc.lower()
            for url in start_urls
            if urlparse(url).netloc
        }

        for url in start_urls:
            await self.queue.add_url(
                url=url,
                depth=0,
            )

        async def process_item(item):
            try:
                result = await self.fetch_and_parse(item.url)

                status_code = result.get("status_code") if result else None
                if not result or (
                    status_code is not None
                    and not 200 <= status_code < 400
                ):
                    error = self.failed_urls.get(
                        item.url,
                        "Empty result",
                    )
                    self.failed_urls[item.url] = error
                    self.queue.mark_failed(item.url, error)
                    if self.stats_collector is not None:
                        details = self.error_details.get(item.url, {})
                        self.stats_collector.record_failure(
                            item.url,
                            details.get("status"),
                        )
                    return

                self.processed_urls[item.url] = result
                self.queue.mark_processed(item.url)
                await self._save_result(result)
                if self.stats_collector is not None:
                    self.stats_collector.record_success(
                        item.url,
                        result.get("status_code", 200),
                    )

                next_depth = item.depth + 1
                if next_depth <= self.max_depth:
                    for link in result.get("links", []):
                        await self.queue.add_url(
                            url=link,
                            depth=next_depth,
                        )

            except Exception as e:
                self.failed_urls[item.url] = str(e)
                self.queue.mark_failed(item.url, str(e))
                if self.stats_collector is not None:
                    self.stats_collector.record_failure(item.url)
                logging.error(
                    f"❌ Crawl error | {item.url} | {e}"
                )

            finally:
                stats = self.queue.get_stats()
                elapsed = max(
                    time.perf_counter() - start_time,
                    0.001,
                )
                speed = len(self.processed_urls) / elapsed

                logging.info(
                    f"📄 Processed={stats['processed']} "
                    f"| Queue={stats['queued']} "
                    f"| Errors={stats['failed']} "
                    f"| Speed={speed:.2f} pages/sec"
                )
                logging.info(
                    f"📊 Robots blocked: {self.blocked_by_robots} "
                    f"| Active: {self.semaphore_manager.active_tasks}"
                )
                rate_stats = self.get_rate_stats()
                logging.info(
                    f"⚡ Requests/sec: "
                    f"{rate_stats['current_requests_per_second']:.2f} "
                    f"| Average delay: "
                    f"{rate_stats['average_delay']:.2f}s"
                )
                if (
                    self.stats_collector is not None
                    and self.progress_callback is not None
                ):
                    progress = self.stats_collector.progress(
                        max_pages=max_pages,
                        active_tasks=self.semaphore_manager.active_tasks,
                        queued=stats["queued"],
                    )
                    callback_result = self.progress_callback(progress)
                    if inspect.isawaitable(callback_result):
                        await callback_result

        while len(self.visited_urls) < max_pages:
            available_slots = min(
                max(1, self.max_concurrent),
                max_pages - len(self.visited_urls),
            )
            batch = []

            while len(batch) < available_slots:
                item = await self.queue.get_next()

                if item is None:
                    break

                if not item.url or item.url in self.visited_urls:
                    continue

                if not self._should_visit(
                    url=item.url,
                    depth=item.depth,
                    root_domains=root_domains,
                    same_domain_only=same_domain_only,
                    include_patterns=include_patterns,
                    exclude_patterns=exclude_patterns,
                ):
                    continue

                self.visited_urls.add(item.url)
                batch.append(item)

            if not batch:
                break

            await asyncio.gather(
                *(process_item(item) for item in batch)
            )

        return self.processed_urls


    async def close(self):
        if self.session is not None:
            await self.session.close()
        if self.storage is not None:
            await self.storage.close()


class AdvancedCrawler(AsyncCrawler):
    def __init__(
        self,
        *,
        start_urls: list[str] | None = None,
        max_pages: int = 100,
        same_domain_only: bool = True,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        sitemap_urls: list[str] | None = None,
        show_progress: bool = True,
        **crawler_options,
    ):
        super().__init__(**crawler_options)
        self.start_urls = list(start_urls or [])
        self.default_max_pages = max_pages
        self.same_domain_only = same_domain_only
        self.include_patterns = include_patterns
        self.exclude_patterns = exclude_patterns
        self.sitemap_urls = list(sitemap_urls or [])
        self.show_progress = show_progress
        self.advanced_stats = CrawlerStats()
        self.stats_collector = self.advanced_stats
        if show_progress:
            self.progress_callback = self._display_progress

    @classmethod
    def from_config(cls, filename: str):
        config = load_config(filename)
        logging_config = dict(config.get("logging") or {})
        if logging_config:
            configure_logging(
                log_file=logging_config.get("file"),
                level=logging_config.get("level", "INFO"),
                max_bytes=logging_config.get("max_bytes", 5_000_000),
                backup_count=logging_config.get("backup_count", 3),
            )

        filters = dict(config.get("filters") or {})
        crawler_options = dict(config.get("crawler") or {})
        if "rate_limit" in crawler_options:
            crawler_options.setdefault(
                "requests_per_second",
                crawler_options.pop("rate_limit"),
            )
        storage = create_storage(config.get("storage"))
        crawler = cls(
            start_urls=config.get("start_urls") or [],
            max_pages=config.get("max_pages", 100),
            same_domain_only=filters.get("same_domain_only", True),
            include_patterns=filters.get("include_patterns") or None,
            exclude_patterns=filters.get("exclude_patterns") or None,
            sitemap_urls=config.get("sitemaps") or [],
            show_progress=config.get("show_progress", True),
            storage=storage,
            **crawler_options,
        )
        crawler.config = config
        return crawler

    def _display_progress(self, progress: dict) -> None:
        eta = progress["eta_seconds"]
        eta_text = f"{eta:.1f}s" if eta is not None else "--"
        print(
            "\r"
            f"Progress: {progress['percentage']:6.2f}% "
            f"({progress['completed']}/{progress['max_pages']}) "
            f"| {progress['speed']:.2f} pages/s "
            f"| ETA {eta_text} "
            f"| active={progress['active_tasks']} "
            f"| queued={progress['queued']}",
            end="",
            flush=True,
        )

    async def crawl(
        self,
        start_urls: list[str] | None = None,
        max_pages: int | None = None,
        same_domain_only: bool | None = None,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        sitemap_urls: list[str] | None = None,
    ) -> dict[str, dict]:
        urls = list(start_urls if start_urls is not None else self.start_urls)
        configured_sitemaps = (
            sitemap_urls
            if sitemap_urls is not None
            else self.sitemap_urls
        )

        if configured_sitemaps:
            session = await self._get_session()
            sitemap_results = await asyncio.gather(
                *(
                    SitemapParser(session=session).fetch_sitemap(url)
                    for url in configured_sitemaps
                )
            )
            for group in sitemap_results:
                urls.extend(group)
        urls = list(dict.fromkeys(urls))

        if not urls:
            raise ValueError("at least one start URL or sitemap is required")

        page_limit = (
            self.default_max_pages
            if max_pages is None
            else max_pages
        )
        self.advanced_stats.start()
        try:
            return await super().crawl(
                start_urls=urls,
                max_pages=page_limit,
                same_domain_only=(
                    self.same_domain_only
                    if same_domain_only is None
                    else same_domain_only
                ),
                include_patterns=(
                    self.include_patterns
                    if include_patterns is None
                    else include_patterns
                ),
                exclude_patterns=(
                    self.exclude_patterns
                    if exclude_patterns is None
                    else exclude_patterns
                ),
            )
        finally:
            self.advanced_stats.finish()
            if self.show_progress:
                print()

    def get_stats(self) -> dict:
        return {
            **self.advanced_stats.get_stats(),
            "rate_limit": self.get_rate_stats(),
            "errors": self.get_error_stats(),
            "storage": self.get_storage_stats(),
            "circuit_breaker": self.get_circuit_breaker_stats(),
        }

    def export_to_json(self, filename: str) -> None:
        self.advanced_stats.export_to_json(filename)

    def export_to_html_report(self, filename: str) -> None:
        self.advanced_stats.export_to_html_report(filename)


def _build_cli_parser():
    import argparse

    parser = argparse.ArgumentParser(
        description="Asynchronous web crawler",
    )
    parser.add_argument("--urls", nargs="+", help="Start URLs")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--max-depth", type=int, default=None)
    parser.add_argument("--output", help="JSON, JSONL, CSV or SQLite output")
    parser.add_argument("--config", help="JSON configuration file")
    parser.add_argument(
        "--respect-robots",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument("--rate-limit", type=float, default=None)
    parser.add_argument("--sitemap", action="append", default=None)
    parser.add_argument("--log-file", default=None)
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )
    return parser


async def _run_cli(args) -> int:
    if args.log_file:
        configure_logging(log_file=args.log_file, level=args.log_level)

    if args.config:
        crawler = AdvancedCrawler.from_config(args.config)
        if args.urls:
            crawler.start_urls = args.urls
        if args.max_pages is not None:
            crawler.default_max_pages = args.max_pages
        if args.max_depth is not None:
            crawler.max_depth = args.max_depth
        if args.respect_robots is not None:
            crawler.respect_robots = args.respect_robots
        if args.rate_limit is not None:
            crawler.rate_limiter.rps = args.rate_limit
            crawler.rate_limiter.min_interval = 1.0 / args.rate_limit
        if args.sitemap:
            crawler.sitemap_urls = args.sitemap
    else:
        if not args.urls and not args.sitemap:
            raise ValueError("--urls, --sitemap or --config is required")
        crawler = AdvancedCrawler(
            start_urls=args.urls or [],
            sitemap_urls=args.sitemap or [],
            max_pages=args.max_pages or 100,
            max_depth=args.max_depth or 2,
            respect_robots=(
                True if args.respect_robots is None else args.respect_robots
            ),
            requests_per_second=args.rate_limit or 1.0,
        )

    if args.output:
        if crawler.storage is not None:
            await crawler.storage.close()
        crawler.storage = storage_from_output(args.output)

    try:
        await crawler.crawl()
        prefix = args.output or "crawler_results"
        crawler.export_to_json(f"{prefix}.stats.json")
        crawler.export_to_html_report(f"{prefix}.report.html")
        print(json.dumps(crawler.get_stats(), ensure_ascii=False, indent=2))
        return 0
    finally:
        await crawler.close()


def _cli_main() -> int:
    parser = _build_cli_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(_run_cli(args))
    except (ValueError, OSError) as error:
        parser.error(str(error))
        return 2


if __name__ == "__main__":
    raise SystemExit(_cli_main())
