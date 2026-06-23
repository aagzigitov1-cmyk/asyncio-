import asyncio
import logging
from collections.abc import Awaitable, Callable
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp


logger = logging.getLogger(__name__)


class RobotsParser:
    def __init__(self):
        self.cache: dict[str, dict] = {}
        self._last_base_url: str | None = None
        self._locks: dict[str, asyncio.Lock] = {}

    def _base_url(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")

    async def fetch_robots(
        self,
        base_url: str,
        session: aiohttp.ClientSession | None = None,
        user_agent: str = "*",
        fetcher: Callable[[str, str], Awaitable[tuple[int, str]]] | None = None,
    ) -> dict:
        base_url = self._base_url(base_url)
        self._last_base_url = base_url

        if base_url in self.cache:
            return self.cache[base_url]

        lock = self._locks.setdefault(base_url, asyncio.Lock())
        async with lock:
            # Another coroutine may have populated the cache while this one
            # was waiting for the per-domain lock.
            if base_url in self.cache:
                return self.cache[base_url]

            rules = await self._load_robots(
                base_url,
                session,
                user_agent,
                fetcher,
            )
            self.cache[base_url] = rules
            return rules

    async def _load_robots(
        self,
        base_url: str,
        session: aiohttp.ClientSession | None,
        user_agent: str,
        fetcher: Callable[[str, str], Awaitable[tuple[int, str]]] | None,
    ) -> dict:

        robots_url = f"{base_url}/robots.txt"
        parser = RobotFileParser(robots_url)
        rules = {
            "base_url": base_url,
            "robots_url": robots_url,
            "parser": parser,
            "status": None,
        }

        owns_session = session is None and fetcher is None
        if session is None and fetcher is None:
            session = aiohttp.ClientSession()

        try:
            if fetcher is not None:
                status, body = await fetcher(robots_url, user_agent)
            else:
                async with session.get(
                    robots_url,
                    headers={"User-Agent": user_agent},
                ) as response:
                    status = response.status
                    body = await response.text()

            rules["status"] = status
            if status in (401, 403):
                parser.disallow_all = True
            elif status >= 400:
                parser.allow_all = True
            else:
                parser.parse(body.splitlines())

        except (aiohttp.ClientError, TimeoutError) as error:
            parser.allow_all = True
            logger.warning(
                "Robots fetch error | %s | %s",
                robots_url,
                error,
            )
        finally:
            if owns_session:
                await session.close()

        return rules

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        rules = self.cache.get(self._base_url(url))
        if rules is None:
            return True
        return rules["parser"].can_fetch(user_agent, url)

    def get_crawl_delay(
        self,
        user_agent: str = "*",
        base_url: str | None = None,
    ) -> float:
        key = self._base_url(base_url) if base_url else self._last_base_url
        rules = self.cache.get(key or "")
        if rules is None:
            return 0.0

        parser = rules["parser"]
        delay = parser.crawl_delay(user_agent)
        if delay is None and user_agent != "*":
            delay = parser.crawl_delay("*")
        return float(delay or 0.0)
