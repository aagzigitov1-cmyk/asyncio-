import logging
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp


logger = logging.getLogger(__name__)


class RobotsParser:
    def __init__(self):
        self.cache: dict[str, dict] = {}
        self._last_base_url: str | None = None

    def _base_url(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")

    async def fetch_robots(
        self,
        base_url: str,
        session: aiohttp.ClientSession | None = None,
        user_agent: str = "*",
    ) -> dict:
        base_url = self._base_url(base_url)
        self._last_base_url = base_url

        if base_url in self.cache:
            return self.cache[base_url]

        robots_url = f"{base_url}/robots.txt"
        parser = RobotFileParser(robots_url)
        rules = {
            "base_url": base_url,
            "robots_url": robots_url,
            "parser": parser,
            "status": None,
        }

        owns_session = session is None
        if session is None:
            session = aiohttp.ClientSession()

        try:
            async with session.get(
                robots_url,
                headers={"User-Agent": user_agent},
            ) as response:
                rules["status"] = response.status

                if response.status in (401, 403):
                    parser.disallow_all = True
                elif 400 <= response.status < 500:
                    parser.allow_all = True
                elif response.status >= 500:
                    parser.allow_all = True
                else:
                    parser.parse((await response.text()).splitlines())

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

        self.cache[base_url] = rules
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
