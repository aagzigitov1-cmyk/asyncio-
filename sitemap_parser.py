import asyncio
import logging
from urllib.parse import urljoin
from xml.etree import ElementTree

import aiohttp


logger = logging.getLogger(__name__)


class SitemapParser:
    def __init__(
        self,
        session: aiohttp.ClientSession | None = None,
        *,
        max_depth: int = 10,
    ):
        self.session = session
        self.max_depth = max_depth
        self._visited_sitemaps: set[str] = set()

    def _local_name(self, tag: str) -> str:
        return tag.rsplit("}", 1)[-1].lower()

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        self._visited_sitemaps.clear()
        owns_session = self.session is None
        session = self.session or aiohttp.ClientSession()
        try:
            urls = await self._fetch_recursive(
                sitemap_url,
                session,
                depth=0,
            )
            return list(dict.fromkeys(urls))
        finally:
            if owns_session:
                await session.close()

    async def _fetch_recursive(
        self,
        sitemap_url: str,
        session: aiohttp.ClientSession,
        *,
        depth: int,
    ) -> list[str]:
        if depth > self.max_depth or sitemap_url in self._visited_sitemaps:
            return []
        self._visited_sitemaps.add(sitemap_url)

        try:
            async with session.get(sitemap_url) as response:
                response.raise_for_status()
                content = await response.text()
            root = ElementTree.fromstring(content)
        except (
            aiohttp.ClientError,
            TimeoutError,
            ElementTree.ParseError,
            UnicodeError,
        ) as error:
            logger.warning("Sitemap error | %s | %s", sitemap_url, error)
            return []

        root_name = self._local_name(root.tag)
        locations = [
            (element.text or "").strip()
            for element in root.iter()
            if self._local_name(element.tag) == "loc" and element.text
        ]

        if root_name == "sitemapindex":
            nested_results = await asyncio.gather(
                *(
                    self._fetch_recursive(
                        urljoin(sitemap_url, location),
                        session,
                        depth=depth + 1,
                    )
                    for location in locations
                )
            )
            return [url for group in nested_results for url in group]

        if root_name == "urlset":
            return [urljoin(sitemap_url, location) for location in locations]

        logger.warning("Unknown sitemap root | %s | %s", sitemap_url, root_name)
        return []
