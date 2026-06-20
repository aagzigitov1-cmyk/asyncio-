from urllib.parse import urljoin, urlparse
import logging

from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)


class HTMLParser:

    def _empty_result(self, url: str) -> dict:
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

    def _safe_extract(
        self,
        extractor_name: str,
        url: str,
        default,
        extractor,
    ):
        try:
            return extractor()
        except Exception as e:
            logger.warning(
                f"Extract error | {extractor_name} | {url} | {e}"
            )
            return default

    async def parse_html(
        self,
        html: str,
        url: str
    ) -> dict:

        result = self._empty_result(url)

        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as e:
            logger.warning(
                f"Parse error | {url} | {e}"
            )
            return result

        metadata = self._safe_extract(
            "metadata",
            url,
            {},
            lambda: self.extract_metadata(soup),
        )

        result.update(
            {
                "title": metadata.get("title", ""),
                "text": self._safe_extract(
                    "text",
                    url,
                    "",
                    lambda: self.extract_text(soup),
                ),
                "links": self._safe_extract(
                    "links",
                    url,
                    [],
                    lambda: self.extract_links(soup, url),
                ),
                "metadata": metadata,
                "images": self._safe_extract(
                    "images",
                    url,
                    [],
                    lambda: self.extract_images(soup, url),
                ),
                "headings": self._safe_extract(
                    "headings",
                    url,
                    {"h1": [], "h2": [], "h3": []},
                    lambda: self.extract_headings(soup),
                ),
                "tables": self._safe_extract(
                    "tables",
                    url,
                    [],
                    lambda: self.extract_tables(soup),
                ),
                "lists": self._safe_extract(
                    "lists",
                    url,
                    [],
                    lambda: self.extract_lists(soup),
                ),
            }
        )

        return result

    def extract_links(
        self,
        soup: BeautifulSoup,
        base_url: str
    ) -> list[str]:

        links = []

        for tag in soup.find_all(
            "a",
            href=True
        ):
            href = tag["href"]

            absolute_url = urljoin(
                base_url,
                href
            )

            parsed = urlparse(
                absolute_url
            )

            try:
                is_valid = (
                    parsed.scheme in ("http", "https")
                    and bool(parsed.netloc)
                    and bool(parsed.hostname)
                )
            except ValueError:
                is_valid = False

            if is_valid:
                links.append(
                    absolute_url
                )

        return list(
            dict.fromkeys(links)
        )

    def extract_text(
        self,
        soup: BeautifulSoup,
        selector: str | None = None
    ) -> str:

        if selector:
            elements = soup.select(
                selector
            )

            return " ".join(
                element.get_text(
                    separator=" ",
                    strip=True
                )
                for element in elements
            )

        return soup.get_text(
            separator=" ",
            strip=True
        )

    def extract_metadata(
        self,
        soup: BeautifulSoup
    ) -> dict:

        title_tag = soup.find(
            "title"
        )

        description_tag = soup.find(
            "meta",
            attrs={
                "name": "description"
            }
        )

        keywords_tag = soup.find(
            "meta",
            attrs={
                "name": "keywords"
            }
        )

        return {
            "title": (
                title_tag.get_text(
                    strip=True
                )
                if title_tag
                else ""
            ),
            "description": (
                description_tag.get(
                    "content",
                    ""
                )
                if description_tag
                else ""
            ),
            "keywords": (
                keywords_tag.get(
                    "content",
                    ""
                )
                if keywords_tag
                else ""
            ),
        }

    def extract_images(
        self,
        soup: BeautifulSoup,
        base_url: str
    ) -> list[dict]:

        images = []

        for img in soup.find_all(
            "img"
        ):
            src = img.get(
                "src",
                ""
            )

            if not src:
                continue

            images.append(
                {
                    "src": urljoin(
                        base_url,
                        src
                    ),
                    "alt": img.get(
                        "alt",
                        ""
                    ),
                }
            )

        return images

    def extract_headings(
        self,
        soup: BeautifulSoup
    ) -> dict:

        return {
            "h1": [
                tag.get_text(
                    strip=True
                )
                for tag in soup.find_all(
                    "h1"
                )
            ],
            "h2": [
                tag.get_text(
                    strip=True
                )
                for tag in soup.find_all(
                    "h2"
                )
            ],
            "h3": [
                tag.get_text(
                    strip=True
                )
                for tag in soup.find_all(
                    "h3"
                )
            ],
        }

    def extract_tables(
        self,
        soup: BeautifulSoup
    ) -> list[list[list[str]]]:

        tables = []

        for table in soup.find_all(
            "table"
        ):
            rows = []

            for tr in table.find_all(
                "tr"
            ):
                cells = [
                    cell.get_text(
                        strip=True
                    )
                    for cell in tr.find_all(
                        [
                            "td",
                            "th",
                        ]
                    )
                ]

                if cells:
                    rows.append(
                        cells
                    )

            if rows:
                tables.append(
                    rows
                )

        return tables

    def extract_lists(
        self,
        soup: BeautifulSoup
    ) -> list[list[str]]:

        result = []

        for html_list in soup.find_all(
            [
                "ul",
                "ol",
            ]
        ):
            items = [
                li.get_text(
                    strip=True
                )
                for li in html_list.find_all(
                    "li"
                )
            ]

            if items:
                result.append(
                    items
                )

        return result
