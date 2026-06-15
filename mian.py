import asyncio
import time

from crawler import AsyncCrawler


async def main():

    crawler = AsyncCrawler(max_concurrent=5)

    urls = [
        "https://example.com",
        "https://www.python.org",
        "https://github.com",
        "https://docs.python.org/3/",
    ]

    # ==========================
    # DAY 1
    # ==========================

    print("\n" + "=" * 60)
    print("DAY 1 - FETCH URLS")
    print("=" * 60)

    start = time.perf_counter()

    html_results = await crawler.fetch_urls(
        urls
    )

    elapsed = time.perf_counter() - start

    for url, html in html_results.items():

        print(
            f"{url} -> {len(html)} chars"
        )

    print(
        f"\nLoaded {len(html_results)} pages"
    )

    print(
        f"Total time: {elapsed:.2f} sec"
    )

    # ==========================
    # DAY 2
    # ==========================

    print("\n" + "=" * 60)
    print("DAY 2 - FETCH AND PARSE")
    print("=" * 60)

    start = time.perf_counter()

    parsed_results = await crawler.fetch_and_parse_many(
        urls
    )

    elapsed = time.perf_counter() - start

    for page in parsed_results:

        print("\n" + "-" * 40)

        print(
            f"URL: {page['url']}"
        )

        print(
            f"TITLE: {page['title']}"
        )

        print(
            f"TEXT LENGTH: {len(page['text'])}"
        )

        print(
            f"LINKS COUNT: {len(page['links'])}"
        )

        print(
            f"IMAGES COUNT: {len(page['images'])}"
        )

        print(
            f"H1 COUNT: "
            f"{len(page['headings']['h1'])}"
        )

    print(
        f"\nProcessed {len(parsed_results)} pages"
    )

    print(
        f"Total time: {elapsed:.2f} sec"
    )

    await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())




import asyncio

from parser import HTMLParser


async def main():
    html = """
    <html>
        <body>
            <a href="/about">About</a>
            <a href="/contacts">Contacts</a>
        </body>
    </html>
    """

    parser = HTMLParser()

    result = await parser.parse_html(
        html,
        "https://example.com"
    )

    print(result["links"])


asyncio.run(main())