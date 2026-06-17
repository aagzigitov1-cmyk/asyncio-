import asyncio

from queue_manager import CrawlerQueue


async def main():

    queue = CrawlerQueue()

    await queue.add_url(
        "https://example.com",
        depth=0,
    )

    await queue.add_url(
        "https://python.org",
        depth=1,
    )

    item = await queue.get_next()

    print(item)

    print(queue.get_stats())



    crawler = AsyncCrawler(
        max_concurrent=10,
        max_depth=2,
    )

    results = await crawler.crawl(
        start_urls=[
            "https://example.com"
        ],
        max_pages=50,
    )

    print(
        f"Processed: {len(results)}"
    )







asyncio.run(main())







