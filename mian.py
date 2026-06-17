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






    # ==========================
    # DAY 3
    # ==========================

    print("\n" + "=" * 60)
    print("DAY 3 - WEBSITE CRAWLING")
    print("=" * 60)

    crawler.max_depth = 2

    start = time.perf_counter()

    crawl_results = await crawler.crawl(
        start_urls=[
            "https://docs.python.org/3/",
        ],
        max_pages=20,
        same_domain_only=True,
    )

    elapsed = time.perf_counter() - start

    print("\n" + "=" * 60)
    print("CRAWL RESULTS")
    print("=" * 60)

    print(
        f"Processed pages: "
        f"{len(crawler.processed_urls)}"
    )

    total_links = sum(
        len(page["links"])
        for page in crawl_results.values()
    )

    print(
        f"Total discovered links: "
        f"{total_links}"
    )

    print(
        f"Visited URLs: "
        f"{len(crawler.visited_urls)}"
    )

    print(
        f"Failed URLs: "
        f"{len(crawler.failed_urls)}"
    )

    print(
        f"Queue stats: "
        f"{crawler.queue.get_stats()}"
    )

    print(
        f"Semaphore stats: "
        f"{crawler.semaphore_manager.stats()}"
    )

    print(
        f"Total time: "
        f"{elapsed:.2f} sec"
    )

    for url, page in list(crawl_results.items())[:5]:

        print("\n" + "-" * 40)

        print(
            f"URL: {url}"
        )

        print(
            f"TITLE: {page['title']}"
        )

        print(
            f"LINKS: "
            f"{len(page['links'])}"
        )



    print("\n" + "=" * 60)
    print("FILTER TEST")
    print("=" * 60)

    filter_crawler = AsyncCrawler(
        max_concurrent=5,
        max_depth=2,
    )

    filtered_results = await filter_crawler.crawl(
        start_urls=[
            "https://docs.python.org/3/library/index.html"
        ],
        max_pages=10,
        include_patterns=["library"],
    )

    print(
        f"Filtered pages: "
        f"{len(filtered_results)}"
    )

    print(
        f"Visited URLs: "
        f"{len(filter_crawler.visited_urls)}"
    )

    print(
        f"Queue stats: "
        f"{filter_crawler.queue.get_stats()}"
    )

    print("\nTutorial pages:")

    for url in list(filtered_results.keys())[:5]:
        print(url)

    await filter_crawler.close()






    from queue_manager import CrawlerQueue


    async def priority_test():

        queue = CrawlerQueue()

        await queue.add_url(
            "low",
            priority=10,
        )

        await queue.add_url(
            "high",
            priority=1,
        )

        await queue.add_url(
            "medium",
            priority=5,
        )

        print("\nPRIORITY TEST")

        while True:

            item = await queue.get_next()

            if item is None:
                break

            print(
                item.url,
                item.priority,
            )





    print("\n" + "=" * 60)
    print("PRIORITY QUEUE TEST")
    print("=" * 60)

    queue = CrawlerQueue()

    await queue.add_url(
        "https://low.com",
        priority=10,
    )

    await queue.add_url(
        "https://high.com",
        priority=1,
    )

    await queue.add_url(
        "https://medium.com",
        priority=5,
    )

    while True:
        item = await queue.get_next()

        if item is None:
            break

        print(
            item.url,
            item.priority,
        )



    filtered_results = await filter_crawler.crawl(
        start_urls=[
            "https://docs.python.org/3/"
        ],
        max_pages=10,
        exclude_patterns=[
            "download"
        ],
    )



    # ==========================
    # DAY 4
    # ==========================

    print("\n" + "=" * 60)
    print("DAY 4 - RATE LIMIT + ROBOTS TEST")
    print("=" * 60)

    test_crawler = AsyncCrawler(
        max_concurrent=5,
        max_depth=1,
        per_domain_limit=2,
    )

    urls = [
        "https://example.com",
        "https://www.python.org",
    ]

    # ==========================
    # 1. BASIC CRAWL TEST
    # ==========================
    print("\n🟢 1. BASIC CRAWL TEST")

    start = time.perf_counter()

    results = await test_crawler.crawl(
        start_urls=urls,
        max_pages=10,
    )

    elapsed = time.perf_counter() - start

    print(f"Pages crawled: {len(results)}")
    print(f"Time: {elapsed:.2f}s")

    # ==========================
    # 2. SYSTEM STATS
    # ==========================
    print("\n🟡 2. SYSTEM STATS")

    print(f"Blocked by robots: {test_crawler.blocked_by_robots}")
    print(f"Rate limited: {test_crawler.rate_limited_count}")
    print(f"Active tasks: {test_crawler.semaphore_manager.active_tasks}")
    print(f"Queue stats: {test_crawler.queue.get_stats()}")

    # ==========================
    # 3. RATE LIMIT TEST (sequential)
    # ==========================
    print("\n🔵 3. RATE LIMIT TEST")

    start = time.perf_counter()

    for _ in range(5):
        await test_crawler.fetch_url("https://example.com")

    elapsed2 = time.perf_counter() - start

    print(f"5 sequential requests: {elapsed2:.2f}s")

    if elapsed2 < 2.0:
        print("❌ RATE LIMIT FAIL (too fast)")
    else:
        print("✅ RATE LIMIT OK")

    # ==========================
    # 4. CONCURRENCY TEST
    # ==========================
    print("\n🟣 4. CONCURRENCY TEST")

    start = time.perf_counter()

    tasks = [
        test_crawler.fetch_url("https://example.com")
        for _ in range(10)
    ]

    await asyncio.gather(*tasks)

    elapsed3 = time.perf_counter() - start

    print(f"10 parallel requests: {elapsed3:.2f}s")

    print(f"Final active tasks: {test_crawler.semaphore_manager.active_tasks}")

    if test_crawler.semaphore_manager.active_tasks != 0:
        print("❌ SEMAPHORE LEAK")
    else:
        print("✅ SEMAPHORE OK")

    await test_crawler.close()


if __name__ == "__main__":
    asyncio.run(main())






