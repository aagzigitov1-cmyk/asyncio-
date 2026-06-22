import asyncio
import json
import logging
import time
import tracemalloc

from crawler import AdvancedCrawler


async def run_scalability_benchmark(
    page_counts=(100, 500, 1000),
    *,
    simulated_latency: float = 0.001,
    max_concurrent: int = 50,
) -> list[dict]:
    results = []
    # Windows' asyncio timer commonly has a coarser resolution than 1 ms.
    # A 10 ms floor models actual network I/O and prevents the benchmark from
    # comparing timer quantisation instead of crawler concurrency.
    benchmark_latency = max(simulated_latency, 0.01)

    for page_count in page_counts:
        urls = [
            f"https://benchmark.local/page/{index}"
            for index in range(page_count)
        ]
        async def fake_fetch_and_parse(url):
            await asyncio.sleep(benchmark_latency)
            return {
                "url": url,
                "title": url,
                "text": "benchmark",
                "links": [],
                "metadata": {},
                "status_code": 200,
                "content_type": "text/html",
            }

        def make_crawler():
            crawler = AdvancedCrawler(
                start_urls=urls,
                max_pages=page_count,
                max_concurrent=max_concurrent,
                same_domain_only=True,
                show_progress=False,
            )
            crawler.fetch_and_parse = fake_fetch_and_parse
            return crawler

        root_logger = logging.getLogger()
        previous_level = root_logger.level
        root_logger.setLevel(logging.WARNING)
        try:
            # Timing with tracemalloc enabled heavily penalizes coroutine and
            # task allocation, so latency and memory are measured in separate
            # runs.  This keeps the async/sync speed comparison fair.
            crawler = make_crawler()
            started = time.perf_counter()
            await crawler.crawl()
            async_elapsed = time.perf_counter() - started
            await crawler.close()

            memory_crawler = make_crawler()
            tracemalloc.start()
            await memory_crawler.crawl()
            _, peak_memory = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            await memory_crawler.close()
        finally:
            if tracemalloc.is_tracing():
                tracemalloc.stop()
            root_logger.setLevel(previous_level)

        sync_started = time.perf_counter()
        for _ in range(page_count):
            time.sleep(benchmark_latency)
        sync_elapsed = time.perf_counter() - sync_started

        results.append(
            {
                "pages": page_count,
                "simulated_latency": benchmark_latency,
                "async_seconds": async_elapsed,
                "sync_seconds": sync_elapsed,
                "speedup": (
                    sync_elapsed / async_elapsed
                    if async_elapsed
                    else 0.0
                ),
                "pages_per_second": (
                    page_count / async_elapsed
                    if async_elapsed
                    else 0.0
                ),
                "peak_memory_mb": peak_memory / 1024 / 1024,
            }
        )

    return results


if __name__ == "__main__":
    print(
        json.dumps(
            asyncio.run(run_scalability_benchmark()),
            indent=2,
        )
    )
