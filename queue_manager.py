import asyncio
from dataclasses import dataclass
from urllib.parse import urldefrag

@dataclass
class QueueItem:
    url: str
    depth: int
    priority: int = 0


class CrawlerQueue:

    def __init__(self):
        self.queue = asyncio.PriorityQueue()

        self.processed: set[str] = set()

        self.failed: dict[str, str] = {}

        self.seen_urls: set[str] = set()

        self.counter = 0
        self.lock = asyncio.Lock()

    async def add_url(
        self,
        url: str,
        depth: int = 0,
        priority: int = 0,
    ):

        url, _ = urldefrag(url)

        async with self.lock:

            if url in self.seen_urls:
                return False

            self.seen_urls.add(url)

            self.queue.put_nowait(
                (
                    priority,
                    self.counter,
                    QueueItem(
                        url=url,
                        depth=depth,
                        priority=priority,
                    ),
                )
            )

            self.counter += 1

        return True


    async def get_next(self) -> QueueItem:
        _, _, item = await self.queue.get()
        return item

    def mark_processed(
        self,
        url: str,
    ):

        self.processed.add(url)

    def mark_failed(
        self,
        url: str,
        error: str,
    ):

        self.failed[url] = error

    def get_stats(
        self,
    ) -> dict:

        return {
            "queued": self.queue.qsize(),
            "processed": len(self.processed),
            "failed": len(self.failed),
            "seen": len(self.seen_urls),
        }