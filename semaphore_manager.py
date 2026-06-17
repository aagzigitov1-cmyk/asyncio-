import asyncio

from collections import defaultdict
from urllib.parse import urlparse


class SemaphoreManager:

    def __init__(
        self,
        global_limit: int = 10,
        per_domain_limit: int = 2,
    ):

        self.global_semaphore = asyncio.Semaphore(
            global_limit
        )

        self.per_domain_limit = per_domain_limit

        self.domain_semaphores = defaultdict(
            lambda: asyncio.Semaphore(
                self.per_domain_limit
            )
        )

        self.active_tasks: int = 0
        self.lock = asyncio.Lock()


    def get_domain(self, url: str) -> str:
        domain = urlparse(url).netloc.lower()
        return domain.split(":")[0]

    async def acquire(self, url: str) -> None:

        domain = self.get_domain(url)

        await self.global_semaphore.acquire()
        await self.domain_semaphores[domain].acquire()

        async with self.lock:
            self.active_tasks += 1

    async def release(self, url: str) -> None:
        domain = self.get_domain(url)

        self.domain_semaphores[domain].release()
        self.global_semaphore.release()

        async with self.lock:
            self.active_tasks = max(0, self.active_tasks - 1)

    def stats(
        self,
    ) -> dict[str, int]:

        return {
            "active_tasks": self.active_tasks,
            "domains": len(
                self.domain_semaphores
            ),
        }