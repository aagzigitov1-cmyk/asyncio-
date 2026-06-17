import asyncio
import time
import random
from collections import defaultdict, deque


class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
        min_delay: float = 0.5,
        jitter: float = 0.2,
    ):
        self.rps = requests_per_second
        self.per_domain = per_domain

        self.min_interval = 1.0 / requests_per_second
        self.min_delay = min_delay
        self.jitter = jitter

        self.calls = defaultdict(deque)
        self.lock = asyncio.Lock()

    def _key(self, domain: str | None):
        return domain if self.per_domain and domain else "global"

    async def acquire(self, domain: str | None = None):
        async with self.lock:
            key = self._key(domain)
            now = time.monotonic()

            q = self.calls[key]

            # чистим старые (окно 1 сек)
            while q and now - q[0] > 1:
                q.popleft()

            # если превышен лимит
            if len(q) >= self.rps:
                wait = self.min_interval - (now - q[0])
                if wait > 0:
                    await asyncio.sleep(wait)

            # human delay
            if self.min_delay:
                await asyncio.sleep(self.min_delay)

            # jitter
            if self.jitter:
                await asyncio.sleep(random.uniform(0, self.jitter))

            q.append(time.monotonic())