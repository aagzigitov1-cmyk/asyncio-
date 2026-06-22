import asyncio
import random
import time
from collections import defaultdict, deque


class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
    ):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be greater than zero")
        if min_delay < 0 or jitter < 0:
            raise ValueError("min_delay and jitter cannot be negative")

        self.rps = requests_per_second
        self.per_domain = per_domain
        self.min_interval = 1.0 / requests_per_second
        self.min_delay = min_delay
        self.jitter = jitter

        self._locks = defaultdict(asyncio.Lock)
        self._last_request: dict[str, float] = {}
        self._domain_delays: dict[str, float] = {}
        self._request_times = deque()
        self._observed_intervals = deque(maxlen=1000)
        self._metrics_lock = asyncio.Lock()
        self.request_count = 0
        self.total_wait_time = 0.0

    def _key(self, domain: str | None) -> str:
        if self.per_domain and domain:
            return domain.lower()
        return "global"

    def set_domain_delay(self, domain: str, delay: float | None) -> None:
        key = self._key(domain)
        if delay and delay > 0:
            self._domain_delays[key] = delay
        else:
            self._domain_delays.pop(key, None)

    async def acquire(self, domain: str | None = None) -> float:
        key = self._key(domain)

        async with self._locks[key]:
            now = time.monotonic()
            interval = max(
                self.min_interval,
                self.min_delay,
                self._domain_delays.get(key, 0.0),
            )

            wait_time = 0.0
            previous = self._last_request.get(key)
            if previous is not None:
                wait_time = max(0.0, previous + interval - now)
                if self.jitter:
                    wait_time += random.uniform(0.0, self.jitter)

            if wait_time:
                await asyncio.sleep(wait_time)

            requested_at = time.monotonic()
            previous = self._last_request.get(key)
            self._last_request[key] = requested_at

            async with self._metrics_lock:
                self.request_count += 1
                self.total_wait_time += wait_time
                self._request_times.append(requested_at)
                if previous is not None:
                    self._observed_intervals.append(requested_at - previous)
                self._trim_request_times(requested_at)

            return wait_time

    def _trim_request_times(self, now: float | None = None) -> None:
        now = now if now is not None else time.monotonic()
        while self._request_times and now - self._request_times[0] > 1.0:
            self._request_times.popleft()

    def get_stats(self) -> dict[str, float | int]:
        self._trim_request_times()
        average_delay = (
            sum(self._observed_intervals) / len(self._observed_intervals)
            if self._observed_intervals
            else 0.0
        )

        return {
            "request_count": self.request_count,
            "current_requests_per_second": len(self._request_times),
            "average_delay": average_delay,
            "total_wait_time": self.total_wait_time,
        }
