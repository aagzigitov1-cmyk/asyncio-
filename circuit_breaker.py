import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum

from retry_strategy import CrawlerError


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(CrawlerError):
    def __init__(
        self,
        domain: str,
        *,
        url: str | None = None,
        retry_after: float = 0.0,
    ):
        super().__init__(
            f"Circuit is open for {domain}; retry after {retry_after:.2f}s",
            url=url,
            retry_after=retry_after,
        )
        self.domain = domain


@dataclass
class DomainCircuit:
    state: CircuitState = CircuitState.CLOSED
    failures: deque = field(default_factory=deque)
    opened_at: float | None = None
    half_open_probe_active: bool = False


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        failure_window: float = 60.0,
        *,
        clock=time.monotonic,
    ):
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be greater than zero")
        if recovery_timeout < 0 or failure_window <= 0:
            raise ValueError("invalid circuit breaker timing")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_window = failure_window
        self._clock = clock
        self._circuits = defaultdict(DomainCircuit)
        self._lock = asyncio.Lock()
        self.opened_count = 0
        self.blocked_requests = 0
        self.recovered_count = 0

    def _trim_failures(self, circuit: DomainCircuit, now: float) -> None:
        while (
            circuit.failures
            and now - circuit.failures[0] > self.failure_window
        ):
            circuit.failures.popleft()

    async def before_request(
        self,
        domain: str,
        *,
        url: str | None = None,
    ) -> None:
        domain = domain.lower()
        async with self._lock:
            circuit = self._circuits[domain]
            now = self._clock()
            self._trim_failures(circuit, now)

            if circuit.state == CircuitState.OPEN:
                opened_at = (
                    circuit.opened_at
                    if circuit.opened_at is not None
                    else now
                )
                elapsed = now - opened_at
                if elapsed >= self.recovery_timeout:
                    circuit.state = CircuitState.HALF_OPEN
                    circuit.half_open_probe_active = True
                    return

                self.blocked_requests += 1
                raise CircuitOpenError(
                    domain,
                    url=url,
                    retry_after=self.recovery_timeout - elapsed,
                )

            if circuit.state == CircuitState.HALF_OPEN:
                if not circuit.half_open_probe_active:
                    circuit.half_open_probe_active = True
                    return
                self.blocked_requests += 1
                raise CircuitOpenError(
                    domain,
                    url=url,
                    retry_after=self.recovery_timeout,
                )

    async def record_success(self, domain: str) -> None:
        domain = domain.lower()
        async with self._lock:
            circuit = self._circuits[domain]
            if circuit.state != CircuitState.CLOSED:
                self.recovered_count += 1
            circuit.state = CircuitState.CLOSED
            circuit.failures.clear()
            circuit.opened_at = None
            circuit.half_open_probe_active = False

    async def record_failure(self, domain: str) -> None:
        domain = domain.lower()
        async with self._lock:
            circuit = self._circuits[domain]
            now = self._clock()
            self._trim_failures(circuit, now)
            circuit.failures.append(now)

            should_open = (
                circuit.state == CircuitState.HALF_OPEN
                or len(circuit.failures) >= self.failure_threshold
            )
            if should_open:
                if circuit.state != CircuitState.OPEN:
                    self.opened_count += 1
                circuit.state = CircuitState.OPEN
                circuit.opened_at = now
                circuit.half_open_probe_active = False

    async def get_state(self, domain: str) -> CircuitState:
        async with self._lock:
            return self._circuits[domain.lower()].state

    def get_stats(self) -> dict:
        now = self._clock()
        domains = {}
        for domain, circuit in self._circuits.items():
            self._trim_failures(circuit, now)
            retry_after = 0.0
            if circuit.state == CircuitState.OPEN:
                opened_at = (
                    circuit.opened_at
                    if circuit.opened_at is not None
                    else now
                )
                retry_after = max(
                    0.0,
                    self.recovery_timeout - (now - opened_at),
                )
            domains[domain] = {
                "state": circuit.state.value,
                "failures": len(circuit.failures),
                "retry_after": retry_after,
            }
        return {
            "opened": self.opened_count,
            "blocked_requests": self.blocked_requests,
            "recovered": self.recovered_count,
            "domains": domains,
        }
