import asyncio
import contextvars
import logging
import time
from collections import Counter
from collections.abc import Awaitable, Callable


logger = logging.getLogger(__name__)


class CrawlerError(Exception):
    def __init__(
        self,
        message: str,
        *,
        url: str | None = None,
        status: int | None = None,
        retry_after: float | None = None,
    ):
        super().__init__(message)
        self.url = url
        self.status = status
        self.retry_after = retry_after


class TransientError(CrawlerError):
    """Temporary failure such as a timeout, HTTP 429, 500 or 503."""


class PermanentError(CrawlerError):
    """Non-retryable failure such as HTTP 401, 403 or 404."""


class NetworkError(CrawlerError):
    """Connection, DNS or other transport-level failure."""


class ParseError(CrawlerError):
    """Failure while converting a downloaded document into structured data."""


class RetryStrategy:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: list[type[BaseException]] | None = None,
        *,
        retry_limits: dict[type[BaseException], int] | None = None,
        backoff_factors: dict[type[BaseException], float] | None = None,
        sleep_func: Callable[[float], Awaitable] = asyncio.sleep,
    ):
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if backoff_factor < 0:
            raise ValueError("backoff_factor cannot be negative")

        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = tuple(
            retry_on or [TransientError, NetworkError]
        )
        self.retry_limits = retry_limits or {}
        self.backoff_factors = backoff_factors or {}
        self._sleep = sleep_func
        self._attempt = contextvars.ContextVar(
            "retry_attempt",
            default=0,
        )

        self.error_counts: Counter[str] = Counter()
        self.successful_retries = 0
        self.retry_attempts = 0
        self.total_retry_wait = 0.0
        self.permanent_error_urls: set[str] = set()
        self.history: list[dict] = []

    @property
    def current_attempt(self) -> int:
        return self._attempt.get()

    def _matching_type(
        self,
        error: BaseException,
        mapping: dict[type[BaseException], object],
    ) -> type[BaseException] | None:
        return next(
            (error_type for error_type in mapping if isinstance(error, error_type)),
            None,
        )

    def _retry_limit(self, error: BaseException) -> int:
        error_type = self._matching_type(error, self.retry_limits)
        if error_type is None:
            return self.max_retries
        return self.retry_limits[error_type]

    def _backoff(self, error: BaseException, retry_number: int) -> float:
        retry_after = getattr(error, "retry_after", None)
        if retry_after is not None:
            return max(0.0, float(retry_after))

        error_type = self._matching_type(error, self.backoff_factors)
        factor = (
            self.backoff_factors[error_type]
            if error_type is not None
            else self.backoff_factor
        )
        return factor * (2 ** retry_number)

    async def execute_with_retry(self, coro, *args, **kwargs):
        retries_done = 0
        started = time.monotonic()

        while True:
            token = self._attempt.set(retries_done)
            try:
                result = await coro(*args, **kwargs)
                if retries_done:
                    self.successful_retries += 1
                    logger.info(
                        "Retry succeeded | attempt=%s | elapsed=%.2fs",
                        retries_done + 1,
                        time.monotonic() - started,
                    )
                return result

            except Exception as error:
                error_name = type(error).__name__
                url = getattr(error, "url", None)
                self.error_counts[error_name] += 1

                if isinstance(error, PermanentError) and url:
                    self.permanent_error_urls.add(url)

                should_retry = isinstance(error, self.retry_on)
                retry_limit = self._retry_limit(error)

                if not should_retry or retries_done >= retry_limit:
                    error.attempts = retries_done + 1
                    logger.error(
                        "Retry failed permanently | type=%s | url=%s "
                        "| attempt=%s | error=%s",
                        error_name,
                        url or "unknown",
                        retries_done + 1,
                        error,
                    )
                    self.history.append(
                        {
                            "type": error_name,
                            "url": url,
                            "attempt": retries_done + 1,
                            "retry": False,
                            "message": str(error),
                        }
                    )
                    raise

                delay = self._backoff(error, retries_done)
                self.retry_attempts += 1
                self.total_retry_wait += delay
                self.history.append(
                    {
                        "type": error_name,
                        "url": url,
                        "attempt": retries_done + 1,
                        "retry": True,
                        "delay": delay,
                        "message": str(error),
                    }
                )
                logger.warning(
                    "Retry scheduled | type=%s | url=%s | attempt=%s/%s "
                    "| delay=%.2fs | error=%s",
                    error_name,
                    url or "unknown",
                    retries_done + 1,
                    retry_limit + 1,
                    delay,
                    error,
                )
                await self._sleep(delay)
                retries_done += 1

            finally:
                self._attempt.reset(token)

    def get_stats(self) -> dict:
        average_retry_wait = (
            self.total_retry_wait / self.retry_attempts
            if self.retry_attempts
            else 0.0
        )
        return {
            "errors_by_type": dict(self.error_counts),
            "successful_retries": self.successful_retries,
            "retry_attempts": self.retry_attempts,
            "average_retry_wait": average_retry_wait,
            "permanent_error_urls": sorted(self.permanent_error_urls),
        }
