"""Extraction base layer: rate-limit-aware HTTP session + extractor ABC.

Day 6-7 (Week 1): rate-limit handling via Tenacity - exponential backoff
with jitter, honoring `Retry-After` headers, retrying only transient
failures (429 / 5xx / network errors).
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from src.models.base import BaseSchema, ExtractionResult, Source, validate_batch
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class TransientAPIError(Exception):
    """Raised for retryable HTTP statuses (429 / 5xx) and network errors."""

    def __init__(self, message: str, retry_after: str | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


DEFAULT_MAX_ATTEMPTS = 5
MAX_RETRY_AFTER_SECONDS = 60.0


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a `Retry-After` header as either delay-seconds or an HTTP-date."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        target = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if target is None:
        return None
    if target.tzinfo is None:
        target = target.replace(tzinfo=UTC)
    return max(0.0, (target - datetime.now(UTC)).total_seconds())


def _wait_with_retry_after(retry_state: Any) -> float:
    """Tenacity wait strategy: honor Retry-After if present, else exponential jitter.

    Waits from `Retry-After` are capped so a hostile/large value cannot stall
    the pipeline indefinitely.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    hinted = _parse_retry_after(getattr(exc, "retry_after", None))
    if hinted is not None:
        return min(hinted, MAX_RETRY_AFTER_SECONDS)
    return wait_exponential_jitter(initial=1, max=30)(retry_state)


class RateLimitedSession:
    """A `requests.Session` wrapper with automatic retry on transient failures."""

    RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
    MAX_ATTEMPTS = DEFAULT_MAX_ATTEMPTS

    def __init__(
        self,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
    ) -> None:
        self._session = requests.Session()
        if headers:
            self._session.headers.update(headers)
        self.timeout = timeout

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("POST", url, **kwargs)

    @retry(
        retry=retry_if_exception_type(TransientAPIError),
        wait=_wait_with_retry_after,
        stop=stop_after_attempt(DEFAULT_MAX_ATTEMPTS),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", self.timeout)
        try:
            response = self._session.request(method, url, **kwargs)
        except requests.RequestException as exc:
            raise TransientAPIError(f"Network error calling {url}: {exc}") from exc

        if response.status_code in self.RETRYABLE_STATUSES:
            raise TransientAPIError(
                f"{method} {url} returned {response.status_code}",
                retry_after=response.headers.get("Retry-After"),
            )

        response.raise_for_status()
        return response

    def set_auth_header(self, value: str) -> None:
        self._session.headers["Authorization"] = value

    def close(self) -> None:
        self._session.close()


class BaseExtractor(ABC):
    """Contract for all API extractors (Day 3-5, Week 1)."""

    source: Source
    resource_models: dict[str, type[BaseSchema]]

    def __init__(self, session: RateLimitedSession) -> None:
        self._session = session

    @abstractmethod
    def iter_raw(
        self, resource: str, since: datetime | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield raw JSON records for `resource`, incrementally after `since`."""

    def extract(
        self, resource: str, since: datetime | None = None
    ) -> ExtractionResult:
        """Fetch, cursor-paginate, and Pydantic-validate one resource."""
        if resource not in self.resource_models:
            raise ValueError(
                f"Unknown resource '{resource}' for source "
                f"'{self.source.value}'. Known: {sorted(self.resource_models)}"
            )
        items = list(self.iter_raw(resource, since=since))
        result = validate_batch(
            self.resource_models[resource], items, self.source, resource
        )
        logger.info(
            "Extraction complete: source=%s resource=%s fetched=%d valid=%d errors=%d",
            self.source.value,
            resource,
            result.total_fetched,
            result.valid_count,
            result.error_count,
        )
        return result

    def close(self) -> None:
        self._session.close()
