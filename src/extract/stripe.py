"""Stripe extractor with cursor-based pagination (Day 3-5, Week 1).

Stripe list endpoints paginate with `starting_after` + `has_more`.
Incremental pulls use the `created[gt]` filter against the last
high-water mark.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator

from src.models.stripe import STRIPE_RESOURCE_MODELS
from src.models.base import BaseSchema, Source
from src.utils.logging_config import get_logger

from .base import BaseExtractor, RateLimitedSession

logger = get_logger(__name__)


class StripeExtractor(BaseExtractor):
    source = Source.STRIPE
    resource_models: dict[str, type[BaseSchema]] = STRIPE_RESOURCE_MODELS

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.stripe.com",
        session: RateLimitedSession | None = None,
    ) -> None:
        session = session or RateLimitedSession(
            headers={"Authorization": f"Bearer {api_key}"}
        )
        super().__init__(session)
        self._base_url = base_url.rstrip("/")

    def iter_raw(
        self, resource: str, since: datetime | None = None
    ) -> Iterator[dict[str, Any]]:
        """Walk all pages of a Stripe list endpoint via cursor pagination."""
        url = f"{self._base_url}/v1/{resource}"
        params: dict[str, Any] = {"limit": 100}
        if since is not None:
            params["created[gt]"] = int(since.timestamp())
            logger.info("Stripe incremental pull: %s created after %s", resource, since)

        starting_after: str | None = None
        page_number = 0
        total = 0

        while True:
            if starting_after is not None:
                params["starting_after"] = starting_after

            response = self._session.get(url, params=params)
            body = response.json()
            data = body.get("data", [])
            total += len(data)
            page_number += 1
            logger.debug(
                "Stripe page %d for %s: %d records", page_number, resource, len(data)
            )

            yield from data

            if not body.get("has_more") or not data:
                break
            # Cursor: the id of the last record on this page.
            starting_after = data[-1]["id"]

        logger.info("Stripe '%s': fetched %d records across %d pages",
                    resource, total, page_number)
