"""Salesforce extractor with cursor-based pagination (Day 3-5, Week 1).

Auth: OAuth2 username-password flow. Pagination: Salesforce returns
`nextRecordsUrl` when a SOQL query exceeds the batch size; we follow it
until `done` is true. Incremental pulls append a LastModifiedDate filter.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from typing import Any

from src.models.base import BaseSchema, Source
from src.models.salesforce import (
    SALESFORCE_RESOURCE_MODELS,
    SALESFORCE_RESOURCE_QUERIES,
)
from src.utils.logging_config import get_logger

from .base import BaseExtractor, RateLimitedSession

logger = get_logger(__name__)


class SalesforceExtractor(BaseExtractor):
    source = Source.SALESFORCE
    resource_models: dict[str, type[BaseSchema]] = SALESFORCE_RESOURCE_MODELS
    API_VERSION = "v60.0"

    def __init__(
        self,
        username: str,
        password: str,
        security_token: str,
        login_url: str = "https://login.salesforce.com",
        session: RateLimitedSession | None = None,
    ) -> None:
        super().__init__(session or RateLimitedSession())
        self._login_url = login_url.rstrip("/")
        self._credentials = {
            "grant_type": "password",
            "username": username,
            "password": f"{password}{security_token}",
        }
        self._instance_url: str | None = None
        self._authenticated = False

    def authenticate(self) -> None:
        """Obtain an OAuth2 access token (idempotent)."""
        if self._authenticated:
            return
        response = self._session.post(
            f"{self._login_url}/services/oauth2/token", data=self._credentials
        )
        payload = response.json()
        self._instance_url = payload["instance_url"]
        self._session.set_auth_header(f"Bearer {payload['access_token']}")
        self._authenticated = True
        logger.info("Salesforce authenticated (instance: %s)", self._instance_url)

    def iter_raw(
        self, resource: str, since: datetime | None = None
    ) -> Iterator[dict[str, Any]]:
        """Run a SOQL query and follow nextRecordsUrl until done."""
        if resource not in SALESFORCE_RESOURCE_QUERIES:
            raise ValueError(
                f"Unknown resource '{resource}'. Known: {sorted(SALESFORCE_RESOURCE_QUERIES)}"
            )

        self.authenticate()
        soql = SALESFORCE_RESOURCE_QUERIES[resource]
        if since is not None:
            soql += f" WHERE LastModifiedDate > {since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            logger.info(
                "Salesforce incremental pull: %s modified after %s", resource, since
            )

        url: str | None = (
            f"{self._instance_url}/services/data/{self.API_VERSION}/query"
        )
        params: dict[str, Any] | None = {"q": soql}
        total = 0

        while url is not None:
            response = self._session.get(url, params=params)
            body = response.json()
            records = body.get("records", [])
            total += len(records)
            yield from records

            if body.get("done", True):
                url = None
            else:
                # Cursor: Salesforce-provided locator for the next batch.
                url = f"{self._instance_url}{body['nextRecordsUrl']}"
                params = None  # locator requests must not repeat the q param

        logger.info("Salesforce '%s': fetched %d records", resource, total)
