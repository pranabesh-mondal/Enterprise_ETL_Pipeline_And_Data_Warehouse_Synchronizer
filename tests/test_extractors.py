"""Tests for cursor-based pagination and rate-limit retry behavior."""

from __future__ import annotations

from datetime import datetime, timezone

import responses

from src.extract.salesforce import SalesforceExtractor
from src.extract.stripe import StripeExtractor
from src.models.base import Source

STRIPE_URL = "https://api.stripe.com/v1/customers"
SF_INSTANCE = "https://test.my.salesforce.com"
SF_TOKEN_URL = f"https://login.salesforce.com/services/oauth2/token"


@responses.activate
def test_stripe_cursor_pagination_follows_starting_after():
    responses.add(
        responses.GET, STRIPE_URL,
        json={"data": [{"id": "cus_1", "created": 1700000000},
                       {"id": "cus_2", "created": 1700000100}],
              "has_more": True},
    )
    responses.add(
        responses.GET, STRIPE_URL,
        json={"data": [{"id": "cus_3", "created": 1700000200}],
              "has_more": False},
    )
    extractor = StripeExtractor("sk_test_dummy")
    items = list(extractor.iter_raw("customers"))

    assert [i["id"] for i in items] == ["cus_1", "cus_2", "cus_3"]
    assert "starting_after=cus_2" in responses.calls[1].request.url


@responses.activate
def test_stripe_rate_limit_retries_and_honors_retry_after():
    # First call: 429 with Retry-After: 0 (instant retry). Second: success.
    responses.add(responses.GET, STRIPE_URL, status=429,
                  headers={"Retry-After": "0"})
    responses.add(
        responses.GET, STRIPE_URL,
        json={"data": [{"id": "cus_1", "created": 1700000000}],
              "has_more": False},
    )
    extractor = StripeExtractor("sk_test_dummy")
    items = list(extractor.iter_raw("customers"))
    assert len(items) == 1
    assert len(responses.calls) == 2


@responses.activate
def test_stripe_extract_validates_and_quarantines():
    responses.add(
        responses.GET, STRIPE_URL,
        json={"data": [{"id": "cus_1", "created": 1700000000},
                       {"id": "cus_bad", "created": "garbage"}],
              "has_more": False},
    )
    extractor = StripeExtractor("sk_test_dummy")
    result = extractor.extract("customers")
    assert result.source == Source.STRIPE
    assert result.valid_count == 1
    assert result.error_count == 1


@responses.activate
def test_salesforce_follows_next_records_url():
    responses.add(responses.POST, SF_TOKEN_URL,
                  json={"access_token": "tok", "instance_url": SF_INSTANCE})
    responses.add(
        responses.GET, f"{SF_INSTANCE}/services/data/v60.0/query",
        json={"records": [{"Id": "001A", "Name": "Acme"}], "done": False,
              "nextRecordsUrl": "/services/data/v60.0/query/01gXX"},
    )
    responses.add(
        responses.GET, f"{SF_INSTANCE}/services/data/v60.0/query/01gXX",
        json={"records": [{"Id": "001B", "Name": "Beta"}], "done": True},
    )
    extractor = SalesforceExtractor(
        "u@x.com", "pw", "token", login_url="https://login.salesforce.com"
    )
    items = list(extractor.iter_raw("accounts"))

    assert [i["Id"] for i in items] == ["001A", "001B"]
    # Locator request must NOT repeat the q param
    assert "q=" not in responses.calls[2].request.url


@responses.activate
def test_salesforce_incremental_adds_modified_filter():
    responses.add(responses.POST, SF_TOKEN_URL,
                  json={"access_token": "tok", "instance_url": SF_INSTANCE})
    responses.add(
        responses.GET, f"{SF_INSTANCE}/services/data/v60.0/query",
        json={"records": [], "done": True},
    )
    extractor = SalesforceExtractor("u@x.com", "pw", "token")
    list(extractor.iter_raw("accounts", since=datetime(2026, 9, 1, tzinfo=timezone.utc)))

    assert "LastModifiedDate" in responses.calls[1].request.url
