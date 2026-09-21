"""Regression tests for the Week 1 audit fixes (robustness & resilience)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
import responses

from src.extract.base import (
    MAX_RETRY_AFTER_SECONDS,
    RateLimitedSession,
    _parse_retry_after,
)
from src.extract.stripe import StripeExtractor
from src.main import run_extraction
from src.models.base import (
    BaseSchema,
    ExtractionResult,
    Source,
    validate_batch,
)
from src.models.stripe import StripeCustomer
from src.storage.s3_lake import LocalDataLake

STRIPE_URL = "https://api.stripe.com/v1/customers"


# ---- 1. validate_batch must survive non-dict payloads ----

def test_validate_batch_handles_non_dict_payload():
    items: list[Any] = ["not-a-dict", {"id": "cus_ok", "created": 1700000000}]
    result = validate_batch(StripeCustomer, items, Source.STRIPE, "customers")
    assert result.valid_count == 1
    assert result.error_count == 1
    assert result.errors[0]["payload"] == {"_raw": "'not-a-dict'"}


# ---- 2. Retry-After parsing: seconds, HTTP-date, garbage, and capping ----

def test_parse_retry_after_seconds():
    assert _parse_retry_after("2") == 2.0


def test_parse_retry_after_http_date_in_past_is_zero():
    assert _parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0


def test_parse_retry_after_invalid_returns_none():
    assert _parse_retry_after("nonsense") is None
    assert _parse_retry_after(None) is None


def test_retry_after_is_capped():
    assert MAX_RETRY_AFTER_SECONDS == 60.0


@responses.activate
def test_http_date_retry_after_does_not_stall_the_pipeline():
    """A Retry-After HTTP-date in the past must retry immediately, not sleep."""
    responses.add(
        responses.GET, STRIPE_URL, status=429,
        headers={"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"},
    )
    responses.add(
        responses.GET, STRIPE_URL,
        json={"data": [{"id": "cus_1", "created": 1700000000}], "has_more": False},
    )
    started = datetime.now(UTC)
    items = list(StripeExtractor("sk_test").iter_raw("customers"))
    elapsed = (datetime.now(UTC) - started).total_seconds()

    assert len(items) == 1
    assert elapsed < 5, f"retry waited too long: {elapsed}s"


# ---- 3. Configured page size is sent to Stripe, clamped to the API max ----

@responses.activate
def test_configured_page_size_used_and_clamped():
    responses.add(
        responses.GET, STRIPE_URL,
        json={"data": [{"id": "cus_1", "created": 1700000000}], "has_more": False},
    )
    list(StripeExtractor("sk_test", page_size=25).iter_raw("customers"))
    assert "limit=25" in responses.calls[0].request.url

    responses.reset()
    responses.add(
        responses.GET, STRIPE_URL,
        json={"data": [], "has_more": False},
    )
    list(StripeExtractor("sk_test", page_size=5000).iter_raw("customers"))
    assert "limit=100" in responses.calls[0].request.url  # clamped


# ---- 4. Per-resource failure isolation + unsupported resource skipping ----

class _FakeExtractor(BaseSchema):
    """Minimal stand-in exposing the BaseExtractor surface used by main."""

    source: Source = Source.STRIPE
    resource_models: dict[str, type[Any]] = {
        "customers": StripeCustomer,
        "charges": StripeCustomer,
    }

    fail_on: str | None = None

    def extract(self, resource: str, since: datetime | None = None) -> ExtractionResult:
        if resource == self.fail_on:
            raise RuntimeError("simulated API outage")
        return ExtractionResult(
            source=self.source,
            resource=resource,
            total_fetched=1,
            valid_records=[{"external_id": "x", "resource": resource}],
        )


def test_run_extraction_isolates_failing_resource(tmp_path):
    extractor = _FakeExtractor(fail_on="charges")
    lake = LocalDataLake(root=tmp_path)

    summary = run_extraction(extractor, lake, "2026-09-17T10-00-00Z")

    assert summary["charges"]["failed"] == 1      # failure captured, not raised
    assert summary["customers"]["failed"] == 0    # other resource still landed
    assert summary["customers"]["valid"] == 1


def test_run_extraction_skips_unsupported_resource(tmp_path):
    extractor = _FakeExtractor()
    lake = LocalDataLake(root=tmp_path)

    summary = run_extraction(
        extractor, lake, "2026-09-17T10-00-00Z", resources=["customers", "invoices"]
    )

    assert "invoices" not in summary  # unsupported for this source -> skipped
    assert "customers" in summary


# ---- 5. RateLimitedSession exposes a consistent retry budget ----

def test_max_attempts_constant_is_wired():
    from src.extract.base import DEFAULT_MAX_ATTEMPTS

    assert RateLimitedSession.MAX_ATTEMPTS == DEFAULT_MAX_ATTEMPTS


@pytest.mark.parametrize("status", [429, 500, 503])
def test_retryable_statuses_covered(status: int):
    assert status in RateLimitedSession.RETRYABLE_STATUSES
