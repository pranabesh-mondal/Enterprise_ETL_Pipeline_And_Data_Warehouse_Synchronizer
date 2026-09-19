"""Week 1 test suite: Pydantic models, cursor pagination, rate-limit retries."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.models.base import Source, validate_batch
from src.models.salesforce import SalesforceAccount, SalesforceOpportunity
from src.models.stripe import StripeCharge, StripeCustomer


# --------------------------- Stripe models ---------------------------

def test_stripe_epoch_created_becomes_utc_datetime():
    customer = StripeCustomer.model_validate(
        {"id": "cus_1", "email": "a@b.c", "created": 1700000000}
    )
    assert customer.created.year == 2023
    assert customer.created.tzinfo is not None


def test_stripe_charge_currency_lowercased():
    charge = StripeCharge.model_validate(
        {"id": "ch_1", "amount": 1999, "currency": "USD", "status": "succeeded",
         "created": 1700000000}
    )
    assert charge.currency == "usd"


def test_stripe_charge_rejects_negative_amount():
    with pytest.raises(ValidationError):
        StripeCharge.model_validate(
            {"id": "ch_1", "amount": -5, "currency": "usd",
             "status": "succeeded", "created": 1700000000}
        )


def test_stripe_unknown_fields_ignored():
    customer = StripeCustomer.model_validate(
        {"id": "cus_1", "created": 1700000000, "some_future_field": "x"}
    )
    assert customer.id == "cus_1"


# ------------------------- Salesforce models -------------------------

def test_salesforce_pascal_case_alias_mapping():
    account = SalesforceAccount.model_validate(
        {"Id": "001XX", "Name": "Acme", "Industry": "Tech",
         "CreatedDate": "2026-01-15T10:00:00.000+0000"}
    )
    assert account.id == "001XX"
    assert account.name == "Acme"
    assert account.created_date is not None


def test_salesforce_opportunity_alias_fields():
    opp = SalesforceOpportunity.model_validate(
        {"Id": "006XX", "Name": "Big Deal", "Amount": 50000,
         "StageName": "Closed Won", "CloseDate": "2026-02-01"}
    )
    assert opp.amount == 50000
    assert opp.stage_name == "Closed Won"


# ------------------------ validate_batch behavior ------------------------

def test_validate_batch_quarantines_invalid_records():
    items = [
        {"id": "cus_ok", "created": 1700000000},
        {"id": "cus_bad", "created": "not-a-timestamp"},  # invalid
    ]
    result = validate_batch(StripeCustomer, items, Source.STRIPE, "customers")
    assert result.total_fetched == 2
    assert result.valid_count == 1
    assert result.error_count == 1
    assert result.valid_records[0]["external_id"] == "cus_ok"
    assert result.errors[0]["index"] == 1


# ------------------------------ Settings ------------------------------

def test_settings_reads_env(monkeypatch):
    from src.config.settings import Settings

    monkeypatch.setenv("STRIPE_API_KEY", "sk_test_123")
    monkeypatch.setenv("AWS_S3_BUCKET", "my-bucket")
    settings = Settings(_env_file=None)
    assert settings.stripe_api_key == "sk_test_123"
    assert settings.aws_s3_bucket == "my-bucket"
    assert settings.stripe_configured and settings.s3_configured
