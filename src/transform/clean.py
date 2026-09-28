"""Cleaning & standardization helpers - Week 2, Day 1-3.

Handles the messy reality of third-party APIs:
  * nulls, empty strings and "N/A"-style sentinels
  * epoch-seconds / epoch-millis / ISO-8601 / date-only timestamps -> UTC
  * currency casing + ISO-4217 zero-decimal currencies (JPY, KRW, ...)
  * amounts expressed in minor units (cents) -> Decimal major units
  * duplicate records

The pure helpers below are engine-agnostic; `clean_frame` (Polars) and
`clean_frame_pandas` (Pandas) apply them to whole DataFrames.
"""

from __future__ import annotations

import math
import re
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd

NULL_TOKENS = frozenset({"", "null", "none", "nan", "n/a", "na", "-", "--"})
CURRENCY_RE = re.compile(r"^[A-Za-z]{3}$")
EPOCH_RE = re.compile(r"^(\d{10}|\d{13})(\.\d+)?$")

# ISO-4217 currencies that have no minor unit (exponent 0).
ZERO_DECIMAL_CURRENCIES = frozenset(
    {
        "BIF", "CLP", "DJF", "GNF", "JPY", "KMF", "KRW", "MGA",
        "PYG", "RWF", "UGX", "VND", "VUV", "XAF", "XOF", "XPF",
    }
)


def is_missing(value: Any) -> bool:
    """True for None, NaN, NaT and string sentinels like "" or "N/A"."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        return value.strip().lower() in NULL_TOKENS
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def clean_string(value: Any, *, title_case: bool = False) -> str | None:
    """Trim, collapse internal whitespace, and null-out sentinel values."""
    if is_missing(value):
        return None
    text = " ".join(str(value).split())
    if not text or text.lower() in NULL_TOKENS:
        return None
    return text.title() if title_case else text


def standardize_currency(value: Any) -> str | None:
    """Normalize a currency code to upper-case ISO-4217 (else None)."""
    if is_missing(value):
        return None
    text = str(value).strip().upper()
    return text if CURRENCY_RE.match(text) else None


def currency_exponent(currency: str | None) -> int:
    """Decimal exponent for a currency (0 for zero-decimal currencies)."""
    return 0 if (currency or "").upper() in ZERO_DECIMAL_CURRENCIES else 2


def minor_to_major(minor: Any, currency: str | None) -> Decimal | None:
    """Convert a minor-unit amount (e.g. cents) to a Decimal major amount."""
    if is_missing(minor):
        return None
    try:
        amount = Decimal(str(minor).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    if amount < 0:
        return None
    exponent = currency_exponent(currency)
    quantum = Decimal(1).scaleb(-exponent)
    return (amount / (Decimal(10) ** exponent)).quantize(quantum)


def major_to_minor(amount: Any, currency: str | None) -> int | None:
    """Convert a major-unit amount (Salesforce) to minor units."""
    if is_missing(amount):
        return None
    try:
        value = Decimal(str(amount).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    if value < 0:
        return None
    return int((value * (Decimal(10) ** currency_exponent(currency))).to_integral_value())


def standardize_datetime(value: Any) -> datetime | None:
    """Normalize epoch / ISO-8601 / date-only values to a UTC datetime."""
    if is_missing(value):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    if isinstance(value, (int, float)):
        return _epoch_to_datetime(float(value))
    if isinstance(value, str):
        text = value.strip()
        if EPOCH_RE.match(text):
            return _epoch_to_datetime(float(text))
        if text.endswith(("Z", "z")):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def _epoch_to_datetime(epoch: float) -> datetime | None:
    """Epoch seconds, or milliseconds when the magnitude implies ms."""
    if abs(epoch) > 1e11:  # 13-digit millis
        epoch = epoch / 1000.0
    try:
        return datetime.fromtimestamp(epoch, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None
