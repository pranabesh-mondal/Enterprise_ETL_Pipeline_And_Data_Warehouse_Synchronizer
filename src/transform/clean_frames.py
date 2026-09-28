"""DataFrame-level cleaning for both engines - Week 2, Day 1-3.

`clean_frame` and `deduplicate_frame` are the Polars (primary) path;
`clean_frame_pandas` and `deduplicate_frame_pandas` are the Pandas
fallback. Both apply the same rules from `src.transform.clean`.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl

from src.transform.clean import (
    NULL_TOKENS,
    clean_string,
    standardize_currency,
    standardize_datetime,
)


def _null_token_condition(column: str) -> pl.Expr:
    """True when a stripped, case-insensitive value is a null sentinel.

    Casting first keeps this safe for all-null / non-string columns.
    """
    text = pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars()
    return text.str.to_lowercase().is_in(list(NULL_TOKENS))


def build_frame(records: list[dict[str, Any]]) -> pl.DataFrame:
    """Create a Polars DataFrame from a list of record dicts."""
    if not records:
        return pl.DataFrame()
    return pl.DataFrame(records, infer_schema_length=len(records))


def clean_frame(
    df: pl.DataFrame,
    *,
    string_columns: tuple[str, ...] = (),
    datetime_columns: tuple[str, ...] = (),
    currency_columns: tuple[str, ...] = (),
    numeric_columns: tuple[str, ...] = (),
) -> pl.DataFrame:
    """Apply standard cleaning rules to a Polars frame (primary engine)."""
    if df.is_empty():
        return df

    expressions: list[pl.Expr] = []
    for column in string_columns:
        if column not in df.columns:
            continue
        trimmed = pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars()
        expressions.append(
            pl.when(_null_token_condition(column))
            .then(None)
            .otherwise(trimmed.str.replace_all(r"\s+", " "))
            .alias(column)
        )
    for column in datetime_columns:
        if column not in df.columns:
            continue
        expressions.append(
            pl.col(column)
            .cast(pl.Utf8, strict=False)
            .map_elements(
                standardize_datetime, return_dtype=pl.Datetime("us", "UTC")
            )
            .alias(column)
        )
    for column in currency_columns:
        if column not in df.columns:
            continue
        expressions.append(
            pl.col(column)
            .cast(pl.Utf8, strict=False)
            .map_elements(standardize_currency, return_dtype=pl.Utf8)
            .alias(column)
        )
    for column in numeric_columns:
        if column not in df.columns:
            continue
        expressions.append(pl.col(column).cast(pl.Int64, strict=False).alias(column))

    return df.with_columns(expressions) if expressions else df


def deduplicate_frame(
    df: pl.DataFrame,
    key_columns: list[str] | tuple[str, ...],
    order_column: str | None = None,
) -> pl.DataFrame:
    """Keep one row per key - the newest by `order_column` when given."""
    if df.is_empty():
        return df
    keys = [column for column in key_columns if column in df.columns]
    if not keys:
        return df
    if order_column and order_column in df.columns:
        df = df.sort(order_column, nulls_last=False)
    return df.unique(subset=keys, keep="last")


def clean_frame_pandas(
    df: pd.DataFrame,
    *,
    string_columns: tuple[str, ...] = (),
    datetime_columns: tuple[str, ...] = (),
    currency_columns: tuple[str, ...] = (),
    numeric_columns: tuple[str, ...] = (),
) -> pd.DataFrame:
    """Pandas equivalent of `clean_frame` (fallback / notebook engine)."""
    if df.empty:
        return df
    df = df.copy()
    for column in string_columns:
        if column in df.columns:
            df[column] = df[column].map(clean_string)
    for column in datetime_columns:
        if column in df.columns:
            df[column] = df[column].map(standardize_datetime)
    for column in currency_columns:
        if column in df.columns:
            df[column] = df[column].map(standardize_currency)
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    return df.astype(object).where(pd.notna(df), None)


def deduplicate_frame_pandas(
    df: pd.DataFrame,
    key_columns: list[str] | tuple[str, ...],
    order_column: str | None = None,
) -> pd.DataFrame:
    """Pandas equivalent of `deduplicate_frame`."""
    if df.empty:
        return df
    keys = [column for column in key_columns if column in df.columns]
    if not keys:
        return df
    if order_column and order_column in df.columns:
        df = df.sort_values(order_column, na_position="first")
    return df.drop_duplicates(subset=keys, keep="last")


def build_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Create a Pandas DataFrame from a list of record dicts."""
    return pd.DataFrame(records)
