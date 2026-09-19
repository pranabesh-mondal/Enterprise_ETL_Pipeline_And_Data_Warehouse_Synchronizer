"""Raw data lake writers (Day 6-7, Week 1).

Raw extracted JSON lands immutably in S3 (or a local folder for dev),
partitioned by source / resource / run so it is auditable and replayable.

Layout:
    raw/{source}/{resource}/run_date={YYYYMMDD}/{run_id}/part-{n:05d}.json
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

MAX_RECORDS_PER_PART = 1000


def build_key(
    source: str, resource: str, run_id: str, part: int = 0
) -> str:
    """Deterministic S3 key for a landing part (unit-testable, no AWS calls)."""
    run_date = run_id[:10].replace("-", "") if "-" in run_id else run_id[:8]
    return (
        f"raw/{source}/{resource}/run_date={run_date}/{run_id}/part-{part:05d}.json"
    )


class S3DataLake:
    """Writes raw JSON batches to AWS S3."""

    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        client: Any = None,
    ) -> None:
        self.bucket = bucket
        self._client = client or boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    def write_json(
        self, records: list[dict[str, Any]], key: str
    ) -> str:
        payload = json.dumps(records, default=str)
        self._client.put_object(
            Bucket=self.bucket, Key=key, Body=payload.encode("utf-8"),
            ContentType="application/json",
        )
        uri = f"s3://{self.bucket}/{key}"
        logger.info("Landed %d records -> %s", len(records), uri)
        return uri

    def write_batch(
        self,
        records: list[dict[str, Any]],
        source: str,
        resource: str,
        run_id: str,
    ) -> list[str]:
        """Chunk records into parts and land each one; returns the S3 URIs."""
        uris: list[str] = []
        for part, start in enumerate(range(0, len(records), MAX_RECORDS_PER_PART)):
            chunk = records[start : start + MAX_RECORDS_PER_PART]
            uris.append(self.write_json(chunk, build_key(source, resource, run_id, part)))
        return uris


class LocalDataLake:
    """Dev/test fallback that mirrors the S3 layout on the local filesystem."""

    def __init__(self, root: str | Path = "data/raw") -> None:
        self.root = Path(root)

    def write_batch(
        self,
        records: list[dict[str, Any]],
        source: str,
        resource: str,
        run_id: str,
    ) -> list[str]:
        uris: list[str] = []
        for part, start in enumerate(range(0, len(records), MAX_RECORDS_PER_PART)):
            chunk = records[start : start + MAX_RECORDS_PER_PART]
            key = build_key(source, resource, run_id, part)
            path = self.root / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(chunk, default=str), encoding="utf-8")
            uri = str(path)
            logger.info("Landed %d records -> %s", len(chunk), uri)
            uris.append(uri)
        return uris


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
