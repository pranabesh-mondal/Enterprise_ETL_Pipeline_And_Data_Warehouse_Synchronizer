"""Week 1 pipeline entrypoint: Extract -> validate -> land raw JSON.

Usage:
    python -m src.main                          # all configured sources
    python -m src.main --local                  # land to ./data/raw instead of S3
    python -m src.main --resources customers    # only Stripe customers
    python -m src.main --since 2026-09-10T00:00:00+00:00
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from src.config.settings import get_settings
from src.extract.base import BaseExtractor
from src.extract.salesforce import SalesforceExtractor
from src.extract.stripe import StripeExtractor
from src.storage.s3_lake import LocalDataLake, S3DataLake, new_run_id
from src.utils.logging_config import get_logger, setup_logging

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enterprise ETL - extraction run")
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=["stripe", "salesforce"],
        default=["stripe", "salesforce"],
    )
    parser.add_argument(
        "--resources",
        nargs="+",
        default=None,
        help="Restrict to specific resources (defaults to all per source)",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="ISO-8601 incremental cutoff (high-water mark)",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Write raw data to ./data/raw instead of S3 (dev mode)",
    )
    return parser.parse_args()


def build_extractors(settings, sources: list[str]) -> list[BaseExtractor]:
    extractors: list[BaseExtractor] = []
    for source in sources:
        if source == "stripe":
            if not settings.stripe_configured:
                logger.warning("Stripe credentials missing - skipping.")
                continue
            extractors.append(
                StripeExtractor(settings.stripe_api_key, settings.stripe_api_base)
            )
        elif source == "salesforce":
            if not settings.salesforce_configured:
                logger.warning("Salesforce credentials missing - skipping.")
                continue
            extractors.append(
                SalesforceExtractor(
                    username=settings.salesforce_username,
                    password=settings.salesforce_password,
                    security_token=settings.salesforce_security_token,
                    login_url=settings.salesforce_login_url,
                )
            )
    return extractors


def run_extraction(
    extractor: BaseExtractor,
    lake: S3DataLake | LocalDataLake,
    run_id: str,
    resources: list[str] | None = None,
    since: datetime | None = None,
) -> dict[str, dict[str, int]]:
    """Extract every requested resource and land valid records in the lake."""
    summary: dict[str, dict[str, int]] = {}
    for resource in resources or sorted(extractor.resource_models):
        result = extractor.extract(resource, since=since)
        if result.valid_records:
            lake.write_batch(
                result.valid_records,
                source=extractor.source.value,
                resource=resource,
                run_id=run_id,
            )
        if result.errors:
            logger.error(
                "Quarantined %d invalid %s records (first: %s)",
                result.error_count,
                resource,
                result.errors[0]["errors"][0].get("msg"),
            )
        summary[resource] = {
            "fetched": result.total_fetched,
            "valid": result.valid_count,
            "errors": result.error_count,
        }
    return summary


def main() -> None:
    args = parse_args()
    settings = get_settings()
    setup_logging(settings.log_level)
    since = datetime.fromisoformat(args.since) if args.since else None
    run_id = new_run_id()
    logger.info("=== ETL extraction run %s (since=%s) ===", run_id, since)

    if args.local or not settings.s3_configured:
        lake: S3DataLake | LocalDataLake = LocalDataLake()
        logger.info("Using LOCAL data lake at data/raw (dev mode).")
    else:
        lake = S3DataLake(
            bucket=settings.aws_s3_bucket,
            region=settings.aws_region,
            access_key_id=settings.aws_access_key_id,
            secret_access_key=settings.aws_secret_access_key,
        )

    extractors = build_extractors(settings, args.sources)
    if not extractors:
        logger.error("No sources configured - check your .env credentials.")
        return

    for extractor in extractors:
        logger.info("--- Source: %s ---", extractor.source.value)
        summary = run_extraction(
            extractor, lake, run_id, resources=args.resources, since=since
        )
        for resource, counts in summary.items():
            logger.info("%s: %s", resource, counts)
        extractor.close()

    logger.info("=== Extraction run complete ===")


if __name__ == "__main__":
    main()
