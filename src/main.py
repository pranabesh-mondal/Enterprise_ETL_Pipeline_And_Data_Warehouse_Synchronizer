"""Week 1-2 pipeline entrypoint: Extract -> validate -> land raw JSON ->
transform into the unified schema.

Usage:
    python -m src.main                          # extraction (all configured sources)
    python -m src.main --local                  # land to ./data/raw instead of S3
    python -m src.main --stage transform        # transform raw -> unified records
    python -m src.main --stage all --local      # extract then transform
    python -m src.main --resources customers    # only Stripe customers
    python -m src.main --since 2026-09-10T00:00:00+00:00
"""

from __future__ import annotations

import argparse
from datetime import datetime

from src.config.settings import get_settings
from src.extract.base import BaseExtractor
from src.extract.salesforce import SalesforceExtractor
from src.extract.stripe import StripeExtractor
from src.storage.s3_lake import LocalDataLake, S3DataLake, new_run_id
from src.transform.runner import run_transform
from src.utils.logging_config import get_logger, setup_logging

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enterprise ETL pipeline run")
    parser.add_argument(
        "--stage",
        choices=["extract", "transform", "all"],
        default="extract",
        help="Pipeline stage to run (default: extract)",
    )
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
    parser.add_argument(
        "--raw-root",
        default="data/raw",
        help="Raw lake root read by the transform stage (default: data/raw)",
    )
    parser.add_argument(
        "--out-root",
        default="data/processed",
        help="Processed zone root written by the transform stage",
    )
    parser.add_argument(
        "--engine",
        choices=["polars", "pandas"],
        default="polars",
        help="DataFrame engine used for the transform stage",
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
                StripeExtractor(
                    settings.stripe_api_key,
                    settings.stripe_api_base,
                    page_size=settings.extraction_page_size,
                )
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
    """Extract every requested resource and land valid records in the lake.

    Failures are isolated per resource: a broken endpoint is logged and
    skipped so the remaining resources (and sources) still complete.
    """
    summary: dict[str, dict[str, int]] = {}
    for resource in resources or sorted(extractor.resource_models):
        if resource not in extractor.resource_models:
            logger.warning(
                "Resource '%s' is not supported by source '%s' - skipping. Known: %s",
                resource,
                extractor.source.value,
                sorted(extractor.resource_models),
            )
            continue

        try:
            result = extractor.extract(resource, since=since)
        except Exception as exc:  # noqa: BLE001 - deliberate per-resource isolation
            logger.exception(
                "Extraction FAILED for %s/%s: %s",
                extractor.source.value,
                resource,
                exc,
            )
            summary[resource] = {"fetched": 0, "valid": 0, "errors": 0, "failed": 1}
            continue

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
            "failed": 0,
        }
    return summary


def run_extract_stage(args: argparse.Namespace, settings, run_id: str) -> None:
    """Extract every requested source/resource and land raw JSON."""
    since = datetime.fromisoformat(args.since) if args.since else None
    extractors = build_extractors(settings, args.sources)
    if not extractors:
        logger.error("No sources configured - check your .env credentials.")
        return

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

    for extractor in extractors:
        logger.info("--- Extract source: %s ---", extractor.source.value)
        summary = run_extraction(
            extractor, lake, run_id, resources=args.resources, since=since
        )
        for resource, counts in summary.items():
            logger.info("%s: %s", resource, counts)
        extractor.close()


def run_transform_stage(args: argparse.Namespace) -> None:
    """Transform raw parts into unified records in the processed zone."""
    logger.info("--- Transform stage (engine=%s) ---", args.engine)
    summary = run_transform(
        raw_root=args.raw_root,
        out_root=args.out_root,
        engine=args.engine,
        sources=args.sources,
    )
    for resource, stats in summary.items():
        logger.info("%s: %s", resource, stats)


def main() -> None:
    args = parse_args()
    settings = get_settings()
    setup_logging(settings.log_level)
    run_id = new_run_id()
    logger.info("=== ETL run %s | stage=%s ===", run_id, args.stage)

    if args.stage in ("extract", "all"):
        run_extract_stage(args, settings, run_id)
    if args.stage in ("transform", "all"):
        run_transform_stage(args)

    logger.info("=== ETL run complete (stage=%s) ===", args.stage)


if __name__ == "__main__":
    main()
