from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from src.models.base import Source
from src.models.unified import ENTITY_MODELS
from src.transform.mappings import MAPPINGS
from src.transform.pipeline import TransformResult, transform_records
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def find_raw_parts(
    raw_root: str | Path,
    source: Source | None = None,
) -> list[tuple[Source, str, Path]]:
    """Discover raw part files: (source, resource, path) triples."""
    root = Path(raw_root)
    parts: list[tuple[Source, str, Path]] = []
    if not root.exists():
        return parts
    for source_dir in sorted(root.iterdir()):
        if not source_dir.is_dir():
            continue
        try:
            source_value = Source(source_dir.name)
        except ValueError:
            continue
        if source is not None and source_value is not source:
            continue
        for resource_dir in sorted(source_dir.iterdir()):
            if not resource_dir.is_dir():
                continue
            for path in sorted(resource_dir.rglob("*.json")):
                parts.append((source_value, resource_dir.name, path))
    return parts


def load_raw_records(path: str | Path) -> list[Any]:
    """Load one raw JSON part (a list of record objects)."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    return [payload]


def write_unified(
    result: TransformResult,
    out_root: str | Path = "data/processed",
    *,
    write_parquet: bool = True,
) -> list[str]:
    """Persist unified records for one source resource; returns written paths."""
    if not result.records:
        return []

    entity = ENTITY_MODELS[
        MAPPINGS[(result.source, result.resource)].entity_type
    ].__name__
    entity_dir = entity.removeprefix("Unified").lower()
    target = (
        Path(out_root)
        / entity_dir
        / f"source={result.source.value}"
        / f"resource={result.resource}"
    )
    target.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    json_path = target / "part-00000.json"
    json_path.write_text(json.dumps(result.records, default=str), encoding="utf-8")
    written.append(str(json_path))

    if write_parquet:
        parquet_path = target / "part-00000.parquet"
        frame = pl.DataFrame(result.records)
        if "attributes" in frame.columns:
            frame = frame.with_columns(
                pl.col("attributes")
                .map_elements(json.dumps, return_dtype=pl.Utf8)
                .alias("attributes_json")
            ).drop("attributes")
        frame.write_parquet(parquet_path)
        written.append(str(parquet_path))

    logger.info(
        "Wrote %d unified %s records -> %s",
        len(result.records), entity, ", ".join(written),
    )
    return written


def run_transform(
    raw_root: str | Path = "data/raw",
    out_root: str | Path = "data/processed",
    *,
    engine: str = "polars",
    sources: list[str] | None = None,
) -> dict[str, dict[str, int]]:
    """Transform every discovered raw part and write the processed zone.

    Raw parts for the same (source, resource) are accumulated first and
    transformed/written once, so multi-part extractions never overwrite
    each other and cross-part duplicates are removed.
    """
    selected = {Source(value) for value in sources} if sources else None
    summary: dict[str, dict[str, int]] = {}
    batched: dict[str, tuple[Source, str, list[Any]]] = {}

    for selected_source in sorted(selected) if selected else [None]:
        for source, resource, path in find_raw_parts(raw_root, source=selected_source):
            key = f"{source.value}/{resource}"
            entry = batched.setdefault(key, (source, resource, []))
            entry[2].extend(load_raw_records(path))

    for key, (source, resource, records) in batched.items():
        result = transform_records(records, source, resource, engine=engine)
        write_unified(result, out_root)
        current = summary.setdefault(
            key, {"input": 0, "mapped": 0, "rejected": 0, "duplicates_removed": 0}
        )
        for stat, value in result.stats.items():
            current[stat] = current.get(stat, 0) + value

    if not summary:
        logger.warning("No raw parts found under %s - nothing to transform.", raw_root)
    return summary