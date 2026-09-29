"""Load stage runner - Week 3, Day 4-6.

Reads unified JSON parts from the processed zone and upserts them into
the warehouse. After each resource, the high-water mark (max created_at /
updated_at) is stored so the next extract run can pull incrementally.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.load.warehouse import WarehouseLoader, get_engine
from src.transform.clean import standardize_datetime
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def find_processed_parts(out_root: str | Path) -> list[Path]:
    """Discover unified part JSON files under the processed zone."""
    root = Path(out_root)
    if not root.exists():
        return []
    return sorted(p for p in root.rglob("part-*.json") if p.suffix == ".json")


def load_unified_records(path: str | Path) -> list[dict[str, Any]]:
    """Load one processed part file (list of unified record dicts)."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    return [payload] if isinstance(payload, dict) else []


def infer_source_resource(path: Path) -> tuple[str, str]:
    """Infer (source, resource) from processed-zone partition path parts."""
    source, resource = "unknown", path.parent.name
    for part in path.parts:
        if part.startswith("source="):
            source = part.split("=", 1)[1]
        if part.startswith("resource="):
            resource = part.split("=", 1)[1]
    return source, resource


def max_event_time(records: list[dict[str, Any]]) -> datetime | None:
    """Max(updated_at, created_at) across records - the new watermark."""
    best: datetime | None = None
    for record in records:
        for key in ("updated_at", "created_at"):
            parsed = standardize_datetime(record.get(key))
            if parsed is not None and (best is None or parsed > best):
                best = parsed
    return best
def run_load(out_root: str | Path = "data/processed",
             database_url: str = "sqlite:///data/warehouse.db",
             run_id: str | None = None) -> dict[str, dict[str, int]]:
    """Upsert every processed part into the warehouse; update watermarks."""
    engine = get_engine(database_url)
    loader = WarehouseLoader(engine)
    summary: dict[str, dict[str, int]] = {}
    for path in find_processed_parts(out_root):
        source, resource = infer_source_resource(path)
        key = f"{source}/{resource}"
        records = load_unified_records(path)
        stats = loader.upsert_records(records)
        watermark = max_event_time(records)
        if watermark is not None:
            loader.set_watermark(source, resource, watermark, run_id)
        current = summary.setdefault(
            key, {"input": 0, "inserted": 0, "updated": 0, "rejected": 0})
        for stat, value in stats.items():
            current[stat] = current.get(stat, 0) + value
        logger.info("%s: %s (watermark=%s)", key, stats, watermark)
    if not summary:
        logger.warning("No processed parts under %s - nothing to load.", out_root)
    return summary

