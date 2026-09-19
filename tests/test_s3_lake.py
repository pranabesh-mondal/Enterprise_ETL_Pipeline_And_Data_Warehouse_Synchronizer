"""Tests for the raw data lake key layout and local landing."""

from __future__ import annotations

import json

from src.storage.s3_lake import LocalDataLake, build_key, new_run_id


def test_build_key_layout():
    key = build_key("stripe", "customers", "2026-09-17T10-00-00Z", part=3)
    assert key == (
        "raw/stripe/customers/run_date=20260917/"
        "2026-09-17T10-00-00Z/part-00003.json"
    )


def test_local_lake_writes_json(tmp_path):
    lake = LocalDataLake(root=tmp_path)
    run_id = new_run_id()
    uris = lake.write_batch(
        [{"external_id": "cus_1"}, {"external_id": "cus_2"}],
        source="stripe",
        resource="customers",
        run_id=run_id,
    )
    assert len(uris) == 1
    with open(uris[0], encoding="utf-8") as fh:
        records = json.load(fh)
    assert records[0]["external_id"] == "cus_1"
