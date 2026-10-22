"""Read source batches and verify immutable manifests before processing."""

import csv
import json
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

from .common import DATASETS, ROOT, canonical, digest, read_json, safe_file, timestamp
from .rules import normalize


def iter_file(path, fmt, dataset):
    with Path(path).open(newline="", encoding="utf-8") as stream:
        if fmt == "csv":
            for row in csv.DictReader(stream):
                if dataset == "sla_targets":
                    for field in ("response_minutes", "resolution_minutes"):
                        row[field] = int(row[field])
                yield row
        elif fmt == "jsonl":
            for line in stream:
                yield json.loads(line)
        else:
            raise ValueError("unsupported format: " + fmt)


def validate_batch(root):
    root = Path(root)
    manifest = read_json(root / "manifest.json")
    contract = read_json(ROOT / "contracts/input.v1.json")
    if set(contract["manifest_required"]) - manifest.keys():
        raise ValueError("incomplete manifest")
    if manifest["batch_kind"] not in ("initial", "delta") or (manifest["batch_kind"] == "initial") != (manifest["parent_batch_id"] is None):
        raise ValueError("invalid batch kind/parent")
    if manifest["dataset_version"] != "v1" or manifest["rule_version"] != "v1":
        raise ValueError("unsupported contract/rule version")
    if timestamp(manifest["report_start"]) >= timestamp(manifest["cutoff"]):
        raise ValueError("invalid reporting interval")
    seen, counts, invalid_rows = set(), Counter(), 0
    for item in manifest["files"]:
        if set(contract["manifest_file_fields"]) - item.keys():
            raise ValueError("incomplete file metadata")
        if item["dataset"] not in DATASETS or item["path"] in seen:
            raise ValueError("unknown dataset or repeated file path")
        seen.add(item["path"])
        path = safe_file(root, item["path"])
        if path.stat().st_size != item["bytes"] or digest(path) != item["sha256"]:
            raise ValueError("checksum/size mismatch: " + item["path"])
        count = 0
        for row in iter_file(path, item["format"], item["dataset"]):
            try:
                normalize(item["dataset"], row)
            except (ValueError, TypeError):
                # Incident record failures are preserved for quarantine. Reference
                # failures invalidate the batch; no synthetic oracle is consulted.
                if item["dataset"] not in ("incident_snapshots", "incident_events"):
                    raise
                invalid_rows += 1
            count += 1
        if count != item["rows"]:
            raise ValueError("row-count mismatch: " + item["path"])
        counts[item["dataset"]] += count
    if set(counts) != set(DATASETS):
        raise ValueError("all six source datasets are required")
    return {"batch_id": manifest["batch_id"], "manifest_sha256": digest(root / "manifest.json"),
            "rows": dict(counts), "schema_fault_rows": invalid_rows}


def read_api(base_url, batch_id, expected, landing=None):
    from urllib.parse import quote, urlencode
    if urlparse(base_url).scheme != "http" or urlparse(base_url).hostname not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("mock API must be local HTTP")
    cursor, seen, records, page_number = "", set(), [], 0
    while True:
        query = urlencode({"cursor": cursor, "limit": 100})
        url = base_url.rstrip("/") + "/v1/batches/" + quote(batch_id, safe="") + "/incident-events?" + query
        with urlopen(url, timeout=10) as response:
            payload = response.read()
        page = json.loads(payload)
        if landing:
            landing.mkdir(parents=True, exist_ok=True)
            (landing / ("page-%06d.json" % page_number)).write_bytes(payload)
        page_number += 1
        if page["batch_id"] != batch_id or page["manifest_sha256"] != expected:
            raise ValueError("mock API batch identity mismatch")
        records.extend(page["records"])
        cursor = page["next_cursor"]
        if cursor is None:
            return records
        if cursor in seen:
            raise ValueError("mock API pagination loop")
        seen.add(cursor)


def load_batch(root, api_url=None, api_landing=None):
    validation = validate_batch(root)
    manifest = read_json(Path(root) / "manifest.json")
    datasets = {key: [] for key in DATASETS}
    for item in manifest["files"]:
        datasets[item["dataset"]].extend(iter_file(safe_file(root, item["path"]), item["format"], item["dataset"]))
    if api_url:
        records = read_api(api_url, manifest["batch_id"], validation["manifest_sha256"], api_landing)
        # The API represents the immutable manifest, not an uncontrolled live feed.
        if canonical(records) != canonical(datasets["incident_events"]):
            raise ValueError("mock API contents differ from source manifest")
        datasets["incident_events"] = records
    return manifest, datasets, validation
