"""Independent expected-result assertions and local benchmark evidence."""

import hashlib
import platform
import sys
from pathlib import Path

from .common import ROOT, canonical, read_json, write_json
from .pipeline import OUTPUTS, query, revision, run
from .rules import evaluate
from .synthetic import generate


def check_fixtures():
    cases = read_json(ROOT / "synthetic/fixtures/rule_cases.v1.json")["cases"]
    for case in cases:
        result = evaluate(case["datasets"], case["cutoff"], case["report_start"])
        if "expected_quarantine_rule" in case:
            rules = {r["rule_id"] for errors in result["quarantined_incidents"].values() for r in errors}
            if result["incident_resolution"] or case["expected_quarantine_rule"] not in rules:
                raise AssertionError(case["name"] + ": incorrect quarantine")
        elif result["incident_resolution"] != [case["expected_incident"]]:
            raise AssertionError(case["name"] + ": expected " + canonical(case["expected_incident"]) + ", got " + canonical(result["incident_resolution"]))
        if "expected_daily" in case and result["service_daily_metrics"] != case["expected_daily"]:
            raise AssertionError(case["name"] + ": incorrect daily metrics")
        for field, disposition in (("expected_duplicate_count", "duplicate"), ("expected_deferred_count", "deferred")):
            if field in case and sum(d["disposition"] == disposition for d in result["dispositions"]) != case[field]:
                raise AssertionError(case["name"] + ": incorrect " + disposition + " count")
    return {"status": "passed", "fixtures": len(cases)}


def output_checksum(db):
    outputs = {name: query(db, "SELECT * FROM gold_" + name + " ORDER BY " + ("incident_id" if name == "incident_resolution" else "date,service_id,priority")) for name in OUTPUTS}
    return hashlib.sha256(canonical(outputs).encode()).hexdigest()


def benchmark(workspace, count=1000, repeats=3):
    if count < 1 or repeats < 1:
        raise ValueError("count and repeats must be positive")
    workspace = Path(workspace)
    if workspace.exists():
        raise ValueError("benchmark workspace exists; use a new path for a new experiment")
    generate(workspace / "source", count=count)
    check_fixtures()
    results = []
    # Separate databases for each repetition; OS disk caches are uncontrolled.
    for number in range(repeats):
        db = workspace / ("run-%03d.sqlite" % number)
        report = run(workspace / "source/initial", db)
        report["output_checksum"] = output_checksum(db)
        report["input_bytes_per_second"] = round(report["input_bytes"] / report["runtime_seconds"], 3)
        results.append(report)
    if len({r["output_checksum"] for r in results}) != 1:
        raise AssertionError("benchmark outputs differ across repetitions")
    criteria = read_json(ROOT / "benchmarks/acceptance.local.v1.json")
    evidence = {"scope": "local development baseline; not G5/G6/G7 cloud evidence",
                "transformation_revision": revision(), "python": sys.version, "platform": platform.platform(),
                "cache_policy": "fresh SQLite DB per repetition; OS caches uncontrolled; no warm-up exclusion",
                "workers": 1, "concurrency": 1, "incident_count": count,
                "acceptance_criteria": criteria, "runs": results,
                "runtime_min_seconds": min(r["runtime_seconds"] for r in results),
                "runtime_max_seconds": max(r["runtime_seconds"] for r in results),
                "smoke_runtime_passed": all(r["runtime_seconds"] <= criteria["smoke_runtime_seconds"] for r in results) if count == 1000 else None}
    write_json(workspace / "evidence.json", evidence)
    return evidence
