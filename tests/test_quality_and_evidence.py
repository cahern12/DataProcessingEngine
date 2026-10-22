import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dpe.common import ROOT, canonical, digest, read_json, write_json
from dpe.pipeline import query, run
from dpe.synthetic import generate
from dpe.validation import benchmark, check_synthetic


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        generate(self.root / "source", count=40)
        self.batch = self.root / "source/initial"
        self.db = self.root / "platform.sqlite"

    def tearDown(self):
        self.temp.cleanup()

    def test_missing_manifest_is_a_persisted_failed_attempt(self):
        with self.assertRaises(FileNotFoundError):
            run(self.root / "missing", self.db)
        row = query(self.db, "SELECT status,evidence FROM attempts")[0]
        report = json.loads(row["evidence"])
        self.assertEqual(row["status"], "failed")
        self.assertIsNone(report["batch_id"])
        self.assertEqual(report["stage"], "ingestion")
        self.assertTrue(Path(report["evidence_file"]).exists())

    def test_checksum_failure_records_available_identity(self):
        manifest = read_json(self.batch / "manifest.json")
        path = self.batch / manifest["files"][0]["path"]
        path.write_text(path.read_text() + "\n")
        with self.assertRaisesRegex(ValueError, "checksum"):
            run(self.batch, self.db)
        report = json.loads(query(self.db, "SELECT evidence FROM attempts")[0]["evidence"])
        self.assertEqual(report["batch_id"], manifest["batch_id"])
        self.assertEqual(report["status"], "failed")

    def test_api_failure_is_recorded(self):
        with patch("dpe.ingestion.urlopen", side_effect=OSError("mock source unavailable")):
            with self.assertRaisesRegex(OSError, "source unavailable"):
                run(self.batch, self.db, api_url="http://127.0.0.1:8765")
        report = json.loads(query(self.db, "SELECT evidence FROM attempts")[0]["evidence"])
        self.assertEqual(report["stage"], "ingestion")
        self.assertEqual(report["status"], "failed")

    def test_metadata_failure_has_fallback_evidence(self):
        blocked = self.root / "not-a-directory"
        blocked.write_text("block parent directory creation")
        with patch("dpe.pipeline.ROOT", self.root):
            with self.assertRaises(OSError):
                run(self.batch, blocked / "platform.sqlite")
        reports = list((self.root / "data/evidence").glob("*.json"))
        self.assertEqual(len(reports), 1)
        report = read_json(reports[0])
        self.assertEqual(report["stage"], "metadata_initialization")
        self.assertEqual(report["status"], "failed")

    def test_quality_policy_does_not_read_synthetic_expectations(self):
        # A previously unknown semantic defect is quarantined under policy,
        # without an expected-result sidecar or manifest defect declarations.
        (self.batch / "expected.v1.json").unlink()
        manifest = read_json(self.batch / "manifest.json")
        self.assertNotIn("defects", manifest)
        item = next(f for f in manifest["files"] if f["dataset"] == "incident_events")
        path = self.batch / item["path"]
        lines = path.read_text().splitlines()
        row = json.loads(lines[0]); row["event_type"] = "close"
        lines[0] = canonical(row)
        path.write_text("\n".join(lines) + "\n")
        item.update(sha256=digest(path), bytes=path.stat().st_size)
        write_json(self.batch / "manifest.json", manifest)
        report = run(self.batch, self.db)
        self.assertEqual(report["status"], "committed")
        self.assertEqual(report["quality"]["quarantined_incidents"], 1)
        self.assertEqual(report["output_rows"]["incident_resolution"], 39)

    def test_synthetic_sidecar_is_an_independent_assertion(self):
        generate(self.root / "faults", count=1000, mode="faults")
        batch = self.root / "faults/initial"
        report = run(batch, self.db)
        self.assertEqual(check_synthetic(batch, report, self.db)["status"], "passed")
        sidecar = read_json(batch / "expected.v1.json")
        sidecar["defects"] = []
        write_json(batch / "expected.v1.json", sidecar)
        with self.assertRaisesRegex(AssertionError, "quarantine mismatch"):
            check_synthetic(batch, report, self.db)

    def test_runtime_failure_preserves_frozen_acceptance_and_evidence(self):
        criteria = read_json(ROOT / "benchmarks/acceptance.local.v1.json")
        frozen = json.loads(json.dumps(criteria))
        def fake_run(*args):
            criteria["smoke_runtime_seconds"] = 99999
            return {"runtime_seconds": frozen["smoke_runtime_seconds"] + 1, "input_bytes": 100}
        with patch("dpe.validation.generate"), patch("dpe.validation.check_fixtures"), \
                patch("dpe.validation.read_json", return_value=frozen), \
                patch("dpe.validation.run", side_effect=fake_run), \
                patch("dpe.validation.output_checksum", return_value="identical"):
            with self.assertRaisesRegex(ValueError, "benchmark acceptance failed"):
                benchmark(self.root / "benchmark", repeats=2)
        report = read_json(self.root / "benchmark/evidence.json")
        self.assertEqual(report["acceptance_criteria"]["smoke_runtime_seconds"], 60)
        self.assertEqual(report["criteria_results"]["smoke_runtime"], "failed")
        self.assertFalse(report["smoke_runtime_passed"])


if __name__ == "__main__":
    unittest.main()
