"""Behavioral tests for source integrity, business rules, publication, and recovery."""

import copy
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen
from unittest.mock import patch

from dpe.api import make_server
from dpe.common import ROOT, canonical, digest, read_json, write_json
from dpe.ingestion import load_batch, validate_batch
from dpe.pipeline import FAILPOINTS, OUTPUTS, query, run
from dpe.rules import evaluate
from dpe.synthetic import generate
from dpe.validation import check_fixtures, output_checksum


class RuleTests(unittest.TestCase):
    def test_independent_expected_results(self):
        self.assertEqual(check_fixtures(), {"fixtures": 51, "status": "passed"})

    def test_historical_backlog_moves_with_assignment(self):
        case = copy.deepcopy(read_json(ROOT / "synthetic/fixtures/rule_cases.v1.json")["cases"][0])
        case["datasets"]["incident_events"] = []
        initial = case["datasets"]["incident_snapshots"][0]
        case["datasets"]["incident_snapshots"].append(dict(initial, source_version=2,
                                                           source_updated_at="2026-01-02T12:00:00Z", service_id="S2"))
        result = evaluate(case["datasets"], "2026-01-04T00:00:00Z", case["report_start"])
        metrics = result["service_daily_metrics"]
        self.assertEqual([(r["date"], r["service_id"], r["backlog_count"]) for r in metrics],
                         [("2026-01-01", "S1", 1), ("2026-01-02", "S2", 1), ("2026-01-03", "S2", 1)])
        self.assertTrue(all(r["resolution_pass_rate"] is None for r in metrics))

    def test_creation_date_correction_replaces_old_daily_count(self):
        case = copy.deepcopy(read_json(ROOT / "synthetic/fixtures/rule_cases.v1.json")["cases"][0])
        initial = case["datasets"]["incident_snapshots"][0]
        case["datasets"]["incident_snapshots"].append(dict(initial, source_version=2,
                                                           source_updated_at="2026-01-01T02:00:00Z", status="resolved",
                                                           created_at="2025-12-31T23:30:00Z"))
        result = evaluate(case["datasets"], "2026-01-03T00:00:00Z", "2025-12-31T00:00:00Z")
        self.assertEqual(result["incident_resolution"][0]["resolution_minutes"], 90)
        self.assertEqual([(r["date"], r["created_count"]) for r in result["service_daily_metrics"]],
                         [("2025-12-31", 1), ("2026-01-01", 0)])


class PlatformTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        generate(self.source, count=40)
        self.initial, self.delta = self.source / "initial", self.source / "delta"
        self.db = self.root / "platform.sqlite"

    def tearDown(self):
        self.temp.cleanup()

    def test_generation_is_byte_deterministic_and_immutable(self):
        other = self.root / "other"
        generate(other, count=40)
        for batch in ("initial", "delta"):
            self.assertEqual((self.source / batch / "manifest.json").read_bytes(), (other / batch / "manifest.json").read_bytes())
            for item in read_json(self.source / batch / "manifest.json")["files"]:
                self.assertEqual(digest(self.source / batch / item["path"]), digest(other / batch / item["path"]))
        with self.assertRaisesRegex(ValueError, "destination exists"):
            generate(self.source)

    def test_checksums_and_manifest_row_counts_are_enforced(self):
        manifest = read_json(self.initial / "manifest.json")
        item = manifest["files"][0]
        path = self.initial / item["path"]
        original = path.read_bytes()
        path.write_bytes(original + b"\n")
        with self.assertRaisesRegex(ValueError, "checksum/size"):
            validate_batch(self.initial)
        path.write_bytes(original)
        item["rows"] += 1
        write_json(self.initial / "manifest.json", manifest)
        with self.assertRaisesRegex(ValueError, "row-count"):
            validate_batch(self.initial)

    def test_manifest_cannot_escape_its_root(self):
        manifest = read_json(self.initial / "manifest.json")
        manifest["files"][0]["path"] = "../outside.csv"
        write_json(self.initial / "manifest.json", manifest)
        with self.assertRaisesRegex(ValueError, "escapes"):
            validate_batch(self.initial)

    def test_chunked_sources_and_pagination_have_no_gaps(self):
        config = read_json(ROOT / "synthetic/configs/default.json")
        config["chunk_rows"] = 7
        path = self.root / "config.json"
        write_json(path, config)
        source = self.root / "chunked"
        generate(source, count=40, config_path=path)
        batch = source / "initial"
        self.assertTrue(all(f["rows"] <= 7 for f in read_json(batch / "manifest.json")["files"]))
        server = make_server(batch, port=0)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        try:
            url = "http://127.0.0.1:" + str(server.server_port)
            _, expected, _ = load_batch(batch)
            _, actual, _ = load_batch(batch, url)
            self.assertEqual(expected, actual)
            report = run(batch, self.db, api_url=url)
            self.assertEqual(report["output_rows"]["incident_resolution"], 40)
            self.assertTrue(list(Path(report["api_landing"]).glob("page-*.json")))
            bid = read_json(batch / "manifest.json")["batch_id"]
            with self.assertRaises(HTTPError) as error:
                urlopen(url + "/v1/batches/" + bid + "/incident-events?limit=0")
            self.assertEqual(error.exception.code, 400)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_commit_replay_and_delta_equal_full_recomputation(self):
        initial = run(self.initial, self.db)
        before = output_checksum(self.db)
        replay = run(self.initial, self.db)
        self.assertEqual(replay["status"], "already_committed")
        self.assertEqual(initial["publication_id"], replay["publication_id"])
        self.assertEqual(before, output_checksum(self.db))
        delta = run(self.delta, self.db)
        self.assertEqual(delta["output_rows"]["incident_resolution"], 44)
        manifest, base, _ = load_batch(self.initial)
        manifest, updates, _ = load_batch(self.delta)
        combined = {key: base[key] + updates[key] for key in base}
        expected = evaluate(combined, manifest["cutoff"], manifest["report_start"])
        for name in OUTPUTS:
            order = "incident_id" if name == "incident_resolution" else "date,service_id,priority"
            actual = query(self.db, "SELECT * FROM gold_" + name + " ORDER BY " + order)
            for row in actual:
                if "quality_flags" in row:
                    row["quality_flags"] = json.loads(row["quality_flags"])
            self.assertEqual(actual, expected[name])
        after = output_checksum(self.db)
        run(self.initial, self.db)  # Replaying old work must not move the active pointer back.
        self.assertEqual(after, output_checksum(self.db))
        with sqlite3.connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM batches").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT count(*) FROM publications").fetchone()[0], 2)
            disposition = json.loads(connection.execute("SELECT payload FROM record_dispositions LIMIT 1").fetchone()[0])
            self.assertIn("source_file", disposition)
            self.assertIn("input_batch_id", disposition)

    def test_delta_requires_exact_parent(self):
        with self.assertRaisesRegex(ValueError, "initial batch"):
            run(self.delta, self.db)
        run(self.initial, self.db)
        manifest = read_json(self.delta / "manifest.json")
        manifest["parent_batch_id"] = "unknown-parent"
        write_json(self.delta / "manifest.json", manifest)
        with self.assertRaisesRegex(ValueError, "active parent"):
            run(self.delta, self.db)

    def test_committed_batch_manifest_is_immutable(self):
        run(self.initial, self.db)
        manifest = read_json(self.initial / "manifest.json")
        manifest["seed"] += 1
        write_json(self.initial / "manifest.json", manifest)
        with self.assertRaisesRegex(ValueError, "identity changed"):
            run(self.initial, self.db)

    def test_delta_identity_includes_parent_manifest_lineage(self):
        run(self.initial, self.db)
        first = run(self.delta, self.db)
        manifest = read_json(self.initial / "manifest.json")
        manifest["seed"] += 1
        write_json(self.initial / "manifest.json", manifest)
        other_db = self.root / "different-lineage.sqlite"
        run(self.initial, other_db)
        second = run(self.delta, other_db)
        self.assertNotEqual(first["publication_id"], second["publication_id"])
        self.assertNotEqual(first["parent_publication_id"], second["parent_publication_id"])

    def test_every_failpoint_preserves_publication_and_can_recover(self):
        criteria = read_json(ROOT / "benchmarks/acceptance.local.v1.json")
        for point in FAILPOINTS:
            with self.subTest(point=point):
                db = self.root / (point + ".sqlite")
                run(self.initial, db)
                checksum = output_checksum(db)
                with self.assertRaisesRegex(RuntimeError, point):
                    run(self.delta, db, fail_at=point)
                self.assertEqual(output_checksum(db), checksum)
                with sqlite3.connect(db) as connection:
                    self.assertEqual(connection.execute("SELECT count(*) FROM batches").fetchone()[0], 1)
                    self.assertEqual(connection.execute("SELECT count(*) FROM publications").fetchone()[0], 1)
                    self.assertEqual(connection.execute("SELECT status FROM attempts ORDER BY rowid DESC LIMIT 1").fetchone()[0], "failed")
                started = time.perf_counter()
                self.assertEqual(run(self.delta, db)["status"], "committed")
                self.assertLess(time.perf_counter() - started, criteria["recovery_seconds"])

    def test_first_batch_failure_exposes_no_outputs(self):
        with self.assertRaises(RuntimeError):
            run(self.initial, self.db, fail_at="between_gold_writes")
        self.assertEqual(query(self.db, "SELECT count(*) n FROM gold_incident_resolution"), [{"n": 0}])
        self.assertEqual(run(self.initial, self.db)["status"], "committed")

    def test_interrupted_raw_archive_can_be_retried(self):
        with patch("dpe.pipeline.shutil.copyfile", side_effect=OSError("simulated storage write failure")):
            with self.assertRaisesRegex(OSError, "storage write failure"):
                run(self.initial, self.db)
        self.assertEqual(query(self.db, "SELECT count(*) n FROM batches"), [{"n": 0}])
        self.assertFalse(list((self.root / "platform-raw").glob("landing-*")))
        self.assertEqual(run(self.initial, self.db)["status"], "committed")

    def test_fault_mode_reconciles_declared_quarantines_and_duplicates(self):
        source = self.root / "faults"
        generate(source, count=1000, mode="faults")
        checked = validate_batch(source / "initial")
        self.assertGreater(checked["declared_schema_fault_rows"], 0)
        manifest = read_json(source / "initial/manifest.json")
        self.assertEqual(len({d["incident_id"] for d in manifest["defects"]}), len(manifest["defects"]))
        self.assertEqual({d["fault"] for d in manifest["defects"]}, {"duplicate", "missing_service", "invalid_priority", "invalid_timestamp", "conflicting_version"})
        report = run(source / "initial", self.db)
        expected = {d["incident_id"] for d in manifest["defects"] if d["expected_disposition"] == "quarantined"}
        self.assertEqual(set(report["quarantined_incidents"]), expected)
        for defect in manifest["defects"]:
            if defect["incident_id"] in expected:
                self.assertIn(defect["rule_id"], {r["rule_id"] for r in report["quarantined_incidents"][defect["incident_id"]]})
        self.assertEqual(report["output_rows"]["incident_resolution"], 1000 - len(expected))
        self.assertEqual(run(source / "delta", self.db)["status"], "committed")

    def test_unexplained_quality_failure_cannot_publish(self):
        manifest = read_json(self.initial / "manifest.json")
        item = next(f for f in manifest["files"] if f["dataset"] == "incident_events")
        path = self.initial / item["path"]
        lines = path.read_text().splitlines()
        record = json.loads(lines[0])
        record["event_type"] = "close"  # Valid source enum, impossible initial transition.
        lines[0] = canonical(record)
        path.write_text("\n".join(lines) + "\n")
        item.update(sha256=digest(path), bytes=path.stat().st_size)
        write_json(self.initial / "manifest.json", manifest)
        with self.assertRaisesRegex(ValueError, "quarantine differs"):
            run(self.initial, self.db)
        self.assertEqual(query(self.db, "SELECT count(*) n FROM publications"), [{"n": 0}])

    def test_consumer_query_cannot_write(self):
        run(self.initial, self.db)
        with self.assertRaises(sqlite3.OperationalError):
            query(self.db, "DELETE FROM batches")


if __name__ == "__main__":
    unittest.main()
