"""Transactional local Bronze/Silver/Gold harness; no cloud/engine claims."""

import hashlib
import json
import platform
import resource
import shutil
import sqlite3
import time
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .common import DATASETS, ROOT, canonical, digest, iso, timestamp, validate_schema, read_json, write_json
from .ingestion import load_batch, validate_batch
from .quality import enforce_quality, load_policy
from .rules import SCHEMAS, evaluate

OUTPUTS = ("incident_resolution", "service_daily_metrics")
FAILPOINTS = ("after_ingestion", "after_silver", "between_gold_writes", "before_publish")


def revision():
    hasher = hashlib.sha256()
    for folder, glob in (("dpe", "*.py"), ("rules", "*"), ("contracts", "**/*.json")):
        for path in sorted((ROOT / folder).glob(glob)):
            if path.is_file():
                hasher.update(str(path.relative_to(ROOT)).encode())
                hasher.update(path.read_bytes())
    return hasher.hexdigest()


def initialize(connection):
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS batches (
          batch_id TEXT PRIMARY KEY, manifest_sha256 TEXT NOT NULL,
          manifest TEXT NOT NULL, revision TEXT NOT NULL, publication_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS bronze_records (
          batch_id TEXT, dataset TEXT, ordinal INTEGER, source_file TEXT,
          source_row INTEGER, ingested_at TEXT, payload TEXT NOT NULL,
          PRIMARY KEY(batch_id,dataset,ordinal));
        CREATE TABLE IF NOT EXISTS silver_incidents (
          publication_id TEXT, incident_id TEXT, payload TEXT,
          PRIMARY KEY(publication_id,incident_id));
        CREATE TABLE IF NOT EXISTS record_dispositions (
          publication_id TEXT, dataset TEXT, ordinal INTEGER, payload TEXT,
          PRIMARY KEY(publication_id,dataset,ordinal));
        CREATE TABLE IF NOT EXISTS publications (
          publication_id TEXT PRIMARY KEY, batch_id TEXT, cutoff TEXT, report TEXT);
        CREATE TABLE IF NOT EXISTS active_publication (
          singleton INTEGER PRIMARY KEY CHECK(singleton=1), publication_id TEXT);
        CREATE TABLE IF NOT EXISTS attempts (
          attempt_id TEXT PRIMARY KEY, batch_id TEXT, status TEXT, evidence TEXT);
    """)
    for name in OUTPUTS:
        fields = []
        for key, prop in SCHEMAS[name]["properties"].items():
            kinds = prop["type"]
            kinds = kinds if isinstance(kinds, list) else [kinds]
            sqltype = "INTEGER" if "integer" in kinds else "REAL" if "number" in kinds else "TEXT"
            fields.append('"' + key + '" ' + sqltype)
        grain = "incident_id" if name == "incident_resolution" else "date,service_id,priority"
        connection.execute("CREATE TABLE IF NOT EXISTS output_" + name + " (publication_id TEXT," + ",".join(fields) + ", PRIMARY KEY(publication_id," + grain + "))")
        columns = ",".join('g."' + key + '"' for key in SCHEMAS[name]["properties"])
        connection.execute("CREATE VIEW IF NOT EXISTS gold_" + name + " AS SELECT " + columns + " FROM output_" + name + " g JOIN active_publication a ON a.publication_id=g.publication_id")
    connection.commit()


def run(batch, db, api_url=None, fail_at=None, policy_path=None):
    if fail_at is not None and fail_at not in FAILPOINTS:
        raise ValueError("unknown failpoint")
    started, clock = iso(datetime.now(timezone.utc)), time.perf_counter()
    cpu_clock = time.process_time()
    attempt = str(uuid.uuid4())
    db = Path(db)
    api_landing = db.parent / (db.stem + "-api-landing") / attempt if api_url else None
    connection = None
    report = {"run_id": None, "attempt_id": attempt, "batch_id": None,
              "source_location": str(Path(batch).resolve()), "stage": "metadata_initialization", "stages": {},
              "dataset_version": None, "generator_seed": None, "rule_version": None,
              "transformation_revision": None,
              "engine": "python-reference+sqlite", "engine_version": "Python " + platform.python_version() + "; SQLite " + sqlite3.sqlite_version,
              "input_manifest_checksum": None, "cutoff": None,
              "started_at": started, "api_landing": str(api_landing) if api_landing else None,
              "cost": {"marginal_cloud_cost": None, "fully_allocated_cloud_cost": None,
                                               "reason": "local development; cloud costs not measured"}}
    evidence_path = db.parent / (db.stem + "-evidence") / (attempt + ".json")
    fallback_path = ROOT / "data/evidence" / (attempt + ".json")
    stage_clock = clock

    def stage(name):
        nonlocal stage_clock
        report["stages"][report["stage"]] = round(time.perf_counter() - stage_clock, 6)
        report["stage"] = name
        stage_clock = time.perf_counter()

    def persist_evidence():
        report["evidence_file"] = str(evidence_path)
        try:
            write_json(evidence_path, report)
        except OSError:
            report["evidence_file"] = str(fallback_path)
            write_json(fallback_path, report)

    def inject(point):
        if fail_at == point:
            raise RuntimeError("injected failure: " + point)

    try:
        # A file survives database initialization failures; the attempt row is
        # committed before any source validation or API request.
        report["status"] = "running"
        persist_evidence()
        db.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(db), timeout=30)
        initialize(connection)
        connection.execute("INSERT INTO attempts VALUES(?,?,?,?)", (attempt, None, "running", canonical(report)))
        connection.commit()
        stage("ingestion")
        path = Path(batch) / "manifest.json"
        if path.is_file():
            report["input_manifest_checksum"] = digest(path)
            try:
                header = read_json(path)
                report.update(batch_id=header.get("batch_id"), cutoff=header.get("cutoff"),
                              dataset_version=header.get("dataset_version"), rule_version=header.get("rule_version"),
                              generator_seed=header.get("seed"))
            except (ValueError, AttributeError):
                pass  # load_batch records the actual validation failure below.
        manifest, incoming, checked = load_batch(Path(batch), api_url, api_landing)
        policy = load_policy(policy_path)
        policy_hash = hashlib.sha256(canonical(policy).encode()).hexdigest()
        transform = revision()
        batch_id = manifest["batch_id"]
        run_id = hashlib.sha256(canonical([batch_id, checked["manifest_sha256"], transform, policy_hash]).encode()).hexdigest()
        report.update(run_id=run_id, transformation_revision=transform, quality_policy=policy,
                      quality_policy_sha256=policy_hash)
        connection.execute("UPDATE attempts SET batch_id=?,evidence=? WHERE attempt_id=?", (batch_id, canonical(report), attempt))
        connection.commit()
        stage("bronze")
        connection.execute("BEGIN IMMEDIATE")
        previous = connection.execute("SELECT manifest_sha256,revision,publication_id FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if previous:
            if previous[:2] != (checked["manifest_sha256"], transform):
                raise ValueError("committed batch identity changed; use a fresh workspace for a new transformation/input version")
            old_policy = json.loads(connection.execute("SELECT report FROM publications WHERE publication_id=?", (previous[2],)).fetchone()[0]).get("quality_policy_sha256")
            if old_policy != policy_hash:
                raise ValueError("committed quality policy changed; use a fresh workspace")
            report.update(status="already_committed", run_id=previous[2], publication_id=previous[2])
        else:
            active = connection.execute("SELECT p.batch_id,p.cutoff,p.publication_id FROM active_publication a JOIN publications p ON p.publication_id=a.publication_id").fetchone()
            if active:
                if manifest["parent_batch_id"] != active[0] or timestamp(manifest["cutoff"]) < timestamp(active[1]):
                    raise ValueError("delta must follow the active parent with a nondecreasing cutoff")
                initial = json.loads(connection.execute("SELECT manifest FROM batches ORDER BY rowid LIMIT 1").fetchone()[0])
                if manifest["report_start"] != initial["report_start"] or manifest["dataset_version"] != initial["dataset_version"]:
                    raise ValueError("report interval/contracts changed across batch chain")
            elif manifest["batch_kind"] != "initial":
                raise ValueError("process initial batch before its delta")
            # A delta result depends on the complete committed input lineage,
            # not just the current manifest and its logical parent batch name.
            run_id = hashlib.sha256(canonical([run_id, active[2] if active else None]).encode()).hexdigest()
            report["run_id"] = run_id
            report["parent_publication_id"] = active[2] if active else None
            # Preserve exact input bytes separately from normalized Bronze records.
            # Failed attempts may leave a valid archive, safe for a later retry.
            archive = db.parent / (db.stem + "-raw") / hashlib.sha256(batch_id.encode()).hexdigest()
            if archive.exists():
                archived = validate_batch(archive)
                if archived["manifest_sha256"] != checked["manifest_sha256"]:
                    raise ValueError("raw archive identity mismatch")
            else:
                archive.parent.mkdir(parents=True, exist_ok=True)
                from .common import safe_file
                staging = Path(tempfile.mkdtemp(prefix="landing-", dir=str(archive.parent)))
                try:
                    for file in manifest["files"]:
                        target = safe_file(staging, file["path"])
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(safe_file(batch, file["path"]), target)
                    shutil.copyfile(Path(batch) / "manifest.json", staging / "manifest.json")
                    validate_batch(staging)
                    staging.rename(archive)
                finally:
                    if staging.exists():
                        shutil.rmtree(staging)
            connection.execute("INSERT INTO batches VALUES(?,?,?,?,?)", (batch_id, checked["manifest_sha256"], canonical(manifest), transform, run_id))
            for dataset, records in incoming.items():
                ordinal = 0
                for file in (f for f in manifest["files"] if f["dataset"] == dataset):
                    connection.executemany("INSERT INTO bronze_records VALUES(?,?,?,?,?,?,?)",
                                           ((batch_id, dataset, ordinal + i, file["path"], i + 1, started, canonical(records[ordinal + i])) for i in range(file["rows"])))
                    ordinal += file["rows"]
            inject("after_ingestion")
            stage("silver")
            combined = {dataset: [] for dataset in DATASETS}
            provenance = {dataset: [] for dataset in DATASETS}
            for source_batch, dataset, source_file, source_row, payload in connection.execute("SELECT r.batch_id,r.dataset,r.source_file,r.source_row,r.payload FROM bronze_records r JOIN batches b ON b.batch_id=r.batch_id ORDER BY b.rowid,r.dataset,r.ordinal"):
                combined[dataset].append(json.loads(payload))
                provenance[dataset].append({"input_batch_id": source_batch, "source_file": source_file, "source_row": source_row})
            result = evaluate(combined, manifest["cutoff"], manifest["report_start"])
            report["quality"] = enforce_quality(result, combined, policy)
            for dataset in DATASETS:
                if sum(result["reconciliation"][dataset].values()) != len(combined[dataset]):
                    raise ValueError("unbalanced reconciliation: " + dataset)
            for row in result["incident_resolution"]:
                connection.execute("INSERT INTO silver_incidents VALUES(?,?,?)", (run_id, row["incident_id"], canonical(row)))
            for item in result["dispositions"]:
                item.update(provenance[item["dataset"]][item["row"]])
                connection.execute("INSERT INTO record_dispositions VALUES(?,?,?,?)", (run_id, item["dataset"], item["row"], canonical(item)))
            inject("after_silver")
            stage("gold")
            for name in OUTPUTS:
                keys = list(SCHEMAS[name]["properties"])
                for row in result[name]:
                    validate_schema(row, SCHEMAS[name])
                    values = [canonical(row[k]) if isinstance(row[k], list) else row[k] for k in keys]
                    connection.execute("INSERT INTO output_" + name + " VALUES(" + ",".join("?" for _ in range(len(keys) + 1)) + ")", [run_id] + values)
                if name == "incident_resolution":
                    inject("between_gold_writes")
            stage("validation")
            for name in OUTPUTS:
                count = connection.execute("SELECT count(*) FROM output_" + name + " WHERE publication_id=?", (run_id,)).fetchone()[0]
                if count != len(result[name]):
                    raise ValueError("output count mismatch: " + name)
            stage("publication")
            inject("before_publish")
            report.update(status="committed", publication_id=run_id, raw_archive=str(archive),
                          input_rows={k: len(v) for k, v in combined.items()},
                          output_rows={k: len(result[k]) for k in OUTPUTS},
                          input_bytes=sum(f["bytes"] for f in manifest["files"]),
                          input_file_count=len(manifest["files"]), quality_status="passed",
                          reconciliation=result["reconciliation"], quarantined_incidents=result["quarantined_incidents"])
            connection.execute("INSERT INTO publications VALUES(?,?,?,?)", (run_id, batch_id, manifest["cutoff"], canonical(report)))
            connection.execute("INSERT OR REPLACE INTO active_publication VALUES(1,?)", (run_id,))
        stage("complete")
        report.update(finished_at=iso(datetime.now(timezone.utc)), runtime_seconds=round(time.perf_counter() - clock, 6),
                      cpu_seconds=round(time.process_time() - cpu_clock, 6),
                      process_peak_rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                      process_peak_rss_unit="bytes" if platform.system() == "Darwin" else "KiB")
        connection.execute("UPDATE attempts SET status=?,evidence=? WHERE attempt_id=?", (report["status"], canonical(report), attempt))
        if report["status"] == "committed":
            connection.execute("UPDATE publications SET report=? WHERE publication_id=?", (canonical(report), run_id))
        connection.commit()
        persist_evidence()
        return report
    except Exception as error:
        if connection:
            connection.rollback()
        report.update(status="failed", error=str(error), finished_at=iso(datetime.now(timezone.utc)), runtime_seconds=round(time.perf_counter() - clock, 6))
        report["stages"][report["stage"]] = round(time.perf_counter() - stage_clock, 6)
        persist_evidence()
        if connection:
            try:
                connection.execute("UPDATE attempts SET status='failed',batch_id=?,evidence=? WHERE attempt_id=?", (report["batch_id"], canonical(report), attempt))
                connection.commit()
            except sqlite3.Error:
                pass  # The evidence file survives an unavailable metadata store.
        raise
    finally:
        if connection:
            connection.close()


def query(db, sql):
    connection = sqlite3.connect(Path(db).resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        return [dict(row) for row in connection.execute(sql)]
    finally:
        connection.close()
