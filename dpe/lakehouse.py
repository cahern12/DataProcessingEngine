"""Real local dbt/Trino/Iceberg execution and snapshot-pinned publication.

Optional dependencies are imported lazily so the reference CLI stays standalone.
"""

import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from .common import DATASETS, ROOT, canonical, digest, iso, read_json, write_json
from .ingestion import load_batch
from .pipeline import revision
from .quality import enforce_quality, load_policy
from .rules import SCHEMAS

OUTPUTS = ("incident_resolution", "service_daily_metrics")
PROJECT = ROOT / "transformations/dbt"


def identifier(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError("invalid SQL identifier")
    return value


def literal(value):
    if value is None:
        return "NULL"
    if type(value) in (int, float):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def normalized(value):
    if isinstance(value, datetime):
        # Fixed microsecond values are compared as canonical UTC ISO strings.
        return iso(value).replace(".000000Z", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [normalized(v) for v in value]
    return value


def query_telemetry(query_id):
    """Persist coordinator statistics before its bounded query history expires."""
    from urllib.request import Request, urlopen
    url = "http://" + os.environ.get("DPE_TRINO_HOST", "127.0.0.1") + ":" + os.environ.get("DPE_TRINO_PORT", "18080")
    try:
        with urlopen(Request(url + "/v1/query/" + query_id, headers={"X-Trino-User": "dpe"}), timeout=10) as stream:
            stats = json.load(stream)["queryStats"]
        return {k: stats[k] for k in ("physicalInputDataSize", "physicalInputPositions", "processedInputDataSize",
                "processedInputPositions", "physicalWrittenDataSize", "writtenPositions", "peakTotalMemoryReservation",
                "spilledDataSize", "totalCpuTime", "elapsedTime")}
    except Exception as error:
        event=ROOT/'data/telemetry/trino-events'/(str(query_id)+'.json')
        if event.exists():
            try:
                raw=read_json(event);stats=raw['statistics']
                mapping={'physicalInputDataSize':'physicalInputBytes','physicalInputPositions':'physicalInputRows',
                         'processedInputDataSize':'processedInputBytes','processedInputPositions':'processedInputRows',
                         'physicalWrittenDataSize':'writtenBytes','writtenPositions':'writtenRows','spilledDataSize':'spilledBytes',
                         'totalCpuTime':'cpuTime','elapsedTime':'wallTime'}
                value={key:(str(stats[source])+'B' if key.endswith('DataSize') else format(stats[source],'.9f')+'s' if key.endswith('Time') else stats[source]) for key,source in mapping.items()}
                value.update(peakTotalMemoryReservation=None,peakUserMemoryBytes=stats.get('peakUserMemoryBytes'),
                             source='durable_completed_event',source_ref=str(event),unsupported=['query_peak_total_memory_reservation'],history_lookup_error=str(error))
                return value
            except Exception as event_error:return {'unavailable':str(error),'completed_event_error':str(event_error)}
        return {"unavailable": str(error)}


class Client:
    def __init__(self):
        from trino.dbapi import connect
        self.connection = connect(host=os.environ.get("DPE_TRINO_HOST", "127.0.0.1"),
                                  port=int(os.environ.get("DPE_TRINO_PORT", "18080")),
                                  user="dpe", catalog="lake", timezone="UTC",
                                  request_timeout=120, max_attempts=3)
        self.queries = []

    def sql(self, sql):
        started = time.perf_counter()
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [{column[0]: normalized(value) for column, value in zip(cursor.description or [], row)} for row in rows]
        except BaseException:
            try:
                if cursor.query_id:cursor.cancel()
            except Exception:pass
            raise
        finally:
            self.queries.append({"query_id": cursor.query_id, "sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
                                 "stage": getattr(self, "stage", "unclassified"),
                                 "runtime_seconds": round(time.perf_counter()-started, 6), "stats": cursor.stats,
                                 "telemetry": query_telemetry(cursor.query_id) if cursor.query_id else {}})

    def create_sources(self, schema, cases):
        schema = identifier(schema)
        self.sql("CREATE SCHEMA IF NOT EXISTS lake." + schema)
        self.sql("CREATE TABLE lake." + schema + ".raw_records(case_id varchar,dataset varchar,ordinal bigint,batch_id varchar,source_file varchar,source_row bigint,payload varchar)")
        self.sql("CREATE TABLE lake." + schema + ".batch_context(case_id varchar,cutoff varchar,report_start varchar)")
        if os.environ.get('DPE_INGEST_MODE')=='parquet_add_files':
            from .parquet_landing import register,raw_rows
            for case in cases:self.sql('INSERT INTO lake.'+schema+'.batch_context VALUES ('+','.join(literal(case[k]) for k in ('name','cutoff','report_start'))+')')
            register(self,'lake.'+schema+'.raw_records',raw_rows(cases))
            return
        rows = [];payload_bytes=0
        batch_size=int(os.environ.get('DPE_INSERT_ROWS','200'))
        byte_limit=int(os.environ.get('DPE_INSERT_BYTES','0'))
        if batch_size<1 or byte_limit<0:raise ValueError('invalid source insertion bounds')
        for case in cases:
            self.sql("INSERT INTO lake." + schema + ".batch_context VALUES (" + ",".join(literal(case[k]) for k in ("name", "cutoff", "report_start")) + ")")
            for dataset, records in case["datasets"].items():
                source=iter(case.get('provenance',{}).get(dataset,[]))
                for number, record in enumerate(records):
                    origin=next(source,{})
                    payload=canonical(record)
                    row_bytes=len(payload.encode())+512
                    if rows and byte_limit and payload_bytes+row_bytes>byte_limit:
                        self._insert(schema,rows);rows=[];payload_bytes=0
                    rows.append((case["name"], dataset, number, origin.get("batch_id", case["name"]),
                                 origin.get("source_file", "fixture"), origin.get("source_row", number+1), payload))
                    payload_bytes+=row_bytes
                    if len(rows) == batch_size:
                        self._insert(schema, rows); rows = [];payload_bytes=0
        if rows:
            self._insert(schema, rows)

    def _insert(self, schema, rows):
        self.sql("INSERT INTO lake." + schema + ".raw_records VALUES " + ",".join(
            "(" + ",".join(literal(v) for v in row) + ")" for row in rows))


def sql_revision():
    hasher = hashlib.sha256(revision().encode())
    for path in sorted(PROJECT.rglob("*")):
        if path.is_file() and path.suffix in (".sql", ".yml") and path.name!=".user.yml":
            hasher.update(str(path.relative_to(PROJECT)).encode()); hasher.update(path.read_bytes())
    for folder in (ROOT / "transformations/spark", ROOT / "orchestration"):
        for path in sorted(folder.rglob("*")):
            if path.is_file() and path.suffix in (".sql", ".py", ".json"):
                hasher.update(str(path.relative_to(ROOT)).encode()); hasher.update(path.read_bytes())
    for path in [ROOT / "requirements-local.lock",ROOT / "requirements-parquet.lock", *sorted((ROOT / "infra/local").rglob("*"))]:
        if path.is_file() and path.suffix not in (".md", ".pyc", ".pyo") and "__pycache__" not in path.parts:
            hasher.update(str(path.relative_to(ROOT)).encode()); hasher.update(path.read_bytes())
    return hasher.hexdigest()


def dbt(schema, evidence, selection=None, exclude=None, command="build", variables=None):
    started = time.perf_counter()
    executable = Path(sys.executable).parent / "dbt"
    executable = str(executable) if executable.exists() else shutil.which("dbt")
    if not executable:
        raise ValueError("dbt unavailable; use .venv/bin/python and requirements-local.lock")
    extra_variables = variables or {}
    variables = {"raw_schema": identifier(schema), "business_rules": read_json(ROOT / "rules/business_rules.v1.json"),
                 "row_schemas": {name: SCHEMAS[name] for name in DATASETS}}
    variables.update(extra_variables)
    import csv
    with (ROOT / "rules/sla_targets.v1.csv").open() as targets:
        variables["sla_targets"] = {row["priority"]: [int(row["response_minutes"]),int(row["resolution_minutes"])] for row in csv.DictReader(targets)}
    evidence = Path(evidence).resolve(); evidence.mkdir(parents=True, exist_ok=True)
    invocation = uuid.uuid4().hex
    artifact_path = evidence / invocation
    args = [executable, command, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT),
            "--target-path", str(artifact_path / "target"), "--log-path", str(artifact_path / "logs"),
            "--vars", canonical(variables), "--indirect-selection", "cautious"]
    if selection:
        args.extend(["--select", *selection])
    if exclude:
        args.extend(["--exclude", *exclude])
    env = dict(os.environ, DPE_DBT_SCHEMA=schema, DBT_SEND_ANONYMOUS_USAGE_STATS="false", DBT_USE_COLORS="false")
    process = subprocess.run(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    (artifact_path / "console.log").parent.mkdir(parents=True, exist_ok=True)
    (artifact_path / "console.log").write_text(process.stdout)
    if process.returncode:
        raise RuntimeError("dbt failed; log: " + str(artifact_path / "console.log") + "\n" + process.stdout[-5000:])
    result = {"invocation": invocation, "artifacts": str(artifact_path), "runtime_seconds": round(time.perf_counter()-started,6)}
    run_results = artifact_path / "target/run_results.json"
    if run_results.exists():
        result["queries"] = []
        for node in read_json(run_results)["results"]:
            response = node.get("adapter_response", {})
            query_id = response.get("query_id")
            if query_id:
                result["queries"].append({"node": node["unique_id"], "query_id": query_id,
                                           "telemetry": query_telemetry(query_id)})
    return result


def build(client, schema, evidence, fail_at=None):
    invocations = []
    for options, stage in [
        ({"exclude":["gold_incident_resolution","gold_service_daily_metrics","incident_daily_contributions","compatibility_probe"],"command":"run"}, "incident_transformation"),
        ({"selection":["gold_incident_resolution"]}, "incident_transformation"),
        ({"selection":["incident_daily_contributions","gold_service_daily_metrics"]}, "daily_aggregation"),
        ({"selection":["unique_gold_keys","valid_reference_data"],"command":"test"}, "validation")]:
        result = dbt(schema, evidence, **options); result["stage"] = stage; invocations.append(result)
        if options.get("selection")==["gold_incident_resolution"] and fail_at=="between_gold_writes":
            raise RuntimeError("injected failure: between_gold_writes")
    return invocations


def compatibility(workspace):
    workspace = Path(workspace); workspace.mkdir(parents=True, exist_ok=True)
    client, schema = Client(), "dpe_compat_" + uuid.uuid4().hex[:12]
    evidence = {"schema": schema, "engine_version": client.sql("select version() version")[0]["version"]}
    try:
        client.sql("CREATE SCHEMA lake." + schema)
        client.sql("CREATE TABLE lake." + schema + ".probe(id bigint,value varchar)")
        client.sql("INSERT INTO lake." + schema + ".probe VALUES(1,'before'),(2,'second')")
        client.sql("UPDATE lake." + schema + ".probe SET value='after' WHERE id=1")
        client.sql("MERGE INTO lake." + schema + ".probe t USING (VALUES(2,'merged'),(3,'new')) s(id,value) ON t.id=s.id WHEN MATCHED THEN UPDATE SET value=s.value WHEN NOT MATCHED THEN INSERT(id,value) VALUES(s.id,s.value)")
        evidence["rows"] = client.sql("SELECT * FROM lake." + schema + ".probe ORDER BY id")
        if evidence["rows"] != [{"id":1,"value":"after"},{"id":2,"value":"merged"},{"id":3,"value":"new"}]:
            raise AssertionError("Iceberg mutation result mismatch")
        evidence["snapshots"] = client.sql('SELECT snapshot_id FROM lake.' + schema + '."probe$snapshots"')
        evidence["files"] = client.sql('SELECT file_format,file_size_in_bytes FROM lake.' + schema + '."probe$files"')
        evidence["dbt"] = dbt(schema, workspace / "dbt", selection=["compatibility_probe"])
        evidence["status"] = "passed"
        return evidence
    except Exception as error:
        evidence.update(status="failed", error=str(error)); raise
    finally:
        evidence["queries"] = client.queries
        write_json(workspace / "compatibility.json", evidence)


def fixture_check(workspace):
    workspace = Path(workspace); workspace.mkdir(parents=True, exist_ok=True)
    cases = read_json(ROOT / "synthetic/fixtures/rule_cases.v1.json")["cases"]
    client, schema = Client(), "dpe_fixtures_" + uuid.uuid4().hex[:12]
    evidence = {"schema": schema, "fixtures": len(cases), "revision": sql_revision()}
    try:
        client.create_sources(schema, cases)
        evidence["dbt"] = build(client, schema, workspace / "dbt")
        rows = {name: client.sql("SELECT * FROM lake." + schema + ".gold_" + name) for name in OUTPUTS}
        errors = client.sql("SELECT * FROM lake." + schema + ".quarantine")
        dispositions = client.sql("SELECT case_id,dataset,ordinal,disposition,rule_id FROM lake."+schema+".record_dispositions")
        differences = []
        for row in dispositions:
            if row['disposition']=='duplicate' and row['rule_id']!='BR-002':
                differences.append({'case':row['case_id'],'dataset':row['dataset'],'ordinal':row['ordinal'],
                                    'error':'duplicate must retain BR-002 rule ID','actual':row['rule_id']})
        for case in cases:
            actual = [{k:v for k,v in row.items() if k!="case_id"} for row in rows["incident_resolution"] if row["case_id"]==case["name"]]
            expected = [] if "expected_quarantine_rule" in case else [case["expected_incident"]]
            differences.extend(compare_rows("incident_resolution", expected, actual, case["name"]))
            if "expected_quarantine_rule" in case and case["expected_quarantine_rule"] not in {
                    r["rule_id"] for r in errors if r["case_id"]==case["name"]}:
                differences.append({"case":case["name"],"error":"missing expected quarantine rule"})
            if "expected_daily" in case:
                actual = [{k:v for k,v in r.items() if k!="case_id"} for r in rows["service_daily_metrics"] if r["case_id"]==case["name"]]
                differences.extend(compare_rows("service_daily_metrics", case["expected_daily"], actual, case["name"]))
            for key, disposition in (("expected_duplicate_count","duplicate"),("expected_deferred_count","deferred")):
                if key in case:
                    count=sum(1 for r in dispositions if r["case_id"]==case["name"] and r["disposition"]==disposition)
                    if count!=case[key]:
                        differences.append({"case":case["name"],"disposition":disposition,"expected":case[key],"actual":count})
        evidence["differences"] = differences
        evidence["status"] = "failed" if differences else "passed"
        if differences:
            raise AssertionError("SQL fixture differences: " + canonical(differences[:10]))
        return evidence
    except Exception as error:
        evidence.update(status="failed", error=str(error)); raise
    finally:
        evidence["queries"] = client.queries
        write_json(workspace / "fixtures.json", evidence)


def compare_rows(dataset, expected, actual, case="__batch__"):
    keys = ["incident_id"] if dataset=="incident_resolution" else ["date","service_id","priority"]
    def indexed(rows):
        result = {}
        for row in rows:
            key = tuple(row[k] for k in keys)
            if key in result:
                raise AssertionError("duplicate output key: " + str(key))
            result[key] = row
        return result
    left, right = indexed(expected), indexed(actual)
    differences = []
    for key in sorted(left.keys() | right.keys()):
        if key not in left or key not in right:
            differences.append({"case":case,"dataset":dataset,"key":key,"expected":left.get(key),"actual":right.get(key)})
            continue
        for field in sorted(left[key].keys() | right[key].keys()):
            if (field in left[key])!=(field in right[key]) or left[key].get(field)!=right[key].get(field):
                differences.append({"case":case,"dataset":dataset,"key":key,"field":field,"expected":left[key].get(field),"actual":right[key].get(field)})
    return differences


@contextmanager
def workspace_lock(workspace):
    workspace = Path(workspace); workspace.mkdir(parents=True, exist_ok=True)
    with (workspace / ".writer.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def atomic_json(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        write_json(temporary, value)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def committed_publications(workspace):
    """Follow only the active ancestry; a prepared orphan is not committed."""
    path = Path(workspace) / "active.json"
    current = read_json(path) if path.exists() else None
    seen = set()
    while current:
        identity = current["publication_id"]
        if identity in seen or not re.fullmatch(r"[0-9a-f]{64}",identity):
            raise ValueError("invalid publication ancestry")
        seen.add(identity)
        yield current
        parent = current["parent_publication_id"]
        if parent is not None and not re.fullmatch(r"[0-9a-f]{64}",parent):
            raise ValueError("invalid parent publication identity")
        current = read_json(Path(workspace)/"publications"/(parent+".json")) if parent else None


def object_store():
    import boto3
    return boto3.client("s3", endpoint_url=os.environ.get("DPE_S3_ENDPOINT", "http://127.0.0.1:19000"),
                        aws_access_key_id="dpe-local", aws_secret_access_key="local-dpe-only", region_name="us-east-1")


def archived_batch(path):
    path=Path(path)
    header,records,validation=load_batch(path)
    if path.name!=validation["manifest_sha256"]:
        raise ValueError("archived manifest identity mismatch")
    return header,records,validation


def run(batch, workspace, fail_at=None, api_url=None, policy_path=None):
    workspace = Path(workspace)
    with workspace_lock(workspace):
        return _run(Path(batch), workspace, fail_at, api_url, policy_path)


def _run(batch, workspace, fail_at, api_url, policy_path):
    attempt, started = uuid.uuid4().hex, time.perf_counter()
    evidence = {"attempt_id":attempt,"status":"running","stage":"ingestion","source_location":str(batch.resolve()),
                "started_at":iso(datetime.now(timezone.utc)),"engine":"dbt+Trino+Iceberg","revision":sql_revision()}
    evidence_path = workspace / "attempts" / (attempt+".json")
    atomic_json(evidence_path, evidence)
    client = None
    try:
        if fail_at not in (None,"between_gold_writes","before_publish"):
            raise ValueError("unsupported lakehouse failpoint")
        from .bounded_io import enabled,sources,release
        bounded=enabled() and not api_url
        if bounded:manifest,incoming,_,checked=sources([batch])
        else:manifest, incoming, checked = load_batch(batch, api_url, workspace / "api-landing" / attempt)
        policy = load_policy(policy_path)
        policy_hash = hashlib.sha256(canonical(policy).encode()).hexdigest()
        identity = hashlib.sha256(canonical([checked["manifest_sha256"], evidence["revision"], policy_hash]).encode()).hexdigest()
        active = read_json(workspace / "active.json") if (workspace / "active.json").exists() else None
        for old in committed_publications(workspace):
            if old["batch_id"] == manifest["batch_id"]:
                if old["identity"] != identity:
                    raise ValueError("committed source/code/policy changed; use a fresh workspace")
                evidence.update(status="already_committed",publication_id=old["publication_id"])
                return evidence
        from .common import timestamp
        if active and (manifest["parent_batch_id"]!=active["batch_id"] or timestamp(manifest["cutoff"])<timestamp(active["cutoff"])):
            raise ValueError("delta must follow active parent and cutoff")
        if not active and manifest["batch_kind"]!="initial":
            raise ValueError("process initial batch first")
        if active and manifest["report_start"]!=active["report_start"]:
            raise ValueError("reporting start changed")
        publication_id=hashlib.sha256(canonical([identity,active["publication_id"] if active else None]).encode()).hexdigest()
        evidence.update(batch_id=manifest["batch_id"],input_manifest_checksum=checked["manifest_sha256"],run_id=publication_id,
                        dataset_version=manifest["dataset_version"],rule_version=manifest["rule_version"],cutoff=manifest["cutoff"],
                        generator_seed=manifest.get("seed"),transformation_revision=evidence["revision"],quality_policy=policy)
        archived = workspace / "sources" / checked["manifest_sha256"]
        if not archived.exists():
            temporary = archived.with_name(archived.name+"."+attempt+".tmp")
            try:
                shutil.copytree(batch, temporary)
                if bounded:
                    from .ingestion import validate_batch
                    validate_batch(temporary)
                else:load_batch(temporary)
                temporary.rename(archived)
            finally:
                if temporary.exists(): shutil.rmtree(temporary)
        stored = sources([archived]) if bounded else archived_batch(archived)
        stored_check=stored[3] if bounded else stored[2]
        if stored_check["manifest_sha256"]!=checked["manifest_sha256"]:
            raise ValueError("archived manifest mismatch")
        archives = (active["archives"] if active else []) + [str(archived.relative_to(workspace))]
        if bounded:
            del incoming,stored
            release()
        combined = {name:[] for name in DATASETS}; provenance = {name:[] for name in DATASETS}
        store = object_store()
        from .common import safe_file
        for archive in archives:
            source=safe_file(workspace,archive)
            if bounded:header,records,_,validation=sources([source])
            else:header, records, validation = archived_batch(source)
            for entry in header["files"] + [{"path":"manifest.json"}]:
                path = safe_file(source,entry["path"])
                key = validation["manifest_sha256"] + "/" + entry["path"]
                store.put_object(Bucket="raw",Key=key,Body=path.read_bytes(),Metadata={"sha256":digest(path)})
            if not bounded:
                for name in DATASETS:
                    combined[name].extend(records[name])
                    for file in (f for f in header["files"] if f["dataset"]==name):
                        provenance[name].extend({"batch_id":header["batch_id"],"source_file":file["path"],"source_row":n+1} for n in range(file["rows"]))
        if bounded:_,combined,provenance,_=sources([safe_file(workspace,a) for a in archives],verify=False)
        for page in (workspace / "api-landing" / attempt).glob("*.json"):
            store.put_object(Bucket="raw",Key="api/"+attempt+"/"+page.name,Body=page.read_bytes())
        evidence["stage"]="bronze"
        client, schema = Client(), "dpe_run_" + attempt[:16]
        client.stage = "history_selection"
        evidence["engine_version"]={"trino":client.sql("SELECT version() version")[0]["version"]}
        evidence["schema"] = schema
        client.create_sources(schema,[{"name":"__batch__","datasets":combined,"provenance":provenance,
                                      "cutoff":manifest["cutoff"],"report_start":manifest["report_start"]}])
        if hasattr(client,'ingestion'):evidence['ingestion']=client.ingestion
        evidence["stage"]="dbt_transformation"
        evidence["dbt"] = build(client,schema,workspace / "dbt" / attempt,fail_at)
        evidence["stage"]="validation"
        client.stage = "validation"
        quarantines = client.sql("SELECT * FROM lake."+schema+".quarantine")
        result = {"quarantined_incidents":{row["incident_id"]:[] for row in quarantines}}
        evidence["quality"] = enforce_quality(result,combined,policy)
        evidence["reconciliation"] = client.sql("SELECT dataset,disposition,count(*) rows FROM lake."+schema+".record_dispositions GROUP BY 1,2")
        for name in DATASETS:
            if sum(row["rows"] for row in evidence["reconciliation"] if row["dataset"]==name)!=len(combined[name]):
                raise AssertionError("unbalanced SQL record dispositions: "+name)
        snapshots = {}
        for name in OUTPUTS:
            table = "gold_"+name
            snapshot = client.sql('SELECT snapshot_id FROM lake.'+schema+'."'+table+'$snapshots"')[0]["snapshot_id"]
            rows = client.sql("SELECT * FROM lake."+schema+"."+table+" FOR VERSION AS OF "+str(snapshot))
            for row in rows:
                from .common import validate_schema
                validate_schema({k:v for k,v in row.items() if k!="case_id"},SCHEMAS[name])
            snapshots[name]={"table":"lake."+schema+"."+table,"snapshot_id":snapshot,"rows":len(rows)}
        evidence["stage"]="publication"
        if fail_at=="before_publish":
            raise RuntimeError("injected failure: before_publish")
        publication={"publication_id":publication_id,"identity":identity,"batch_id":manifest["batch_id"],
                     "parent_publication_id":active["publication_id"] if active else None,
                     "cutoff":manifest["cutoff"],"report_start":manifest["report_start"],"revision":evidence["revision"],
                     "rule_version":manifest["rule_version"],"contract_version":"v1","archives":archives,
                     "snapshots":snapshots,"attempt_id":attempt}
        atomic_json(workspace / "publications" / (publication_id+".json"),publication)
        atomic_json(workspace / "active.json",publication)
        evidence.update(status="committed",publication_id=publication_id,snapshots=snapshots)
        return evidence
    except Exception as error:
        evidence.update(status="failed",error=str(error)); raise
    finally:
        evidence.update(finished_at=iso(datetime.now(timezone.utc)),runtime_seconds=round(time.perf_counter()-started,6))
        if client: evidence["queries"]=client.queries
        atomic_json(evidence_path,evidence)


def consume(workspace,dataset,limit=None,publication_id=None):
    if dataset not in OUTPUTS:
        raise ValueError("unknown contracted dataset")
    if publication_id is not None and not re.fullmatch(r"[0-9a-f]{64}",publication_id):
        raise ValueError("invalid publication identity")
    if publication_id:
        publication=next((p for p in committed_publications(workspace) if p["publication_id"]==publication_id),None)
        if publication is None: raise ValueError("publication is not committed")
    else:
        publication=read_json(Path(workspace)/"active.json")
    snapshot=publication["snapshots"][dataset]
    parts=snapshot["table"].split(".")
    if len(parts)!=3 or any(identifier(part)!=part for part in parts) or type(snapshot["snapshot_id"]) is not int:
        raise ValueError("invalid publication metadata")
    sql="SELECT * FROM "+snapshot["table"]+" FOR VERSION AS OF "+str(snapshot["snapshot_id"])+" ORDER BY "+("incident_id" if dataset=="incident_resolution" else "date,service_id,priority")
    if limit is not None:
        if type(limit) is not int or limit<1: raise ValueError("limit must be positive")
        sql+=" LIMIT "+str(limit)
    return {"publication_id":publication["publication_id"],"dataset":dataset,
            "rows":[{k:v for k,v in row.items() if k!="case_id"} for row in Client().sql(sql)]}


def verify(workspace, count=1000):
    """Integration evidence for phases 1–3; leaves inputs and outputs inspectable."""
    from .synthetic import generate
    from .rules import evaluate
    workspace=Path(workspace)
    if workspace.exists():
        raise ValueError("verification workspace exists; choose a fresh path")
    workspace.mkdir(parents=True)
    evidence={"status":"running","scope":"05 phases 1–3; local integration","revision":sql_revision()}
    try:
        evidence["compatibility"]=compatibility(workspace/"compatibility")
        evidence["fixtures"]=fixture_check(workspace/"fixtures")
        generate(workspace/"source",count=count)
        platform=workspace/"platform"
        initial=workspace/"source/initial"; delta=workspace/"source/delta"
        evidence["initial"]=run(initial,platform)
        before=(platform/"active.json").read_bytes()
        evidence["failures"]={}
        for point in ("between_gold_writes","before_publish"):
            try:
                run(delta,platform,fail_at=point)
            except RuntimeError as error:
                if str(error)!="injected failure: "+point: raise
                evidence["failures"][point]="passed"
            else:
                raise AssertionError("failpoint did not interrupt publication")
            if (platform/"active.json").read_bytes()!=before:
                raise AssertionError("failed batch changed active publication")
            for dataset in OUTPUTS:
                if consume(platform,dataset)["publication_id"]!=evidence["initial"]["publication_id"]:
                    raise AssertionError("consumer observed partial publication")
        evidence["delta"]=run(delta,platform)
        h,base,_=load_batch(initial); header,updates,_=load_batch(delta)
        combined={name:base[name]+updates[name] for name in DATASETS}
        expected=evaluate(combined,header["cutoff"],header["report_start"])
        differences=[]
        publication=evidence["delta"]["publication_id"]
        for dataset in OUTPUTS:
            actual=consume(platform,dataset,publication_id=publication)["rows"]
            differences.extend(compare_rows(dataset,expected[dataset],actual))
        evidence["full_rebuild_differences"]=differences
        if differences: raise AssertionError("full-rebuild differences: "+canonical(differences[:5]))
        for batch in (initial,delta):
            if run(batch,platform)["status"]!="already_committed":
                raise AssertionError("replay was not idempotent")
        if read_json(platform/"active.json")["publication_id"]!=publication:
            raise AssertionError("old batch replay moved active publication")
        evidence["status"]="passed"
        return {"status":"passed","fixtures":evidence["fixtures"]["fixtures"],
                "initial_incidents":evidence["initial"]["snapshots"]["incident_resolution"]["rows"],
                "delta_incidents":evidence["delta"]["snapshots"]["incident_resolution"]["rows"],
                "failpoints":evidence["failures"],"report":str(workspace/"verification.json")}
    except Exception as error:
        evidence.update(status="failed",error=str(error));raise
    finally:
        atomic_json(workspace/"verification.json",evidence)
