"""Run from the repository root: python3 -m dpe --help."""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .common import write_json


def main():
    parser = argparse.ArgumentParser(description="Local ITSM platform development harness")
    commands = parser.add_subparsers(dest="command", required=True)
    gen = commands.add_parser("generate", help="Generate immutable initial/delta source batches")
    gen.add_argument("--out", type=Path, required=True)
    gen.add_argument("--profile", choices=["smoke", "small", "medium", "large"], default="smoke")
    gen.add_argument("--count", type=int)
    gen.add_argument("--config", type=Path)
    gen.add_argument("--mode", choices=["baseline", "faults"])
    gen.add_argument("--seed", type=int)
    validate = commands.add_parser("validate", help="Verify source checksums, counts, schemas")
    validate.add_argument("batch", type=Path)
    commands.add_parser("check-fixtures", help="Check hand-authored business-rule results")
    serve = commands.add_parser("serve", help="Serve immutable event pages on localhost")
    serve.add_argument("batch", type=Path)
    serve.add_argument("--port", type=int, default=8765)
    execute = commands.add_parser("run", help="Ingest, reconcile, and publish a local batch")
    execute.add_argument("batch", type=Path)
    execute.add_argument("--db", type=Path, required=True)
    execute.add_argument("--api-url")
    execute.add_argument("--fail-at", choices=["after_ingestion", "after_silver", "between_gold_writes", "before_publish"])
    execute.add_argument("--report", type=Path)
    execute.add_argument("--quality-policy", type=Path)
    sql = commands.add_parser("query", help="Execute read-only SQL against published data")
    sql.add_argument("--db", type=Path, required=True)
    sql.add_argument("--sql", required=True)
    demo = commands.add_parser("demo", help="Generate Smoke data and publish initial + delta")
    demo.add_argument("--workspace", type=Path, default=Path("data/demo"))
    bench = commands.add_parser("benchmark", help="Repeat local runs; record comparable evidence")
    bench.add_argument("--workspace", type=Path, required=True)
    bench.add_argument("--count", type=int, default=1000)
    bench.add_argument("--repeats", type=int, default=3)
    for name in ("lake-compat", "lake-fixtures"):
        command = commands.add_parser(name, help="Verify the real local Trino/Iceberg path")
        command.add_argument("--workspace", type=Path, required=True)
    verify = commands.add_parser("lake-verify", help="Verify compatibility, fixtures, Smoke/delta, publication failures, and replay")
    verify.add_argument("--workspace", type=Path, required=True)
    verify.add_argument("--count", type=int, default=1000)
    for name in ("lake-incremental-verify", "lake-incremental-restart-check"):
        check = commands.add_parser(name, help="Verify targeted processing on the real local stack")
        check.add_argument("--workspace", type=Path, required=True)
        if name == "lake-incremental-verify": check.add_argument("--count", type=int, default=1000)
    lake = commands.add_parser("lake-run", help="Publish a batch through dbt/Trino/Iceberg")
    lake.add_argument("batch", type=Path)
    lake.add_argument("--workspace", type=Path, required=True)
    lake.add_argument("--strategy", choices=["targeted", "full"], default="targeted")
    lake.add_argument("--api-url")
    lake.add_argument("--quality-policy", type=Path)
    lake.add_argument("--fail-at", choices=["after_input_persistence", "during_selective_writes", "between_gold_writes", "before_publish"])
    audit = commands.add_parser("lake-audit", help="Audit all committed raw source bytes separately")
    audit.add_argument("--workspace", type=Path, required=True)
    consumer = commands.add_parser("lake-query", help="Read a contracted, snapshot-pinned Gold product")
    consumer.add_argument("--workspace", type=Path, required=True)
    consumer.add_argument("--dataset", choices=["incident_resolution", "service_daily_metrics"], required=True)
    consumer.add_argument("--limit", type=int, default=10)
    consumer.add_argument("--publication", help="Pin multiple reads to the same publication ID")
    sequence=commands.add_parser("generate-sequence",help="Generate versioned 30/60/90-day deliveries")
    sequence.add_argument("--out",type=Path,required=True);sequence.add_argument("--days",type=int,choices=[30,60,90],default=30)
    sequence.add_argument("--count",type=int,default=1000);sequence.add_argument("--seed",type=int,default=42);sequence.add_argument("--config",type=Path)
    cost=commands.add_parser("cost-report",help="Calculate imported usage/rates and explicit allocations")
    cost.add_argument("--usage",type=Path,required=True);cost.add_argument("--rates",type=Path);cost.add_argument("--out",type=Path,required=True)
    imported=commands.add_parser("cost-import",help="Import explicitly mapped cost-detail CSV")
    imported.add_argument("--csv",type=Path,required=True);imported.add_argument("--mapping",type=Path,required=True);imported.add_argument("--out",type=Path,required=True)
    for name in ("cost-demo","poc-status","spark-compat","spark-fixtures","poc-verify"):
        command=commands.add_parser(name);command.add_argument("--workspace",type=Path,required=True)
    spark_run=commands.add_parser("spark-run",help="Full Spark pipeline through the common Gold publication contract")
    spark_run.add_argument("batch",type=Path);spark_run.add_argument("--workspace",type=Path,required=True)
    spark_run.add_argument("--fail-at",choices=["between_gold_writes","before_publish"])
    poc=commands.add_parser("poc-benchmark",help="Paired local Trino/Spark workloads and explicit cost reports")
    poc.add_argument("--workspace",type=Path,required=True);poc.add_argument("--count",type=int,default=1000)
    poc.add_argument("--repeats",type=int,default=3);poc.add_argument("--engines",nargs='+',choices=['trino','spark'],default=['trino','spark'])
    calibration=commands.add_parser("poc-calibrate",help="Generate a tier and assess the frozen local input budget without processing it")
    calibration.add_argument("--workspace",type=Path,required=True)
    calibration.add_argument("--count",type=int,default=100000)
    args = parser.parse_args()
    from .ingestion import validate_batch
    from .pipeline import query, run
    from .synthetic import generate
    from .validation import benchmark, check_fixtures
    if args.command == "generate-sequence":
        from .sequences import generate_sequence
        result=generate_sequence(args.out,args.days,args.count,args.seed,args.config)
    elif args.command == "cost-report":
        from .costing import report
        result=report(args.usage,args.out,args.rates)
    elif args.command == "cost-import":
        from .cost_import import import_details
        result=import_details(args.csv,args.mapping,args.out)
    elif args.command == "cost-demo":
        from .economics_demo import demo
        result=demo(args.workspace)
    elif args.command == "poc-status":
        from .benchmark_reports import scorecard
        from .common import read_json
        prior=read_json(args.workspace/'run_results.json') if (args.workspace/'run_results.json').exists() else {}
        result=scorecard(args.workspace,prior.get('runs'),prior.get('findings'))
    elif args.command == "poc-calibrate":
        from .benchmark_calibration import calibrate
        result=calibrate(args.workspace,args.count)
    elif args.command == "poc-benchmark":
        from .benchmark_runner import run as benchmark_run
        result=benchmark_run(args.workspace,args.count,args.repeats,tuple(args.engines))
    elif args.command in ("spark-compat","spark-fixtures","spark-run"):
        from .engines import spark
        if args.command=="spark-compat":result=spark.compatibility(args.workspace)
        elif args.command=="spark-fixtures":result=spark.fixtures(args.workspace)
        else:result=spark.run(args.batch,args.workspace,args.fail_at)
    elif args.command == "poc-verify":
        from .poc_validation import verify
        result=verify(args.workspace)
    elif args.command == "generate":
        result = generate(args.out, args.profile, args.count, args.config, args.mode, args.seed)
    elif args.command == "validate":
        result = validate_batch(args.batch)
    elif args.command == "check-fixtures":
        result = check_fixtures()
    elif args.command == "serve":
        from .api import make_server
        with make_server(args.batch, port=args.port) as server:
            print("Serving immutable incident events at http://127.0.0.1:%d" % server.server_port, flush=True)
            server.serve_forever()
        return
    elif args.command == "run":
        result = run(args.batch, args.db, args.api_url, args.fail_at, args.quality_policy)
        if args.report:
            write_json(args.report, result)
    elif args.command == "query":
        result = query(args.db, args.sql)
    elif args.command.startswith("lake-"):
        from . import lakehouse
        if args.command == "lake-compat":
            result = lakehouse.compatibility(args.workspace)
        elif args.command == "lake-fixtures":
            result = lakehouse.fixture_check(args.workspace)
        elif args.command == "lake-incremental-verify":
            from .incremental_validation import verify
            result = verify(args.workspace, args.count)
        elif args.command == "lake-incremental-restart-check":
            from .incremental_validation import restart_check
            result = restart_check(args.workspace)
        elif args.command == "lake-run":
            from . import incremental
            engine = incremental if args.strategy == "targeted" else lakehouse
            result = engine.run(args.batch, args.workspace, args.fail_at, args.api_url, args.quality_policy)
        elif args.command == "lake-audit":
            from .incremental import audit
            result = audit(args.workspace)
        elif args.command == "lake-verify":
            result = lakehouse.verify(args.workspace, args.count)
        else:
            result = lakehouse.consume(args.workspace, args.dataset, args.limit, args.publication)
    elif args.command == "demo":
        if not (args.workspace / "source").exists():
            generate(args.workspace / "source")
        fixtures = check_fixtures()
        initial = run(args.workspace / "source/initial", args.workspace / "platform.sqlite")
        delta = run(args.workspace / "source/delta", args.workspace / "platform.sqlite")
        result = {"fixtures": fixtures, "initial": initial, "delta": delta}
        write_json(args.workspace / "evidence.json", result)
    else:
        result = benchmark(args.workspace, args.count, args.repeats)
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, OSError, AssertionError, sqlite3.Error) as error:
        print("error: " + str(error), file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)
