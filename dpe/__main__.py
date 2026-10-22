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
    sql = commands.add_parser("query", help="Execute read-only SQL against published data")
    sql.add_argument("--db", type=Path, required=True)
    sql.add_argument("--sql", required=True)
    demo = commands.add_parser("demo", help="Generate Smoke data and publish initial + delta")
    demo.add_argument("--workspace", type=Path, default=Path("data/demo"))
    bench = commands.add_parser("benchmark", help="Repeat local runs; record comparable evidence")
    bench.add_argument("--workspace", type=Path, required=True)
    bench.add_argument("--count", type=int, default=1000)
    bench.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    from .ingestion import validate_batch
    from .pipeline import query, run
    from .synthetic import generate
    from .validation import benchmark, check_fixtures
    if args.command == "generate":
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
        result = run(args.batch, args.db, args.api_url, args.fail_at)
        if args.report:
            write_json(args.report, result)
    elif args.command == "query":
        result = query(args.db, args.sql)
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
