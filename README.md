# Enterprise Data Processing POC

A runnable local foundation for the platform in [the requirements](docs/plan/01-enterprise_data_processing_poc_validation_plan.md) and [build plan](docs/plan/03-initial_platform_build_plan.md). The first implementation uses fictional ITSM data, our own business rules, and a Python/SQLite development harness.

The intended Azure + dbt/Trino + Iceberg architecture remains the integration target. Per the local-first scope, this implementation does not deploy Azure, Airflow, Trino, Iceberg, or Spark. Its results establish local business-rule, ingestion, and publication behavior; distributed performance, interoperability, and cloud costs remain unmeasured.

## Run it

Run these commands from this directory using Python 3.9+ on macOS/Linux. No third-party Python packages, cloud account, or containers are needed. Commands run from the source checkout; wheel/release packaging is outside this iteration.

```sh
python3 -m dpe check-fixtures
python3 -m unittest discover -s tests -v
python3 -m dpe demo
```

The demo generates an initial batch with 1,000 unique incidents and a delta with new incidents, corrections, reopened incidents, late events, and repeated deliveries. It publishes 1,100 incidents after the delta. Outputs are in `data/demo/platform.sqlite`, and the execution report is in `data/demo/evidence.json`. Generated data, databases, and raw archives are ignored by Git.

Query a stable Gold interface:

```sh
python3 -m dpe query --db data/demo/platform.sqlite \
  --sql 'SELECT state, count(*) AS incidents FROM gold_incident_resolution GROUP BY state'

python3 -m dpe query --db data/demo/platform.sqlite \
  --sql 'SELECT * FROM gold_service_daily_metrics ORDER BY date, service_id, priority LIMIT 10'
```

`demo` can be repeated against an unchanged implementation. Committed batches become `already_committed`, and replaying an older batch does not roll back the active publication. After changing code/contracts/rules, choose a fresh workspace, for example `python3 -m dpe demo --workspace data/demo-v2`.

## Generate and process your own scenarios

```sh
python3 -m dpe generate --out data/custom --count 2000 --seed 7
python3 -m dpe validate data/custom/initial
python3 -m dpe run data/custom/initial --db data/custom.sqlite \
  --report data/custom-initial-report.json
python3 -m dpe run data/custom/delta --db data/custom.sqlite
```

Generation is deterministic for a fixed configuration and version, writes partitioned files, and refuses to overwrite an existing destination. Each batch has a manifest with record counts, actual bytes, hashes, source-time ranges, configuration, and declared defects. The delta names its exact parent. Its late snapshot correction is effective before the initial cutoff, while its reopen/reassignment occurs afterward.

Use `--profile smoke|small|medium|large` or override with `--count`. [The default configuration](synthetic/configs/default.json) controls priorities, scenario weights, service/group skew, comments per incident, chunk size, and defect rates. Supply another JSON file with `--config`. `baseline` produces valid data; `faults` introduces at most one declared fault per incident:

```sh
python3 -m dpe generate --out data/faults --mode faults
python3 -m dpe run data/faults/initial --db data/faults.sqlite
```

Fault scenarios cover duplicates, conflicting snapshot versions, missing required references, invalid priorities, and malformed timestamps. Quarantine expectations come from the source manifest. Unexpected quarantines fail publication, and the run report accounts for accepted, duplicate, quarantined, and deferred records.

The generator streams records into chunks, but the local reference evaluator and publication harness load complete batch histories into memory. Use Smoke-sized inputs for development; Medium/Large profiles are available for source generation and eventual distributed processing, not a claim of local processing capacity.

## Exercise API ingestion

Start the local read-only mock API in one terminal:

```sh
python3 -m dpe serve data/custom/initial --port 8765
```

In another terminal:

```sh
python3 -m dpe run data/custom/initial --db data/api.sqlite \
  --api-url http://127.0.0.1:8765
```

The endpoint is `/v1/batches/<batch_id>/incident-events?limit=100`. Follow its `next_cursor` using the `cursor` parameter. Cursors retain file/byte positions across partitions. The client checks batch identity, manifest hash, and complete event contents against the immutable source. Original API page bytes are retained in an attempt-specific API landing directory beside the database; exact source files are also archived.

## Understand and verify the rules

- [Business rules](rules/business_rules.v1.json) specify canonical domains, state transitions, time calculations, and correction semantics.
- [SLA targets](rules/sla_targets.v1.csv) define our synthetic P1–P4 thresholds on a 24×7 UTC clock.
- [Input](contracts/input.v1.json), [processing](contracts/processing.v1.json), and [output](contracts/output.v1.json) contracts define source formats and consumer semantics. Row schemas are in `contracts/schemas/`.
- [Fixtures](synthetic/fixtures/rule_cases.v1.json) contain 51 independently specified cases, including thresholds ±1 second, holds, reopen cycles, invalid data, and daily measures across midnight.

Daily metrics are sparse UTC date/service/priority rows. Creation uses initial dimensions; final resolutions use dimensions at resolution; backlog uses historical day-end state and dimensions. A reopened incident has no final resolution until it is resolved again. SLA counts and averages describe final resolutions on that day; pending SLA outcomes are excluded from rate denominators, and an empty denominator produces `null`.

The evaluator in `dpe/rules.py` is a reference implementation checked against hand-authored results. Future dbt/Trino and Spark implementations must pass these same fixtures rather than deriving expected results from their own SQL.

## Recovery and operator evidence

One SQLite transaction commits the batch checkpoint, Bronze records, Silver results, both Gold outputs, and the active publication pointer. Failed attempts retain evidence, while consumers continue to see the prior committed publication. Multi-query consumers should use a single SQLite read transaction for a consistent publication.

Inspect `batches`, `bronze_records`, `silver_incidents`, `record_dispositions`, `publications`, and `attempts` with a SQLite client. Every Bronze record retains its source batch, file, row, and ingestion timestamp. Dispositions link back to this provenance. Publication reports include input/output counts, reconciliation, quarantine reasons, versions, hashes, elapsed/CPU time, process peak RSS, and raw archive paths. Peak RSS is a process lifetime high-water mark, not per-workload memory attribution.

To demonstrate rollback before publication:

```sh
python3 -m dpe run data/custom/initial --db data/recovery.sqlite
python3 -m dpe run data/custom/delta --db data/recovery.sqlite \
  --fail-at between_gold_writes
# Expected: nonzero exit; the initial publication remains active.
python3 -m dpe run data/custom/delta --db data/recovery.sqlite
```

Other failpoints are `after_ingestion`, `after_silver`, and `before_publish`. Tests also simulate a raw storage write failure. Delta processing fully recomputes from committed raw records; partition-targeted incremental optimization is deferred. Raw archives and publication history remain until explicitly deleted. Local archival retention and SQLite transactions do not validate Iceberg maintenance or distributed failure behavior.

## Record a local baseline

```sh
python3 -m dpe benchmark --workspace data/benchmark-new --repeats 3
```

This freezes one source batch and repeats full processing into fresh databases, verifies equal output checksums, and writes `evidence.json`. [Local acceptance criteria](benchmarks/acceptance.local.v1.json) were defined before the first measured run. OS caches are uncontrolled. Runtime and bytes/second are local development measurements; cloud cost fields remain `null`. Cloud capacity, cost limits, scale tests, and engine comparisons are deferred.

See [the execution record](docs/plan/04-local_build_execution.md) for current plan coverage and the next integration work.
