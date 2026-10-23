# Enterprise Data Processing POC

A runnable local foundation for the platform in [the requirements](docs/plan/01-enterprise_data_processing_poc_validation_plan.md) and [build plan](docs/plan/03-initial_platform_build_plan.md). It includes fictional ITSM data, our own business rules, a Python/SQLite reference harness, and an optional dbt/Trino/Iceberg processing path.

The local lakehouse uses RustFS object storage, a Nessie Iceberg REST catalog backed by PostgreSQL, Trino, and dbt. Azure, Airflow, and Spark are not deployed. Cloud performance, Azure storage compatibility, and economics remain unmeasured.

## Run the local lakehouse

With a container runtime running:

```sh
make local-up
make local-build
make local-verify
```

This starts the digest-pinned services, builds the pinned runner image, and verifies Iceberg mutations, dbt execution, 53 independent fixtures, a 1,000-incident initial batch, a 1,100-incident delta result, publication failures, replay, and full-rebuild equivalence. Evidence is retained under `data/lake-check/`. Verification requires a fresh workspace; repeat it with `make local-verify LOCAL_WORKSPACE=data/lake-check-next`.

Read a snapshot-pinned Gold product:

```sh
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-query --workspace data/lake-check/platform \
  --dataset incident_resolution --limit 5
```

Use `make local-down` to stop the services while retaining storage volumes. On this Mac, the dedicated runtime is Colima profile `dpe`; use `colima start --profile dpe` to start it and `colima stop --profile dpe` to release its CPU/memory resources. See the [local lakehouse guide](docs/guides/local_lakehouse.md) for setup, host execution, persistence checks, and individual batch commands.

## Run targeted incremental processing

`lake-run` defaults to targeted execution. It validates and stores each incoming delivery, finds changed/time-dependent incidents, reads their bucketed histories, and rebuilds affected daily groups. Unchanged contributors remain included in group aggregates. V1 rules and Gold contracts are unchanged.

```sh
# Source generation remains independent of the execution strategy.
python3 -m dpe generate --out data/incremental-source

docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-run data/incremental-source/initial --workspace data/incremental-platform
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-run data/incremental-source/delta --workspace data/incremental-platform

# Full SQL rebuild remains available in a separate workspace.
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-run data/incremental-source/initial --workspace data/full-platform --strategy full
```

For the complete edge-case, failure/retry, and two-workload comparison suite:

```sh
make local-incremental-verify LOCAL_WORKSPACE=data/incremental-next
# After restarting services and waiting for health:
make local-incremental-restart-check LOCAL_WORKSPACE=data/incremental-next
```

Use a fresh verification workspace. The suite freezes [numeric acceptance criteria](benchmarks/acceptance.incremental.v1.json), compares both products against full SQL and Python rebuilds, and records physical Trino reads/writes by stage. The Make/CI wrapper also captures actual Iceberg scan-file counters, cache/external bytes, complete written-file inventories, and unchanged contributors to affected daily groups. It measures a mostly resolved workload with a 1% delta and an open-heavy workload. Required validation and publication copying remain included in total IO/runtime.

Published tables are immutable because this local REST catalog cannot read an earlier snapshot after an in-place mutation. Unchanged Gold/contribution rows are copied into isolated publication tables; that storage and IO cost is reported explicitly. See [decision 003](docs/decisions/003-targeted-immutable-publications.md).

A cutoff-only delta contains zero-byte JSONL files with `rows: 0` for both incident datasets and unchanged complete reference snapshots. `BatchWriter.finish()` creates those files automatically. Its manifest still names the exact parent and the new cutoff. Reference mutations, code/policy changes, and reporting-start changes require a new version/workspace.

Attempts, candidate IDs/reasons, affected groups, dispositions, complete-population quality counts, delivery snapshots, and both Gold snapshots are retained under the workspace. Failpoints are `after_input_persistence`, `during_selective_writes`, `between_gold_writes`, and `before_publish`. Checkpoints advance with the active publication. Full historical raw-byte integrity validation is an explicit operation:

```sh
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-audit --workspace data/incremental-platform
```

## Run it

The reference harness runs from this directory using Python 3.9+ on macOS/Linux. It needs no third-party Python packages, cloud account, or containers. Commands run from the source checkout; wheel/release packaging is outside this iteration.

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

Generation is deterministic for a fixed configuration and version, writes partitioned files, and refuses to overwrite an existing destination. Each batch has a manifest with record counts, actual bytes, hashes, source-time ranges, and configuration. Synthetic defect expectations live in a separate `expected.v1.json` sidecar consumed only by validation. The delta names its exact parent. Its late snapshot correction is effective before the initial cutoff, while its reopen/reassignment occurs afterward.

Use `--profile smoke|small|medium|large` or override with `--count`. [The default configuration](synthetic/configs/default.json) controls priorities, scenario weights, service/group skew, comments per incident, chunk size, and defect rates. Supply another JSON file with `--config`. `baseline` produces valid data; `faults` introduces at most one declared fault per incident:

```sh
python3 -m dpe generate --out data/faults --mode faults
python3 -m dpe run data/faults/initial --db data/faults.sqlite
```

Fault scenarios cover duplicates, conflicting snapshot versions, missing required references, invalid priorities, and malformed timestamps. Publication follows the [versioned quality policy](rules/quality_policy.v1.json), which permits up to 10% quarantined incident identities by default. Unknown defects are handled by the same policy. Use `--quality-policy` with `run` or `lake-run` to supply a stricter policy. Schema-invalid incident records are preserved for quarantine; manifest integrity failures and invalid reference data fail the batch. Synthetic assertions compare actual quarantine rules and duplicate dispositions separately. Every raw record has a reconciliation disposition.

The generator streams records into chunks. The reference and full-rebuild paths load complete histories into Python memory; targeted execution loads incoming records and candidate IDs, with histories processed in Trino. Complete output validation still reads all Gold rows. Use Smoke-sized inputs for development; Medium/Large profiles are available for source generation and eventual distributed processing, not a claim of local processing capacity.

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
- [Fixtures](synthetic/fixtures/rule_cases.v1.json) contain 53 independently specified cases, including thresholds ±1 second, holds, reopen cycles, invalid data, reassignment, late corrections, and daily measures across midnight.

Daily metrics are sparse UTC date/service/priority rows. Creation uses initial dimensions; final resolutions use dimensions at resolution; backlog uses historical day-end state and dimensions. A reopened incident has no final resolution until it is resolved again. SLA counts and averages describe final resolutions on that day; pending SLA outcomes are excluded from rate denominators, and an empty denominator produces `null`.

The evaluator in `dpe/rules.py` is a reference implementation checked against hand-authored results. The dbt/Trino implementation passes these fixtures; future engines must use the same independent expectations.

## Recovery and operator evidence

One SQLite transaction commits the batch checkpoint, Bronze records, Silver results, both Gold outputs, and the active publication pointer. Failed attempts retain evidence, while consumers continue to see the prior committed publication. Multi-query consumers should use a single SQLite read transaction for a consistent publication.

Inspect `batches`, `bronze_records`, `silver_incidents`, `record_dispositions`, `publications`, and `attempts` with a SQLite client. Every Bronze record retains its source batch, file, row, and ingestion timestamp. Dispositions link back to this provenance. Attempts are persisted before source validation/API access; evidence files survive metadata-store failures. Publication reports include input/output counts, reconciliation, quarantine reasons, versions, hashes, stage timing, elapsed/CPU time, process peak RSS, and raw archive paths. Peak RSS is a process lifetime high-water mark, not per-workload memory attribution.

To demonstrate rollback before publication:

```sh
python3 -m dpe run data/custom/initial --db data/recovery.sqlite
python3 -m dpe run data/custom/delta --db data/recovery.sqlite \
  --fail-at between_gold_writes
# Expected: nonzero exit; the initial publication remains active.
python3 -m dpe run data/custom/delta --db data/recovery.sqlite
```

Other failpoints are `after_ingestion`, `after_silver`, and `before_publish`. Tests also simulate a raw storage write failure. The SQLite reference path fully recomputes from committed raw records; the lakehouse also supports targeted processing as described above. Raw archives and publication history remain until explicitly deleted. Local archival retention and SQLite transactions do not validate Iceberg maintenance or distributed failure behavior.

## Record a local baseline

```sh
python3 -m dpe benchmark --workspace data/benchmark-new --repeats 3
```

This freezes the acceptance configuration before execution, repeats full processing into fresh databases, verifies equal output checksums, and writes `evidence.json`. Failed required runtime criteria preserve evidence and return a nonzero exit. [Local acceptance criteria](benchmarks/acceptance.local.v1.json) apply to the reference harness. OS caches are uncontrolled. Runtime and bytes/second are local development measurements; cloud cost fields remain `null`. Cloud capacity, cost limits, scale tests, and engine comparisons are deferred.

See [the initial execution record](docs/plan/04-local_build_execution.md) and [the local improvement execution record](docs/plan/06-local_lakehouse_execution.md) for plan coverage and remaining work.

Targeted processing is tracked in [07 — Targeted Incremental Processing Plan](docs/plan/07-targeted_incremental_processing_plan.md) and [08 — Targeted Incremental Execution](docs/plan/08-targeted_incremental_execution.md).
