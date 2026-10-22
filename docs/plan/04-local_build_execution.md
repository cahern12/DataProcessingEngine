# Local Build Execution Record

Date: 2026-10-22. Scope: execute [03 — Initial Platform Build Plan](03-initial_platform_build_plan.md), with the user's subsequent instruction to keep execution local for now.

The implementation now provides a runnable local source → Bronze → Silver → Gold development loop. Python and SQLite implement this local path. The intended Airflow/dbt/Trino/Iceberg architecture has not yet been deployed or validated; [decision 001](../decisions/001-local_execution_harness.md) records that boundary.

## 1. Use case, acceptance criteria, and contracts

Established fictional ITSM incidents as the first domain. Versioned input, processing, and output contracts specify source schemas, execution metadata, quality gates, output grain, publication consistency, and daily measure semantics.

The default source period starts at `2026-01-01T00:00:00Z`; the initial cutoff is `2026-01-08T00:00:00Z` and delta cutoff is `2026-01-09T00:00:00Z`. The local acceptance register sets zero fixture differences, zero duplicate Gold keys, balanced reconciliation, a 60-second Smoke runtime limit, and a 60-second recovery limit. Cloud runtime/scale/cost limits remain deferred.

## 2. Business rules and independent fixtures

Implemented BR-001 through BR-008 as versioned definitions plus a Python reference evaluator. Defined our own synthetic SLA targets, canonical values, state transitions, hold pairing, final-resolution semantics, historical dimensions, and late-correction behavior.

Added 51 hand-authored expected-result fixtures. Additional behavior tests verify historical backlog reassignment and a correction that moves creation to an earlier reporting date. These fixtures are the comparison baseline for a future SQL implementation.

## 3. Reproducible synthetic sources

Implemented six source datasets in CSV/JSON Lines, chunked generation, configurable distributions/skew/event fan-out, full/delta batches, and immutable manifests. The generator supports Smoke/Small/Medium/Large source sizes and custom counts. Separate baseline/fault modes distinguish valid source histories from deliberate defects.

Added a read-only paginated localhost API for incident events. Ingestion verifies checksums, record counts, schemas, API batch identity, and API/file equivalence. Exact source bytes and API page payloads are archived, and accepted/rejected records remain traceable to source batch/file/row.

## 4. Local processing, publication, and recovery

Implemented persistent Bronze records, Silver incident results, and both Gold products. Consumers query `gold_incident_resolution` and `gold_service_daily_metrics` without knowing source paths. Validation and publication occur within the same SQLite transaction.

Committed batches are immutable. Retries have new attempt IDs but retain their logical run/publication identity. A delta must follow the active parent. Replay of older committed work does not move the active publication backward. Failures during raw archival or at the four pipeline failpoints are recoverable without exposing partial Gold outputs.

## 5. Verification and local evidence

Verified with:

- `python3 -m dpe check-fixtures` — all 51 expected-result fixtures pass.
- `python3 -m unittest discover -s tests -v` — 17 tests pass, including live loopback API pagination, checksum rejection, source-path confinement, declared faults, read-only consumption, replay, parent lineage, full/delta equivalence, and recovery.
- `python3 -m dpe demo` — initial publication has 1,000 unique incidents; the delta publication has 1,100. Source records reconcile, including repeated deliveries.
- `python3 -m dpe benchmark --workspace data/benchmark-smoke --repeats 3` — repeated local Smoke runs produce identical logical output checksums. Machine-specific timing/resource evidence is retained in the generated report, not treated as a distributed benchmark.

Generated evidence is ignored by Git. Commands and consumer examples are in the [README](../../README.md). No cloud cost is inferred from local execution.

## 6. Coverage against 03 and remaining integration work

| Original step | Current status |
|---|---|
| 1 — Scope and acceptance | Defined for local development; cloud benchmark limits remain open |
| 2 — Repository and contracts | Implemented; runtime modules are grouped in `dpe/` |
| 3 — Rules and fixtures | Implemented and verified locally |
| 4 — Synthetic generation | Implemented and verified with Smoke batches, deltas, and mock API |
| 5 — Storage/catalog compatibility | Pending local container runtime and a concrete, pinned Trino/dbt/catalog stack |
| 6 — Azure environment | Deferred by the local-only scope |
| 7 — Complete pipeline | Local equivalent implemented; Iceberg writes and Airflow/dbt execution remain pending |
| 8 — Recovery | Local replay, recomputation, transaction rollback, and raw storage failures verified; distributed failures/maintenance remain pending |
| 9 — Scale/engine economics | Local Smoke baseline only; distributed scale, Spark comparison, and cloud costs remain pending |
| 10 — POC decision | Pending architecture evidence; no Proceed/Do Not Proceed recommendation yet |

Next, execute the local version of the compatibility spike with Trino, dbt, an Iceberg REST catalog, and a clearly identified local storage substitute. Use the existing source batches and independent fixtures, then port the local transformations and publication contract to that stack. Complete the original Azure storage validation and economic gates when cloud execution returns to scope.
