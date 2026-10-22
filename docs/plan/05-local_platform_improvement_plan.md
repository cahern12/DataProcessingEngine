# Local Platform Improvement Plan

Execution status: phases 1–3 are implemented and verified locally. See [06 — Local Lakehouse Execution](06-local_lakehouse_execution.md) for evidence and the remaining phases.

This plan follows [04 — Local Build Execution Record](04-local_build_execution.md) and advances the architecture in [01 — POC Requirements](01-enterprise_data_processing_poc_validation_plan.md). It keeps execution local while moving from the Python/SQLite development harness to a verified dbt/Trino/Iceberg processing path.

The next milestone is **one reproducible local Bronze → Silver → Gold pipeline on the intended processing stack that passes the existing independent business-rule fixtures**. Retain the Python evaluator as a correctness reference and the current consumer contracts as the starting point.

Complete the phases in order. The immediate implementation scope is phases 1–3. Phases 4–6 extend that working path with incremental processing, more representative workloads, and operator tooling. Add automated regression checks with each phase; phase 6 completes the integration workflow.

## 1. Close the quality-control and execution-evidence gaps

Make the current harness dependable before introducing additional services.

### Record failures from the start of an attempt

Currently, batch loading and validation occur before the attempt is persisted in `dpe/pipeline.py`. Invalid manifests, source checksum failures, and API failures can therefore terminate without an attempt record.

- Create the attempt identity before source validation or API access.
- Record source location and the current stage even when the batch ID or manifest checksum is not yet available.
- Retain success/failure status, error details, timestamps, and any available source identity for every attempt.
- Define a local evidence-file fallback for failures that prevent access to the metadata database itself.

### Enforce benchmark acceptance criteria

Currently, `dpe/validation.py` records `smoke_runtime_passed`, but a failed runtime criterion does not make the benchmark command exit unsuccessfully.

- Preserve the benchmark evidence before returning a nonzero exit status when a required criterion fails.
- Distinguish passed, failed, and unevaluated criteria explicitly.
- Capture the exact acceptance configuration at experiment start and use that same version throughout the run.
- Test threshold failures using controlled timing/results so regression tests do not depend on machine speed.

### Separate synthetic truth from processing policy

Currently, publication compares quarantine results with defects declared in the synthetic source manifest. That is useful for a controlled test, but normal processing should be governed by a quality policy that can handle previously unseen bad records.

- Move injected-defect expectations into a synthetic validation sidecar consumed by the test harness.
- Keep schema, checksums, source identity, and delivery metadata in the normal input contract.
- Define a versioned quality policy for which errors quarantine records, which fail a batch, and which rejection thresholds prevent publication.
- Have the validation harness compare actual dispositions and rule IDs with the independent synthetic expectations after processing.
- Preserve reconciliation of every raw record regardless of whether it was an intentional defect.

**Exit condition:** source/API failures leave inspectable attempt evidence; required benchmark failures produce a nonzero exit; quality policy controls publication without reading synthetic expected outcomes; regression tests cover all three behaviors.

## 2. Build a reproducible local storage, catalog, and engine stack

Execute the local compatibility spike from step 5 of [03 — Initial Build Plan](03-initial_platform_build_plan.md).

1. Establish a local container runtime and document startup, shutdown, resource requirements, and persistence locations.
2. Select a local object store, an Iceberg REST catalog and its required persistent backend, Trino, and a dbt execution image/environment.
3. Pin the selected component and adapter versions after verifying compatibility. Record the exact choices and configuration in an architecture decision.
4. Add a Compose definition, persistent volumes, readiness checks, and repeatable initialization for storage locations, catalog namespaces, and required table schemas.
5. Prove create, insert, read, update/merge, and snapshot inspection through Trino against the chosen Iceberg catalog/storage combination.
6. Execute one dbt model and its quality check through the Trino adapter.
7. Restart services and verify that the catalog, table metadata, and stored data remain usable.

Use a small compatibility dataset for this phase. A startup command succeeding does not satisfy the exit condition; the read/write/restart checks must produce saved evidence.

**Exit condition:** a fresh checkout can start the pinned stack and run an automated compatibility check. Required writes/updates work, table state survives restarts, and the chosen versions and resource profile are documented.

## 3. Port one complete processing path and verify equivalence

Use the existing Smoke batch and independent fixtures before increasing volume or changing business rules.

| Layer | Implementation work | Required checks |
|---|---|---|
| Raw landing | Preserve exact source files and mock API page payloads in the local object store | Manifest checksums, source identity, safe retry |
| Bronze | Load source-faithful Iceberg tables with batch/file/row provenance | Source-to-Bronze counts and explicit handling of unparseable payloads |
| Silver | Implement normalization, version selection, references, event state, holds, and SLA calculations in dbt models | BR-001 through BR-008 behavior, quarantine reasons, and record dispositions |
| Gold | Build `incident_resolution` and `service_daily_metrics` behind the output contract | Schema, grain, key uniqueness, daily measures, and fixture equality |

Keep test expectations independent of SQL. For valid fixture cases, compare every contracted incident field with its hand-authored expected value. For invalid cases, compare expected rule IDs and quarantine outcomes. Expand exact daily-metric expectations beyond the existing cross-midnight case to include reassignment, reopening, and late corrections.

Normalize comparison details explicitly: timestamp precision/timezone, nulls, numeric precision, array order, and row ordering. Apply tolerances only where the output contract permits them; report differences by dataset, key, and field.

### Preserve publication consistency across Gold tables

SQLite currently publishes both products in one transaction. Do not carry that assumption into independent Iceberg table commits.

- Create an immutable publication record containing the validated snapshot IDs for every required Gold table, the cutoff, and transformation/rule versions.
- Implement a consumer read path that resolves one publication and reads those snapshots consistently.
- Activate the new publication only after every required write and validation succeeds.
- Inject a failure between the two Gold writes and verify that the previous publication remains consumable.
- Retry the interrupted batch and verify that it does not create duplicate logical outputs.

**Exit condition:** the intended local stack processes the Smoke batch end to end, all existing fixture cases pass through the SQL implementation, expanded daily fixtures match, both Gold contracts remain stable, and partial writes cannot become an active publication.

**Milestone reached:** a fixture-verified local dbt/Trino/Iceberg pipeline. This is the completion target for the immediate implementation scope.

## 4. Implement incremental processing with a full-rebuild comparison

The current harness loads complete committed histories into memory and fully recomputes outputs for each delta. Keep that behavior as a comparison baseline while adding a targeted path to the SQL implementation.

1. Track committed manifests and source identities/versions with explicit checkpoints and retry semantics.
2. Determine affected incident IDs from new snapshots, events, and relevant reference changes. A late event must not be missed because its event time precedes the delivery checkpoint.
3. Reconstruct complete histories for affected incidents, including earlier records needed for creation priority, holds, and reopen cycles.
4. Identify all affected daily partitions and dimension keys. Include the previous grouping when a correction changes a date, service, or priority; a reopen may change backlog across multiple days.
5. Update affected Silver/Gold results and publish them through the same validated publication mechanism.
6. Rebuild from the same complete raw input at the same cutoff and compare both output products with the incremental result.

Measure records/bytes scanned, affected incidents/partitions, runtime, and peak resource use. Separate work avoided from work merely moved to another stage.

**Exit condition:** initial plus delta processing equals full recomputation; replay remains idempotent; late corrections remove obsolete contributions; evidence shows which inputs and partitions were processed.

## 5. Expand synthetic histories and business complexity

Once the first SQL path is correct, make workloads more representative without changing the established v1 baseline silently.

### Generate sequences of enterprise-style batches

- Support configurable 30-, 60-, and 90-day batch sequences with deterministic identities and parent/checkpoint relationships.
- Add bursts, quiet days, source outages, recovery/backfill deliveries, and out-of-order arrival.
- Add corrections delivered weeks after their source/event time, repeated pages/files, and reference updates.
- Include compatible and incompatible schema-change scenarios with explicit expected contract outcomes.
- Vary skew, event fan-out, priority distribution, and dimension cardinality independently of incident count.

### Introduce additional versioned business rules

| Rule extension | Processing behavior to exercise |
|---|---|
| Business-hour calendars and holidays | Timezone-aware intervals and calendar joins, including daylight-saving boundaries |
| Effective-dated SLA policies | Selection of the correct historical policy and reproducible rule versions |
| Assignment history | Effective-dated relationships, ownership changes, and historical attribution |
| Different service support policies | Additional joins and conditional calculations with independently known outcomes |

Specify whether each new policy applies at creation, at event time, or retrospectively before implementing it. Introduce a new rule/output contract version when semantics change. Author expected-result fixtures for every new rule, boundary, and failure case before writing its SQL transformation.

**Exit condition:** multi-batch scenarios regenerate deterministically, declared defects reconcile independently, new rules have hand-checked fixtures, and both full and incremental paths produce equivalent contracted outputs.

## 6. Improve operator commands, stage telemetry, and continuous verification

Build on the execution evidence captured in phase 1 and the component/query telemetry from the local stack.

The following commands are proposed interfaces, not commands implemented by this document:

| Proposed command | Purpose |
|---|---|
| `python3 -m dpe status` | Show active publication, checkpoint, latest outcome, and service readiness |
| `python3 -m dpe runs` | Inspect attempts, versions, stage status, and durations |
| `python3 -m dpe inspect-quarantine` | Filter rejected records by batch, dataset, rule, and source location |
| `python3 -m dpe compare-runs` | Compare outputs, data/configuration versions, resource profiles, and performance |

Capture timing separately for ingestion, Bronze load, Silver transforms, Gold transforms, validation, and publication. Link pipeline attempts to dbt invocation/model IDs, Trino query IDs, table snapshots, and the active publication. Preserve sufficient evidence to explain both a slow run and a failed run.

Add continuous integration that runs:

- Contract and business-rule fixture checks on every relevant change.
- Source-integrity, replay, quality-policy, and failure/recovery tests.
- A fresh local-stack compatibility check and a small dbt/Trino end-to-end test.
- Incremental/full-rebuild comparisons and regression checks for publication consistency.
- Benchmark acceptance checks on a controlled runner; keep uncontrolled machine timings informational.

**Exit condition:** an operator can inspect a failure without manually joining metadata tables, runs can be compared with their input/configuration differences visible, and automated checks reject correctness, recovery, and required acceptance failures.

## Evidence and scope boundaries

For each phase, record the implementation revision, input manifest hashes, rules/contracts, component versions, resource settings, validation results, and unresolved findings. Keep generated bulk data out of Git; commit reproducible configuration, fixtures, validation logic, and a concise execution record in the numbered planning sequence.

These phases advance local correctness, interoperability, recovery, and operability evidence. Azure ADLS compatibility, AKS scaling, fully allocated cloud economics, and the Trino/Spark comparison remain required by the original POC and will need their own measured runs. The local milestone does not by itself justify a production architecture recommendation.

Implementation references for phase 2: [Trino Iceberg connector documentation](https://trino.io/docs/current/connector/iceberg.html) and [dbt Trino setup documentation](https://docs.getdbt.com/docs/local/connect-data-platform/trino-setup). Record the actual tested versions when implementing the compatibility spike.
