# Initial Platform Build Plan

This document turns the [POC requirements](01-enterprise_data_processing_poc_validation_plan.md) and [architecture sketch](02-ServiceNow%20Data%20Processing%20Engine%20Architecture.png) into a chronological build sequence. Complete the steps in order; each step has an exit condition that makes the next step possible.

The first milestone is a reproducible batch pipeline that processes **our own synthetic ITSM data and business rules** through Bronze → Silver → Gold on Azure, validates the results, and records execution and cost evidence. We will start with Airflow → dbt → Trino → Iceberg / ADLS, then compare selected workloads with Spark once the first path works.

Dataset sizes, rules, and implementation choices below are proposed POC defaults. Record changes in version control before using them in measured runs. The synthetic business rules are ours; they do not claim to reproduce ServiceNow product behavior or an existing organization's policies.

## 1. Define the first use case and its acceptance criteria

Use **IT service incident processing** as the first domain. It fits the architecture sketch and gives us joins, changing records, event histories, deduplication, time calculations, and aggregations without needing production data or a live ServiceNow connection.

The first consumer-facing data products will be:

| Gold dataset | Grain | Purpose |
|---|---|---|
| `incident_resolution` | One row per incident at the batch cutoff | Current state, assignment, response time, resolution time, and SLA outcome |
| `service_daily_metrics` | One row per UTC date, service, and priority | Created/resolved counts, end-of-day backlog, SLA pass/fail counts, and average resolution time |

Before implementation, record:

- A fixed UTC reporting period and batch cutoff; never use the machine's current clock to calculate expected results.
- Dataset and rule versions, logical output names, and the initial output schema version (`v1`).
- Correctness criteria: exact expected rows for hand-authored fixtures, no duplicate Gold keys, no unexplained reconciliation differences, and no invalid records silently discarded.
- Recovery criteria: rerunning a committed batch leaves the logical outputs unchanged; an interrupted batch can be replayed without duplicate publication.
- The benchmark matrix and numeric runtime, recovery, scale, and cost limits. Hardware-dependent limits can be finalized after environment sizing, but must be frozen before benchmark execution.
- Which broader requirements are deferred: streaming, CDC, production integrations, self-service, and production disaster recovery remain outside the first build.

**Exit condition:** the first data products, scope, correctness expectations, and acceptance-criteria register are committed. Any unresolved benchmark limits have a named decision and must be closed in step 6.

## 2. Establish the repository structure and contracts

Create the working structure before writing the pipeline. These directories are proposed; this plan does not create them yet.

```text
contracts/                 # Versioned input, internal processing, and output contracts
rules/                     # Business-rule definitions and SLA reference data
synthetic/                 # Generator, scenarios, and hand-authored expected results
ingestion/                 # Batch file and mock API ingestion
transformations/dbt/       # Bronze, Silver, and Gold models and quality checks
orchestration/             # Airflow DAGs and batch publication workflow
infra/                     # Reproducible Azure and runtime configuration
validation/                # Reconciliation, expected-result, and recovery checks
benchmarks/                # Workloads, run specifications, and cost calculations
docs/decisions/            # Architecture choices and compatibility findings
```

Define three contracts in machine-readable files:

| Contract | Initial contents |
|---|---|
| Input | Source/dataset identity, schema, business keys, extract mode, source timestamps, format, classification, retention, batch manifest, and schema-change policy |
| Processing | Rule/model versions, dependencies, engine/resource profile, batch cutoff, quality gates, retry/replay behavior, and required telemetry |
| Output | Logical name, grain, schema, semantics, version, owner, freshness target, quality expectations, and SQL access interface |

Use a shared execution envelope containing `run_id`, `batch_id`, dataset version, generator seed, rule version, transformation revision, engine/version, input manifest checksum, cutoff, and execution timestamps. A retry gets a new attempt identifier while retaining the same logical batch identity.

Keep generated bulk data and secrets out of Git. Commit generator code, configurations, small fixtures, rule definitions, contracts, and dependency locks. Document the commands for generating data and running validation as those commands are implemented.

**Exit condition:** input and output contracts are concrete enough to generate records and assert expected output columns, keys, and meanings.

## 3. Write our business rules and small expected-result fixtures

Define business behavior before generating large datasets. Keep rule IDs stable so a result can be traced to its rule and test scenario.

| Rule | Initial POC behavior | Complexity exercised |
|---|---|---|
| `BR-001` Normalize records | Trim identifiers; map defined source status/priority variants to canonical values; convert offset-bearing timestamps to UTC. Quarantine unknown values and timestamps without an accepted timezone. | Parsing, casts, mapping joins |
| `BR-002` Select record versions | Collapse identical deliveries of the same source record version. For incident snapshots, select the latest `source_updated_at` at or before the cutoff, then the highest `source_version`. Quarantine conflicting records with the same key and version. | Deduplication and window functions |
| `BR-003` Validate relationships | Require a valid service and assignment group. Use an explicit `unknown_user` member for missing optional users; retain a quality flag. Quarantine missing required references. | Multi-table joins and referential checks |
| `BR-004` Reconstruct incident state | Order events by event time and source sequence; use only events at or before the cutoff. Reject impossible transitions and conflicting event identities. Define reopened incidents as unresolved until a subsequent resolution. | Event sequencing and state reconstruction |
| `BR-005` Calculate SLA time | Use elapsed UTC minutes on a 24×7 clock. First response is the first qualifying agent-response event. Resolution time is creation to the latest resolution of a currently resolved/closed incident, less valid non-overlapping on-hold intervals. A reopen resets that final resolution and does not pause elapsed time. | Interval arithmetic, windows, aggregation |
| `BR-006` Apply SLA targets | Use priority at creation: P1 response/resolution = 15/240 minutes; P2 = 60/480; P3 = 240/1,440; P4 = 480/4,320. Passing includes equality. Missing response/resolution is pending until its clock exceeds the target, then breached. | Reference joins and boundary conditions |
| `BR-007` Publish daily measures | Attribute created counts to creation date and resolved counts to the final qualifying resolution date as of the cutoff. Calculate backlog from state at each UTC day end; exclude still-pending SLA outcomes from pass-rate denominators. Publish counts alongside rates. | Historical state, date expansion, grouped metrics |
| `BR-008` Apply late corrections | Apply late records by source/event time and version. Recompute affected incidents and daily partitions, including old and new dimension/date keys when a correction moves a record. | Incremental merges and bounded reprocessing |

Store SLA targets as versioned reference data. Enumerate the accepted statuses, allowed state transitions, qualifying response events, and on-hold interval pairing rules in `rules/` before coding transformations. Define precision and rounding once; compare SLA thresholds using unrounded durations. Reconstruct creation-time priority and historical dimensions from versioned source records/events rather than assuming the latest snapshot represents the past.

Write small fixtures with **manually specified expected outputs**, including:

- An ordinary resolved incident and incidents exactly on either side of each SLA boundary.
- An unresolved incident, an incident with no response, and a reopened incident.
- Multiple on-hold periods, an open hold ending at the cutoff, and invalid overlapping intervals.
- Duplicate deliveries, conflicting versions, out-of-order events, and late corrections that change a previous day's result.
- Missing users, missing required references, invalid domains, malformed timestamps, and events after the cutoff.
- A resolution spanning midnight, a correction moving a service/date grouping, and a day with no eligible SLA denominator.

The expected-result fixtures must be independent of the production SQL. An additional small Python reference calculator can support larger comparisons, but should be checked against the same hand-calculated fixtures.

**Exit condition:** every business rule has an unambiguous definition and at least one expected-result scenario; boundary and failure cases are represented.

## 4. Build the deterministic synthetic data generator

Generate a coherent fictional organization, then its incidents and histories. Use invented identities and identifiers; no production extracts are required.

| Dataset | Key / relationship | Initial source representation |
|---|---|---|
| `users` | `user_id`; optional incident requester/agent references | CSV relational-style extract |
| `assignment_groups` | `group_id`; required incident assignment reference | CSV relational-style extract |
| `services` | `service_id`; required incident service reference | CSV relational-style extract |
| `incident_snapshots` | `incident_id` + `source_version`; references users, groups, services | JSON Lines application extract |
| `incident_events` | `event_id`; references incident and optional actor | Paginated local mock REST API, backed by generated JSON |
| `sla_targets` | Rule version + creation priority | Versioned reference CSV |

Generate valid base relationships first, then inject controlled defects. The generator must accept a seed, fixed reporting period, size profile, scenario mix, distribution settings, and defect rates. Include tunable event fan-out, a heavily used service/group for join skew, priority imbalance, and high-cardinality incident IDs.

Use separate modes for a valid baseline and deliberate fault scenarios. Fault manifests must identify injected record IDs and expected dispositions so intentional defects can be distinguished from pipeline bugs. Do not infer generator truth solely from transformation results.

Start with these profiles; event counts depend on configured fan-out, while snapshot counts also include configured revisions and duplicates:

| Profile | Unique incidents | Use |
|---|---:|---|
| Fixture | Approximately 20–50 hand-authored cases | Exact business-rule verification |
| Smoke | 1,000 | Developer iteration and the first complete pipeline |
| Small | 100,000 | First measured runs |
| Medium | 1,000,000 | Joins, incremental processing, and recovery |
| Large | 10,000,000 | Scale, skew, resource pressure, and selected recovery runs |

For each generated batch, emit a manifest containing seed/configuration, schema and generator versions, row counts by dataset, file checksums, actual bytes, event-time ranges, and injected defects. Do not equate row-count profiles with fixed TB sizes. Support partitioned/chunked generation so larger profiles do not require all records in memory.

Generate an initial full batch plus a follow-up delta containing new incidents, updated snapshots, reopened incidents, late events, and duplicate deliveries. Regeneration with identical inputs must produce identical logical records and stable input checksums.

**Exit condition:** the Smoke profile and its delta can be regenerated, validated against their manifests, and served through the file and mock API interfaces.

## 5. Prove storage, catalog, and engine compatibility

Before building transformations, run a small integration spike against the intended Azure storage path:

1. Select and pin a concrete Iceberg REST catalog implementation, its metadata database, and compatible runtime/connector versions. Record the decision and required Azure authentication configuration.
2. Establish ADLS Gen2 access and catalog connectivity with explicit identities and permissions.
3. Create, write, read, update, and inspect a tiny Iceberg table through Trino. Verify the write/update operations needed for the planned incremental strategy.
4. Run one dbt model and one quality test through the Trino adapter against that table.
5. Inspect the resulting Parquet files, Iceberg metadata, and catalog registration; verify that table state survives a service restart.
6. Record capability gaps before relying on them in the pipeline. Validate Spark access to the same catalog/storage when preparing the later engine comparison.

Local containers may support development. Local timings and storage substitutes are development evidence; Azure architecture and cost conclusions require Azure runs.

**Exit condition:** the pinned Trino/dbt/catalog/storage combination supports the first pipeline's reads, writes, and update strategy, with a repeatable smoke check.

## 6. Provision the smallest observable Azure environment

Use infrastructure as code to provision a dedicated POC resource group, ADLS Gen2, AKS, the selected catalog and metadata database, Trino, Airflow, and monitoring. Run dbt as a versioned execution task. Start with enough capacity for the Smoke profile and size up deliberately.

- Separate raw landing, Bronze, Silver, Gold, quarantine, and evidence locations. Preserve original source files/API page payloads in raw landing; Bronze provides a source-faithful queryable representation with ingestion metadata.
- Use managed/workload identities where supported, a secret store for remaining credentials, and explicit storage, catalog, and consumer permissions.
- Tag resources for the POC and capture resource IDs, capacity, uptime, pricing basis, currency, and allocation policy. Define budget alerts and teardown/pause procedures.
- Capture pipeline/task/query identifiers, timestamps, row and byte counts, failures, CPU/memory, file statistics, and catalog/storage overhead from the first run.
- Finalize the numeric benchmark thresholds from step 1 against the documented initial capacity. Freeze the acceptance criteria and benchmark configuration before measured runs.

**Exit condition:** infrastructure can be recreated, services can access only their intended resources, telemetry is available, and a cost-attribution method is documented. Spark capacity is added when its comparison is ready.

## 7. Build and validate one complete batch pipeline

Use the Smoke profile first. Implement the path in this order:

1. **Ingest:** validate the manifest, read files and mock API pages, preserve original payloads, and register the logical batch. Handle pagination and safe retries without duplicating deliveries.
2. **Bronze:** load source-faithful Iceberg tables with batch ID, source file/page, record identity, source timestamps, and ingestion timestamp. Retain invalid payloads with their locations and error reasons.
3. **Silver:** normalize, deduplicate, validate relationships, reconstruct event histories, and calculate incident-level measures. Quarantine invalid records with rule IDs and enough provenance to investigate or replay them.
4. **Gold:** publish the two versioned data products from step 1 using stable logical SQL interfaces.
5. **Validate:** run the fixture suite, schema/key/domain checks, reference checks, and source-to-target reconciliation. Account for accepted, duplicate, rejected, pending/deferred, and unchanged records by processing stage; document transformations that change grain.
6. **Publish:** expose a validated batch through the consumer interface and record its output snapshot IDs and contract version. Keep consumers on the previous validated publication if any required table or check fails.

Iceberg commits are table-scoped. Define a batch publication manifest containing the validated snapshots for all required Gold tables, and a consumer access mechanism that resolves one publication consistently. Do not assume separate table commits provide atomic publication across the data products. Prove this mechanism with a failure between Gold writes.

An initial manually invoked run is useful for debugging. The milestone is complete when an Airflow DAG executes the full path, including validation, failure handling, publication, and the run evidence report.

**Exit condition:** one command or DAG trigger takes a reproducible Smoke batch from source to validated Gold, and a consumer can query it without knowing physical paths or the processing engine. This is the **first working platform milestone**.

## 8. Add incremental processing, replay, and recovery

Process the generated delta only after the initial batch is correct.

1. Define a manifest/source-version checkpoint strategy; do not use ingestion time alone to decide whether a source record is new.
2. Apply late events and corrections, recompute affected outputs, and compare the incremental result with a full rebuild at the same cutoff.
3. Replay the identical batch and verify stable logical rows, measures, and published batch identity. Physical file/snapshot identity need not remain unchanged.
4. Interrupt ingestion, a Silver write, and publication between Gold writes. Restart each run and prove that failed or partial work is not exposed to consumers.
5. Test worker loss and controlled catalog/storage-access failures. Capture recovery time, attempts, operator actions, and any cleanup required.
6. Document raw replay, quarantine correction, retention, compaction, and snapshot-expiration procedures. Preserve snapshots needed by active publications and recovery checks.

**Exit condition:** delta processing matches full recomputation, retries do not duplicate published results, and representative failures have recorded recovery evidence. Repeat the selected recovery scenarios at Medium/Large scale in step 9.

## 9. Scale the workload and compare engines selectively

Once correctness and recovery work, run Small → Medium → Large profiles. Use controlled scan/filter, multi-table join, aggregation/window, full-pipeline, and failure/recovery workloads from the requirements.

- Freeze the input manifest, cutoff, rule/model revisions, engine versions, resources, cache policy, and concurrency for each experiment. Separate warm-up from measured runs and repeat measured runs to report variability.
- Record runtime, throughput, CPU/memory, bytes read/written, file counts/sizes, quality results, retries, manual intervention, and tuning changes.
- Calculate marginal execution cost and fully allocated cost separately. Include standing AKS capacity, catalog/database, orchestration, monitoring, storage, and maintenance overhead; document allocation assumptions and estimated versus billed values. Never count the same shared cost twice.
- Add Spark for selected joins, windows, pipeline writes, and recovery workloads, including a baseline comparison even if Trino meets its targets. Use equivalent input snapshots, rules, outputs, and documented resource/cost envelopes.
- Give each engine isolated output tables/namespaces while comparing results; avoid uncontrolled concurrent writes to the same tables. Validate normalized row/measure equality and consumption through the same output contract.
- Capture a current-state reference only if a runnable equivalent exists. Otherwise document that comparison as unavailable.

**Exit condition:** benchmark results are reproducible, judged against predefined limits, and sufficient to explain whether a second engine provides value relative to its operational cost.

## 10. Assemble the POC evidence and decide the next build

Link every must-have gate to evidence rather than treating deployment as completion:

| Gate | Required evidence |
|---|---|
| G1 — End-to-end processing | Source manifests, completed Airflow runs, and Bronze/Silver/Gold outputs |
| G2 — Processing abstraction | Stable output contracts, a consumer SQL example, and consistent publication behavior |
| G3 — Correctness and trust | Rule fixtures, quality results, quarantine records, and reconciliation reports |
| G4 — Repeatability and recovery | Regeneration/replay checks, incremental/full equivalence, and failure-test reports |
| G5 — Performance and scale | Small/Medium/Large results, resource measurements, and scaling analysis |
| G6 — Transparent economics | Marginal and allocated workload costs, standing cost, and attribution assumptions |
| G7 — Technology fit | Trino/Spark findings, compatibility decisions, tuning/operator effort, and remaining risks |

Conclude with **Proceed**, **Proceed with Changes**, or **Do Not Proceed**, supported by the technical, operational, economic, and architectural evidence. Choose production components from those findings; the POC does not commit us to operating both engines.

The immediate implementation backlog is **steps 1–4**: define the first contracts, specify and hand-check the rules, and build the deterministic Smoke dataset plus delta. That gives the infrastructure and transformation work concrete inputs and independently known expected results.
