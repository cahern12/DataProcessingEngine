# POC Hypothesis, Benchmark, and Economics Validation Plan

Date: 2026-10-22. Review baseline: implementation commit `5931bc6`.

This plan audits [01 — POC Validation Plan and Platform Requirements](01-enterprise_data_processing_poc_validation_plan.md) against the implementation and evidence in [04](04-local_build_execution.md), [06](06-local_lakehouse_execution.md), and [08](08-targeted_incremental_execution.md). It then defines the remaining work in execution order.

**Assessment: the implementation does not yet fully test H1–H5, close G1–G7, execute the complete benchmark matrix, or produce monetary costs by technology and workload.** It provides a verified local processing foundation. Local correctness and recovery results must remain distinct from Azure, multi-engine, scaling, and economic conclusions.

The existing local-only direction remains in force. The first stages below can be developed locally. Azure deployment and billed experiments are a later, separately authorized stage. This document does not provision services, choose a subscription, or authorize spending. Until Azure evidence exists, the original Azure-specific requirements remain open.

## 1. Current coverage and the evidence still required

### Architecture hypotheses

| ID | Current assessment | Evidence available | Missing proof |
|---|---|---|---|
| H1 — Azure/open-format engine independence | Partially investigated locally; unproven as stated | Iceberg/Parquet, REST catalog, stable Gold interface, local Trino read/write | ADLS/Azure execution; Spark interoperability; the same consumer reading either engine's publication without engine-specific changes |
| H2 — dbt + Trino technical, operational, and economic viability | Partially investigated | End-to-end SQL path, fixtures, replay/recovery, two Smoke-scale incremental comparisons | Representative workload families, larger volumes, worker/concurrency scaling, operational effort, and acceptable monetary cost against frozen limits |
| H3 — Spark may fit some workloads better | Untested | Independent Python reference available for correctness | Implement selected equivalent Spark workloads; compare cost, speed, recovery, and operational burden; report when Spark offers no material advantage |
| H4 — Equivalent governed Trino/Spark outputs | Untested | Stable schemas, fixtures, snapshot-pinned publication and comparison utilities | Both engines producing equivalent Iceberg outputs through one publication/consumer contract, including failure and restart cases |
| H5 — Defensible workload-level cost measurement | Untested | Runtime, resource, file, and IO evidence for local Trino runs | Rate/usage provenance, per-workload and per-technology attribution, shared/idle allocation, billing reconciliation, uncertainty, and placement comparisons |

H3 is a comparison question. A finding that Spark provides no material advantage is useful evidence and does not require adopting Spark in production. H4 still requires the selected interoperability/equivalence experiments.

### Mandatory POC gates

| Gate | Current assessment against the complete original scope | What closes the remaining gap |
|---|---|---|
| G1 — End-to-end processing | Demonstrated for local synthetic ITSM sources; partial overall | Representative chosen source patterns and complete governed processing on the Azure target |
| G2 — Processing abstraction | Demonstrated for the local consumer interface; partial overall | Identical consumer requests and output contracts across Trino- and Spark-produced publications, including Azure storage |
| G3 — Correctness and trust | Strong local evidence; partial across the full matrix | Existing fixtures plus workload-specific expected results, both engines, every required scale cell, contract/refusal tests, and reconciliation |
| G4 — Repeatability and recovery | Local publication failures, replay, and service restart pass; partial overall | Distributed worker/executor and coordinator/driver failures, orchestration retries, storage/catalog faults, bounded recovery, and retained pinned reads |
| G5 — Performance and scale | Smoke-scale telemetry only | Controlled Small/Medium/Large, worker-count and concurrency experiments, repeated runs, resource saturation and limits |
| G6 — Transparent economics | Not met | Marginal, standing, shared/idle allocation, fully allocated, per-engine, per-technology and per-TB cost reports with defensible provenance |
| G7 — Technology fit | Not met | Evidence-based technical/operational/economic comparison and Proceed / Proceed with Changes / Do Not Proceed recommendation |

No gate is declared fully closed for the complete Azure POC by this review. This does not invalidate the narrower local milestones already verified.

### Benchmark and economic coverage

The two latest experiments compare **targeted Trino against full-rebuild Trino**, not Trino against Spark. They use roughly 1,000 incidents, one local Trino node, and one workload at a time. The controlled case reads about 80% fewer processing bytes; the open-heavy case reads about 65% more. Neither demonstrates a general runtime improvement, distributed scaling, or monetary savings.

The SQL pipeline contains joins, windows and aggregation, but running those operations inside one pipeline does not satisfy the original isolated workload matrix.

| Requirement in 01 | Current coverage | Remaining work |
|---|---|---|
| Scan/filter; join; aggregation/window families | Operations exist inside the pipeline | Independently specified workloads, measured separately, at all required sizes |
| Bronze → Silver → Gold | Local full and targeted paths verified | Both engines, representative sizes, Azure execution |
| Failure/recovery at Medium and Large | Local faults/restart at Smoke scale | Distributed failure experiments and recovery cost at required sizes |
| Worker scaling and concurrency | Not benchmarked | Explicit scale-up/out and competing-workload experiments |
| Current-state comparison where practical | Python/SQLite correctness/development baseline | Identify actual current approach; measure comparable cases or record its unavailability/limits |
| Marginal/allocated/fully allocated cost and cost per TB | Not implemented | Versioned cost ledger, pricing, allocation, reports and reconciliation |
| Standing cost and cost/volume/worker curves | Not implemented | Idle measurements, workload-mix assumptions and controlled economic experiments |

`dpe/pipeline.py` currently emits `marginal_cloud_cost: null` and `fully_allocated_cloud_cost: null`, with an explanation that cloud costs were not measured. `contracts/processing.v1.json` explicitly says not to report zero cloud cost. The lakehouse reports contain resource/IO statistics, not monetary cost calculations. `benchmarks/acceptance.local.v1.json` leaves cloud runtime/scale/cost limits undefined.

Consequently, **we do not currently output the associated monetary costs for each technology for each workload**. Reduced scanned bytes are processing evidence, not a cloud bill.

## 2. Freeze the experiment contract and acceptance criteria

Do this before adding engines or collecting decision-grade measurements.

1. Create a requirements-to-evidence register covering H1–H5, G1–G7, all five workload families, all eleven deliverables in section 8 of 01, and the required economic outputs. Every entry needs an owner role, experiment IDs, acceptance criteria, evidence location and verdict.
2. Freeze the source manifest/snapshot set, v1 rules, Gold contracts, cutoff/reporting interval, quality policy, baseline commit, environment and comparison boundaries for each experiment. Preserve the existing local acceptance configurations as historical milestones.
3. Define business limits for completion time, recovery time, per-run cost, fully allocated cost/TB, standing daily/monthly cost, scale behavior and acceptable operator effort. These are required inputs to a decision, not values inferred from whichever result wins.
4. Save a versioned acceptance specification and hash before measurement. A missing required limit produces `not_assessed`; a missing mandatory experiment cannot silently pass.
5. Maintain separate hypothesis verdicts (`supported`, `not_supported`, `inconclusive`) and gate verdicts (`passed`, `failed`, `not_assessed`). A slower or more expensive engine can be a valid experimental result.

Proposed engineering rules to finalize and freeze before the first comparable run:

| Criterion | Proposed rule |
|---|---|
| Correctness | Zero unexplained field/key differences, duplicate Gold keys or reconciliation differences; existing precision/null semantics preserved |
| Publication/replay | No partial publication, checkpoint advancement on failure, or old-replay rollback |
| Required matrix coverage | 100% of required cells have evidence; resource-limited, timed-out and skipped cells remain visible |
| Telemetry | Every required run/attempt and required component has captured metrics or an explicit unsupported field; missing decision-critical metrics invalidate that comparison |
| Repetition | At least three measured repetitions per required normal cell; prefer five for rankings; report median and range, not a p95 from three samples |
| Financial reconciliation | Proposed maximum 2% unexplained difference against the comparable settled billing/export scope; persist residuals and fail the economics check beyond the frozen tolerance |
| Material benefit from a second engine | Proposed decision trigger: at least 20% lower fully allocated cost or 30% lower completion time on a required workload, while meeting all other limits and justifying added operations; freeze or replace before measurement |
| Runtime, recovery, standing cost and workload cost | Workload-specific numeric limits must be supplied before decision-grade execution; existing 60-second local limits are not Azure business SLOs |

The percentages above are proposals, not results or accepted business requirements. Do not mark G5–G7 passed while essential limits remain unset.

**Exit condition:** an executable acceptance specification and coverage register identify exactly what constitutes success, failure and missing evidence.

## 3. Specify representative datasets, sizes and workloads

Continue using our own synthetic data and independently authored rules/results.

Keep the 1,000-incident Smoke suite as a fast regression check. Starting candidates for the decision matrix are Small = 100,000 incidents, Medium = 1,000,000 and Large = 10,000,000, matching existing generator profile counts. These are provisional until calibration records actual snapshot/event rows, logical bytes, compressed bytes, file sizes and memory requirements. Then freeze the tier definitions. A generation profile is not a processing-capacity result.

Build deterministic 30/60/90-day delivery sequences with independently configurable history length, event fan-out, skew, cardinality, open/resolved mix and delta size. Include late backfills, duplicate-only days, empty deliveries, future eligibility and corrections. Test that a configured zero delivery/new-incident fraction really produces zero; preserve old fixture identities and version changed generator semantics. The current generator's minimum-one-new-incident behavior is documented in 08.

Use synthetic application extracts, CSV/JSONL/Parquet feeds, the mock API, and a small separately isolated synthetic relational extract where practical. Do not use the Nessie metadata database as the benchmark business source. Select representative patterns rather than requiring every source product/format listed as an example in 01.

Define the workload matrix explicitly:

| ID | Workload | Sizes | Trino | Spark | Expected-result and measurement focus |
|---|---|---|---|---|---|
| W1 | Scan/filter | S/M/L | Required | Optional; record rationale | Fixed predicates/selectivity, projection, pruning, known qualifying keys and counts |
| W2 | Multi-table join | S/M/L | Required | Required | Snapshots/events/reference joins; balanced and skewed keys; known cardinality; join/shuffle/spill behavior |
| W3 | Aggregation/window | S/M/L | Required | Required | Version ranking, event ordering, cumulative durations and grouped results with known answers |
| W4 | Bronze → Silver → Gold | S/M/L | Required | Required | Same source histories and v1 outputs; full rebuild for both, targeted Trino as an additional strategy |
| W5 | Failure/recovery | M/L | Required | Required | Repeat selected W2/W4 cases with controlled faults; exact recovery output and all-attempt resource/cost accounting |

Measure initial processing, sparse deltas, open-heavy cutoff advancement, late corrections and duplicate-only delivery as distinct W4 variants. A Spark full rebuild must not be presented as equivalent incremental capability. If claiming an engine's incremental implementation, implement and verify that strategy separately; otherwise label its scope explicitly.

Workload specifications must contain SQL/job definition hashes, expected grain/schema, input/output volume, distribution, seeds, reference snapshots and the independent correctness method. Scale invariants alone do not replace exact comparisons where exact contracted results are required. New business semantics, such as business calendars or effective-dated SLAs, require new rule fixtures/versions; they are not a prerequisite for every engine comparison.

**Exit condition:** every required matrix cell references reproducible source and workload specifications with a defined correctness check.

## 4. Implement the rate catalog and cost ledger

Build this locally before treating any measured run as an economic result.

### Cost evidence classes

| Cost basis | Meaning | Permitted conclusion |
|---|---|---|
| `local_measured_usage` | Measured local runtime/resources, with no defensible cloud bill | Local resource behavior; monetary fields remain unavailable unless a separate local rate model is supplied |
| `modeled_cloud` | Explicit scenario quantities/rates, possibly informed by local measurements | A projection with assumptions and sensitivity; not observed Azure cost or validated Azure performance |
| `azure_provisional` | Azure measured usage and documented prices/allocation, awaiting settled usage/export data | Provisional economic result; retain reconciliation status |
| `azure_reconciled` | Azure usage/cost scope reconciled to the chosen billing basis, with documented allocations to runs | Decision-grade workload economics within the declared scope and tolerance |

A local host's runtime multiplied by an Azure VM hourly price does not prove Azure runtime, sizing or actual workload cost. Keep projections separate from measured cloud results. Missing rates, quantities or allocations are `null` with reason and an incomplete status; zero is reserved for a verified zero charge, and `not_applicable` is explicit.

### Price and usage provenance

Maintain immutable rate snapshots containing provider, region, service/SKU/meter IDs, currency, effective dates, billing unit/tiers, purchase model, retrieval time, source URL/file and checksum. Preserve usage-source files and the unit conversion that binds each quantity to its rate. Use decimal arithmetic and retain precision before display rounding.

Use the [Azure Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices) for clearly labeled public-price modeling. Its retail rates do not establish our negotiated invoice cost. For billed experiments, use the applicable effective price/billing data and declare actual versus amortized cost treatment. Keep on-demand, reservation/savings-plan and spot assumptions separate; do not add actual and amortized representations of the same charge.

### Technology/component attribution

Every cost ledger line has one billing identity, one cost category and one component owner. Provide alternative **technology** and **Azure service/SKU** views over those same lines; do not sum the two views together.

| Technology/component | Cost to attribute |
|---|---|
| Trino | Worker and coordinator infrastructure, startup/shutdown and applicable spill/temporary resources |
| Spark | Executor and driver infrastructure, startup/shutdown, shuffle/spill and required history/event services |
| dbt runner | Its task/container resources and selected product charges, if any; exclude compute already attributed to Trino |
| Airflow | Scheduler, worker/executor, metadata database and applicable managed-service resources |
| Iceberg REST catalog | Catalog service capacity and its PostgreSQL/backend share |
| Object storage / ADLS | Raw/Bronze/Silver/Gold retention, metadata, retained versions/copies/orphans, transactions and temporary storage |
| Container infrastructure | Residual AKS control/system/platform services, image registry and infrastructure not already assigned to engine/service lines |
| Monitoring/evidence | Metrics/log ingestion, retention, exporters/collectors and applicable service charges |
| Network | Applicable transfers, egress, network appliances/endpoints and related metered charges |
| Current-state reference | Its measured infrastructure and applicable license/service costs, where available |

Parquet and Iceberg are formats, not independent Azure compute meters. A format's storage, metadata, transaction and maintenance consequences appear under the relevant resources. An open-source software license does not make its runtime infrastructure free. Shared VM/node charges must not be counted once as AKS infrastructure and again as engine compute.

Distinguish storage-operation quantities from scan bytes: obtain request counts from storage/service telemetry or controlled logs, including metadata calls and retries. Apply the actual meter's units/tiering. Track byte-hours for temporary/retained data and the declared retention interval; do not price all output forever or omit copies/orphans.

**Exit condition:** hand-calculable synthetic-rate fixtures prove units, tiers, missing data, rounding, retries, shared-node attribution and prevention of duplicate charges. The system can emit component-level cost rows before cloud deployment, correctly labeled as modeled or unavailable.

## 5. Calculate marginal, standing and fully allocated costs

Define an accounting window and a common workload boundary before allocation. Record all attempts from first submission to validated publication, plus the chosen retention/maintenance horizon. Report engine-only and complete-service costs separately.

For logical workload `w`, component `c`, standing pool `p` and window `T`:

```text
M_w,c = additional billable cost caused by executing w, assigned once to c
M_w   = sum_c M_w,c

B_p(T) = standing/shared pool cost over T, excluding all costs already in M
A_w,p  = B_p(T) * weight_w,p
F_w    = M_w + sum_p A_w,p

logical_TB_w = common logical input bytes_w / 1,000,000,000,000
marginal_cost_per_logical_TB_w = M_w / logical_TB_w
fully_allocated_cost_per_logical_TB_w = F_w / logical_TB_w
```

A warm fixed-size cluster may incur little additional compute cash cost for one run; it still has active and idle capacity costs to allocate. Report attributed active compute alongside genuine marginal cost when those differ. Do not call CPU-utilization share an additional VM bill.

Apply these accounting rules:

1. Each underlying charge belongs to marginal cost or a standing/shared pool exactly once. Failed/retried attempts belong to the same logical workload; failed-only runs still carry their costs.
2. Partition baseline engine pools, orchestration, catalog, monitoring, storage and infrastructure. Allocate each by a frozen defensible driver: reserved capacity-time, task duration, requests, byte-hours or declared demand. Actual usage is a diagnostic; capacity allocation must also account for idle/system resources.
3. For a fully allocated workload view, weights sum to one across eligible workloads in a pool/window. Allocate idle/standing capacity explicitly. If no workloads exist, keep the pool as standing cost; do not divide by zero. Show unattributable amounts and fail completeness where material amounts remain unexplained.
4. Reconcile `sum(M) + sum(B)` with the chosen total cost scope. Reconcile `sum(F)` with that total when every pool has eligible workloads, otherwise display the idle-only/unallocated remainder. Idle/system cost breakdowns are explanatory subsets, not additional charges to add twice.
5. Use the same logical input definition when comparing engines or full versus targeted execution. Do not divide targeted cost by selected bytes and full cost by complete history while labeling both as the same cost/TB. Also report physical scan bytes/TB as a separate efficiency metric.
6. A cutoff-only zero-input delivery has `not_applicable` cost per new-delivery TB. Its full-history logical-workload denominator, per-run cost and cost per published incident/day can still be reported explicitly. Never divide by zero.
7. Attribute retry, validation, immutable-copy, compaction, garbage collection and recovery costs. Show a predeclared amortized maintenance share for recurring workloads as well as the observed experiment's maintenance cost; avoid charging both representations into one total.
8. Report observed window standing cost and a separate recurring workload-mix scenario. Freeze assumed run frequency, uptime, retention, concurrency and demand before projecting daily/monthly costs. Show low/base/high demand, rate and allocation sensitivity.
9. Record tuning and operator labor hours. Keep infrastructure economics and any labor/TCO scenario distinguishable with its own rate assumptions.

**Exit condition:** component subtotals equal each run total; logical-run costs include every attempt; allocated totals reconcile to the accounting scope; idle and missing data are visible; scenarios cannot be mistaken for bills.

## 6. Produce the required economic and benchmark outputs

The outputs below are proposed artifacts to implement, not existing files or commands.

| Artifact | Required content |
|---|---|
| `run_results.json` | Immutable run/attempt identity, inputs/configuration, resource/IO/runtime, quality/publication, criteria and provenance |
| `cost_ledger.jsonl` | Charge/usage identity, time interval, resource, component, quantity/unit, rate reference, purchase/cost basis and marginal/shared classification |
| `workload_costs.csv` | One row per workload × engine × strategy × size × worker profile × concurrency × repetition, plus all-attempt run total |
| `technology_costs.csv` | Child rows per run and technology/component, with direct, allocated, total and idle/maintenance breakdowns |
| `platform_costs.csv` | Observed standing pools, allocation windows/drivers, idle/system/unallocated amounts, and separate recurring-demand scenarios |
| `cost_performance.csv` | Comparable runtime/throughput/cost/TB, scale efficiency, tuning/operator effort and quality results |
| `billing_reconciliation.json` | Export/version/source checksums, included/excluded scope, actual/amortized basis, allocation transformations, residuals and provisional/reconciled state |
| `poc_scorecard.json` and Markdown report | Per-hypothesis/gate verdicts linked to required experiments, missing cells, cost completeness and recommendation |

Each workload summary must expose at least:

```text
experiment_id, logical_run_id, attempt_ids, workload_id, engine, engine_version,
strategy, dataset_version, manifest/snapshot_hashes, size, worker_profile,
concurrency, cache_policy, repetitions, logical_input_bytes, physical_scan_bytes,
output_bytes, runtime_seconds, quality_status, recovery_status,
currency, cost_basis, rate_snapshot_id, allocation_policy_version,
marginal_cost, attributed_active_compute_cost, allocated_platform_cost,
fully_allocated_cost, marginal_cost_per_logical_TB,
fully_allocated_cost_per_logical_TB, standing_window_id,
retry_cost, maintenance_share, cost_completeness, reconciliation_status
```

Technology rows must join back to that same run and sum to its cost; standing-window totals are separate records, not repeated into every workload total. A presentation table should let a reviewer compare Trino and Spark for the same workload and expand each row to engine, dbt, Airflow, catalog, storage, monitoring, infrastructure and network costs. Use explicit `not_applicable` components where appropriate.

Reports must retain unsuccessful, timed-out and resource-limited runs and their incurred costs. A correctness failure cannot win a cost ranking. Repeated-run summaries must retain underlying individual runs and their variability.

**Exit condition:** one command builds machine-readable and human-readable per-workload/per-technology reports, and tests verify their totals and incomplete-data behavior.

## 7. Prove Spark/catalog compatibility and common publication

Before benchmarking, pin a compatible Spark, JVM, Iceberg connector and storage-client combination through a small local compatibility spike. Keep the current Trino path reproducible.

1. Connect Spark to the selected REST catalog and local object store; prove create, read, insert and the write operations needed by the chosen workloads.
2. Read Trino-created Iceberg tables from Spark and Spark-created tables from Trino. Verify schema types, UTC timestamps, decimals/rounding, nulls, partitioning and snapshot identity.
3. Implement independent Spark transformations for the selected workloads, including both Gold products. Pass the existing 53 fixtures and add cases for any portability differences; do not derive expected values from Spark output.
4. Publish either engine's outputs through the same logical dataset/publication interface. A consumer must read Spark-generated Gold through the unchanged common SQL/CLI interface.
5. Test old pinned publications after newer writes, injected failures and restart. The current Nessie path could not read an earlier snapshot after mutation; retain immutable publication isolation unless an alternative is actually proven through both engines.
6. Measure immutable-copy, retained-file and failed-attempt/orphan overhead. Do not use an unsupported catalog time-travel assumption to make one engine's writes appear cheaper.

Spark's catalog configuration supports Iceberg integration; actual compatibility still requires the pinned-stack experiment. See the [Apache Iceberg Spark configuration documentation](https://iceberg.apache.org/docs/latest/spark-configuration/).

**Exit condition:** both engines pass the selected correctness/interoperability suite and publish through one consumer contract. This provides local H1/H4 evidence while Azure remains pending.

## 8. Build a common benchmark runner and durable telemetry

Introduce engine adapters behind a shared experiment runner. Add local Airflow orchestration for the scheduled-path comparison required by 01, with version-pinned dependencies and recorded resource use. Keep a direct runner for isolating engine execution from orchestration overhead.

One experiment should perform: prepare isolated inputs/output namespace → record frozen specification → start telemetry → submit work → validate outputs/publication → collect artifacts → calculate costs → evaluate criteria → persist report before exit. Cleanup runs only after required evidence is retained and protected publications are excluded.

Record a logical run ID across Airflow DAG/task IDs, retries, dbt invocations/models, Trino query IDs, Spark application/job/stage/task IDs, input/output snapshots, resource identities and cost lines. Engine-local success is not publication success.

Capture:

| Category | Required evidence |
|---|---|
| Identity/configuration | Dataset/workload/rule/contract/code versions, manifests/snapshots, engine/JVM/connector versions, worker count/shape, coordinator/driver resources, session settings, infrastructure revision |
| Time | Queue, provisioning/startup, ingestion, Bronze/Silver/Gold, validation, publication, recovery and complete time to usable output |
| Resources | CPU time and sampled utilization, memory peaks/reservations, disk/spill, network/shuffle, worker/executor counts over time, allocated capacity and autoscaling intervals |
| Data/IO | Logical source bytes, engine scan bytes, cache versus external IO, object-store requests, output/temporary/retained bytes, file counts and size percentiles |
| Correctness/reliability | Output contract results, exact differences, reconciliation, quarantine, retries, failed attempts, pinned-read checks and checkpoint state |
| Operations | Operator interventions, manual recovery steps, tuning/configuration changes, implementation/tuning hours and maintenance actions |
| Financial inputs | Meter quantities/units, resource IDs, rate references, accounting windows, component ownership, allocation policy and provenance/completeness |

CPU utilization is not billed VM time; engine scan bytes are not object-store request counts. Sample resource usage and allocated/billed capacity separately. Measure raw ingestion, validation, publication copying, catalog/orchestration, collectors and maintenance rather than limiting the system boundary to successful SQL statements.

Prefer durable completed-query capture over reliance on expiring query history. Trino offers an [HTTP query event listener](https://trino.io/docs/current/admin/event-listeners-http.html); Spark provides [event logs, REST APIs and executor metrics](https://spark.apache.org/docs/latest/monitoring.html). Verify needed metric fields in the pinned releases and retain collector overhead. Preserve failed-run logs and do not compact away evidence required for recovery analysis.

**Exit condition:** the runner produces the same experiment envelope for both engines, with durable all-attempt telemetry and explicit measurement boundaries.

## 9. Execute controlled local comparisons and scaling experiments

First run the compatibility spike, fixtures, Smoke correctness, economics fixtures and report checks. Then increase data/resource requirements in controlled steps.

1. Run required S/M/L normal cells on both engines, using the same logical inputs and common Parquet/table layout for the controlled comparison. Benchmark engine-specific tuning/layout separately and record tuning effort.
2. For representative W2/W4 sizes, test 1/2/4 workers with fixed per-worker resource shapes to measure scale-out. Separately test equal-total-resource/equal-budget configurations. Include Trino coordinator and Spark driver resources in totals; worker count alone is not a fair resource match.
3. Test concurrency 1/2/4 on a frozen resource envelope. Report queue time, time to usable publication, throughput, saturation, failure rate and standing-cost allocation under competing work.
4. Separate cold/startup and warm runs with documented cache policies. Randomize/interleave engine order, perform declared warmups and retain all measured repetitions. Do not label an uncontrolled OS/object cache as cold.
5. Preserve fixed-layout results before tuning pruning, file size, broadcast/join strategy, shuffle partitions, memory or fault tolerance. Report the final reproducible settings and the effort needed to find them.
6. Run the Python/SQLite harness where feasible as a development reference. Determine whether it represents the organization's actual current-state system. If another current system is available, specify equivalent workload measurements; otherwise record that organizational comparison as unavailable, with owner/reason and impact on conclusions. Do not silently relabel our reference evaluator as the existing enterprise platform.

The current 4-CPU/6-GiB local runtime is not evidence that it can execute the complete distributed matrix. A cell exceeding resources is recorded as resource-limited and remains open until executed on a suitable controlled runner. Never shrink data or change a size label after observing failure.

**Exit condition:** the local runner demonstrates fair engine comparison and cost/report mechanics, with all capacity gaps explicit. Locally modeled results remain separate from Azure gate closure.

## 10. Validate the Azure processing and billing boundary

Prepare this stage locally; execute it only when Azure work is authorized. Before deployment, record the subscription, region, currency, resource SKUs, maximum experiment spend, environment lifetime, retention, quotas and shutdown/cleanup procedure. Freeze Azure acceptance limits and rate snapshots before decision-grade runs.

Build reproducible infrastructure definitions for the selected ADLS Gen2, Iceberg REST catalog/backend, AKS, Airflow/dbt/Trino and comparative Spark roles. Start with the smallest environment that can validate compatibility; use budgeted resource profiles for the matrix. Record managed identity/access, storage/network topology and all service/monitoring resources actually charged. Technology roles are candidates to evaluate, not a requirement to retain every component in production.

Prove real ADLS ingestion, Iceberg writes, restart, same-consumer cross-engine reads and failed-write publication behavior. The current filesystem lock/atomic pointer is a local coordinator: select and test a supported Azure-accessible publication/checkpoint mechanism with single-writer enforcement or conditional updates. Do not assume local file locking coordinates different pods/nodes. Test stale-parent publication attempts and preserve committed pins during concurrent non-conflicting workload execution.

Establish defensible attribution using dedicated resource groups, workload/experiment identifiers, namespaces/node pools and resource-to-run interval mappings. Resource tags alone do not assign a short-lived query a separate VM invoice line. Use node/pod allocations and service-level usage to derive per-run shares, retaining the provider-billed resource totals.

Collect Azure Cost Management usage/cost exports for the dedicated scope, including standing/idle, storage transactions, monitoring and relevant network services. [Cost Management exports](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-export-acm-data) support cost/usage datasets; choose and record one consistent accounting basis. Handle delayed/revised data by publishing provisional results first and a versioned reconciled report later.

[AKS cost analysis](https://learn.microsoft.com/en-us/azure/aks/cost-analysis) can supply cluster/namespace allocation evidence, including idle/system/unallocated categories. Verify its prerequisites and availability for the selected setup; otherwise use the measured node/pod attribution model and billing exports. This plan does not assume a portal view is a ready-made per-query cost API. Our ledger must conserve total charges across either approach. Azure allocation rules can inform shared-service accounting but [do not change the underlying invoice](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/allocate-costs).

Repeat required workload/size, worker-scale and concurrency experiments on the Azure target. Retain measured economic results for each required engine/workload cell and standing-cost curves for realistic run frequencies. Compare modeled versus measured outcomes and explain the difference rather than replacing the original assumptions.

**Exit condition:** Azure compatibility and required performance cells have measured evidence; usage/price/allocation lineage produces reconciled per-workload/per-technology costs. Otherwise H1 and the affected gates remain open regardless of local success.

## 11. Complete distributed recovery and operating-cost experiments

Run W5 at Medium and Large on both engines in the suitable controlled environment, with configured limits frozen before injection. Test worker/executor loss, coordinator/driver interruption, catalog/object-store unavailability, orchestration retry and failure between Gold products/before activation. Use a fault profile per engine; do not assume identical automatic recovery behavior.

For each fault, record detection, retry policy, time from first failure to validated usable output, duplicate/orphan side effects, checkpoint state, previous consumer-visible snapshots and every manual action. Verify exact outputs against a clean run and old/current replay after recovery. Capture all-attempt cost, warm capacity while waiting, temporary/orphan storage, cleanup, and failed-only runs.

Run repeated publications and the selected retention/compaction policy to measure table/file growth, planning overhead and maintenance cost. Prove protected publication snapshots are not deleted. Include the current immutable-copy strategy in the comparison, and evaluate an alternative only after proving its historical-read behavior.

Assess the recurring operational burden of each candidate architecture: deployment, upgrades, tuning, monitoring, retry/recovery, catalog maintenance, cleanup, and the additional Spark runtime if retained. Preserve hands-on hours separately from infrastructure totals.

**Exit condition:** G4 recovery criteria pass at required sizes, failures remain visible in economics, and the operational complexity comparison has measured evidence.

## 12. Assemble the POC decision and close the requirements register

Build a final evidence package that maps directly to section 8 of 01:

| Original deliverable | Completion artifact |
|---|---|
| Working Azure POC environment | Infrastructure revision, deployed configuration, compatibility and persistence results |
| Representative Bronze/Silver/Gold datasets | Reproducible source manifests and governed table/publication snapshots |
| Version-controlled pipelines/transforms | Engine and orchestration definitions, dependency locks and experiment specifications |
| Quality and reconciliation results | Fixture, workload, complete-population and cross-engine comparison reports |
| Execution and lineage metadata | Run/attempt/job/query/resource/snapshot/cost links |
| Performance and cost benchmarks | Required matrix, volume/worker/concurrency curves, standing cost and allocation/reconciliation reports |
| Trino versus Spark comparison | Equivalent selected workloads, resource/cost basis, variability and tuning effort |
| Failure/recovery findings | Required Medium/Large fault reports, bounded recovery and retry/cleanup costs |
| Operational complexity assessment | Measured interventions/hours, recurring service/maintenance inventory |
| Architecture risks/findings | Catalog/time-travel/isolation, portability, capacity, attribution and unresolved limits |
| Production architecture recommendation | Supported hypothesis verdicts, seven gate verdicts, workload-placement rationale and one of the three required decisions |

Keep broader enterprise requirements BR-01–BR-15 in a separate applicability register with demonstrated, partial, future or out-of-scope status. Do not expand this POC into production multi-tenancy, disaster recovery, enterprise onboarding, CDC or streaming merely to populate that register; section 9 of 01 explicitly describes longer-term direction.

The decision must compare the simplest viable Trino path against adding Spark for specific proven workload advantages. Present workload runtime, marginal/fully allocated cost, standing platform cost, scale limits, operational effort and consumer portability together. Identify uncertainty and sensitivity; do not pick an engine on scan bytes alone.

Use `Proceed` only with sufficient required evidence and passing gates. Use `Proceed with Changes` for a supported viable design with concrete changes and an explicit validation path. Use `Do Not Proceed` when required constraints are not met. Until mandatory experiments or business limits are available, label the package incomplete; do not convert missing Azure/economic evidence into a final production recommendation.

Record execution in the next numbered planning document with exact revisions, evidence paths, H/G scorecards, cost completeness, unsupported cells and remaining decisions.

**Exit condition:** an independent reviewer can trace every claimed requirement to reproducible evidence and reconstruct each workload's component costs without double counting.

## 13. Proposed implementation structure and first build sequence

The paths below describe work to implement; they do not exist merely because this plan names them.

```text
benchmarks/specs/                 workload, size, resources and acceptance specifications
benchmarks/pricing/               immutable rate/scenario snapshots with provenance
contracts/benchmark/             run, metric, cost-line and scorecard schemas
transformations/spark/           comparative engine implementations
infra/local/                     optional Spark/Airflow profiles and durable telemetry
infra/azure/                     later Azure infrastructure definitions
orchestration/                   version-controlled benchmark DAGs
dpe/benchmark_runner.py          common experiment coordinator (proposed)
dpe/engines/                     Trino/Spark adapters (proposed)
dpe/costing.py                   rate binding, ledger and allocation (proposed)
dpe/benchmark_reports.py         summaries, reconciliation and scorecards (proposed)
tests/                           synthetic-rate fixtures, engine equivalence, report tests
```

Start with these concrete increments, in order:

1. Add the coverage register, benchmark schemas/specifications and acceptance template; retain unknown business limits explicitly.
2. Implement the cost ledger, rate/usage import, allocation and per-technology/per-workload reports with independent arithmetic fixtures. Demonstrate local usage versus modeled cost labeling on the existing Trino path.
3. Complete the Spark/REST-catalog compatibility spike and fixture-equivalent Gold publication.
4. Add Airflow/common submission, durable telemetry and the five workload definitions behind one runner.
5. Run local Smoke and attainable scale comparisons, producing paired Trino/Spark cost/performance artifacts and explicit resource-limited cells.
6. Prepare the Azure deployment, attribution and budget specification for later execution; finish the required Azure matrix, reconciliation and recovery work before closing the POC.

This sequence addresses the missing economic outputs early while preserving the verified v1 pipeline. It coordinates the relevant workload/operations work in [05](05-local_platform_improvement_plan.md); it does not require all future business-rule extensions before meaningful POC comparison can begin.

## 14. Source references

The external references support integration and accounting mechanisms, not a claim that those integrations have already been implemented. Documentation was reviewed on 2026-10-22; pin and verify actual component/API versions during execution.

- [Apache Iceberg Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/): catalog integration options; the selected release combination still needs a compatibility spike.
- [Trino HTTP event listener](https://trino.io/docs/current/admin/event-listeners-http.html): durable query-event capture for resource/data-access telemetry.
- [Spark monitoring and instrumentation](https://spark.apache.org/docs/latest/monitoring.html): event logging, historical inspection, REST and executor metrics.
- [Azure Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices): public regional/SKU price inputs for labeled modeling.
- [Azure Cost Management exports](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-export-acm-data): actual/amortized cost and usage export mechanisms.
- [AKS cost analysis](https://learn.microsoft.com/en-us/azure/aks/cost-analysis): Kubernetes-oriented cost allocation and setup prerequisites.
- [Azure shared-cost allocation](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/allocate-costs): allocation/accountability versus the underlying invoice.
