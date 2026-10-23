# POC Hypothesis, Benchmark, and Economics Execution Record

Date: 2026-10-22. Implements the local stages of [09](09-poc_hypothesis_benchmark_and_economics_validation_plan.md), against the requirements in [01](01-enterprise_data_processing_poc_validation_plan.md). Baseline commit: `1e5d550`. Azure execution remains outside the user's current local-only scope.

**Outcome: the local platform now supports equivalent Trino/Spark processing, repeatable comparative workloads, Airflow submission, durable engine telemetry, and a tested workload/component accounting system. The complete original POC remains incomplete.** Required Azure, distributed scale/recovery and reconciled billing evidence are still missing.

## 1. Decision targets adopted before comparisons

The user authorized us to choose targets from the requirements. [Acceptance v1](../../benchmarks/specs/acceptance.poc.v1.json) and [its rationale](../../benchmarks/specs/target_rationale.md) make those choices reviewable and reproducible:

- Full BSG completion: 3/15/60 minutes at Small/Medium/Large. Isolated workload limits are specified separately.
- Recovery: 15/60 minutes at Medium/Large; detection within 2 minutes and at most one manual action.
- Marginal cost: $0.50/$2/$10 per S/M/L run. Fully allocated cost: $2/$5/$20 per run; $500/logical TB applies at inputs of at least 10 GB.
- Standing platform: at most $750/month; total at most $1,500/month under 24 runs/day, 30 days/month, 90/9/1% S/M/L mix and 7-day retention. Sensitivity is 6/24/96 runs/day.
- Quality: zero unexplained differences/duplicate Gold keys/reconciliation discrepancies, at least three measured repetitions, and at most 2% unexplained billing residual.
- One-to-four-worker scale-out: at least 2× speedup with at most 2.5× cost; four-way concurrency: at least 1.5× throughput with at most 3× completion time.
- A second engine needs at least 20% lower fully allocated cost or 30% lower completion time on a required workload, while meeting the other gates and justifying its operational burden.

These are initial POC decision targets, not established production SLOs. The future $300 / 24-hour cloud campaign and $50 / 6-hour stage ceilings are planning constraints, not spending authorization. Limits are frozen into each experiment before execution; revisions require a new rationale and retention of earlier results.

## 2. Implemented capability

| Area | Implemented behavior | Remaining boundary |
|---|---|---|
| Acceptance and coverage | Named H1–H5, G1–G7, eleven deliverables, economic requirements and BR applicability; explicit required matrix and per-run assessments | An incomplete experiment cannot close the original Azure gates |
| Synthetic history | Deterministic 30/60/90 delivery days; configurable source distribution/fan-out/delta/new incidents; empty/quiet days, outages/backfills, duplicates, duplicate-only days, future events and weeks-late corrections | Large histories are generated/tested at small counts; not a throughput claim |
| Engine independence | Spark SQL independently computes both v1 Gold products on shared Iceberg/REST/S3-compatible storage | No ADLS proof; Spark is full rebuild only |
| Common publication | Both engines publish immutable two-product snapshots and use the same existing Trino consumer | Local filesystem lock/atomic pointer is not a distributed coordinator |
| Comparative runner | W1 filters, W2 joins, W3 windows/durations, W4 BSG; deterministic interleaved ordering, exact independent expected results and repetitions | Required W4 variants and distributed W5 are listed but not automated at required sizes |
| Orchestration | Real local Airflow DAG → HTTP coordinator → engine adapters; persisted request identity and duplicate submission reuse | Coordinator restart marks jobs interrupted; no transparent distributed recovery claim |
| Telemetry | Durable Trino completed-query events, Spark application/event logs and task metrics; optional timestamped host Docker sampling | Provider-billed capacity/transactions, retained byte-hours and recurring operator labor remain incomplete |
| Economics | Decimal rates/tiers, explicit CSV usage import, all-attempt accounting, marginal/standing allocation, per-run/per-technology/TB outputs, billing residuals and demand scenarios | No actual Azure prices, usage export or reconciled cost has been collected |
| Azure preparation | Target/budget/role/attribution/coordinator/shutdown specification | No deployment, IaC, managed identity or cloud publication mechanism implemented |

The original six synthetic source datasets and 53 independently authored rule fixtures remain the correctness contract. Gold state, SLA and daily metrics come from Spark SQL, not a Python-computed Gold copy. Shared Python normalization/schema policy is kept separate from the expected-result evaluator. The legacy generator's minimum-one-delta behavior remains unchanged by default; `allow_empty_delta` explicitly selects the versioned zero-delivery behavior.

## 3. Compatibility and correctness findings

Pinned comparative stack: Spark 3.5.5, Iceberg 1.8.1 runtime and AWS bundle with checked SHA-256 digests, upstream Spark Python 3.8/Java 11; host/Trino runner Python 3.12, Trino 484, dbt Core 1.12.6, dbt-trino 1.10.6, Nessie 0.109.0, PostgreSQL 17.9, RustFS 1.0.0-alpha.81. Exact runtime versions/settings and implementation hashes are retained in results. Both JVM heaps are 2 GiB; Spark uses `local[4]` and two shuffle partitions within the shared 4 CPU / 6 GiB VM.

A real failed paired run exposed a portability issue: Iceberg 1.8.1's vectorized Spark reader rejected a Trino-created `DELTA_LENGTH_BYTE_ARRAY` string column. Spark now disables Iceberg vectorization. This is a recorded compatibility/tuning choice with a possible performance cost, not proof of an optimal Spark configuration. The [Iceberg Spark configuration documentation](https://iceberg.apache.org/docs/latest/spark-configuration/) describes the setting. The failed evidence remains at `data/poc-benchmark-dev/`; the corrected development comparison passed at `data/poc-benchmark-reader-fix/`.

The local interoperability/publication acceptance suite passed:

- Spark reads Trino-created Iceberg tables; Trino reads Spark-created tables and exact Gold snapshot pins.
- All 53 independent rule cases pass in both SQL engines, including quarantine, duplicate/deferred dispositions and both Gold contracts.
- The 1,000-incident initial / 1,100-incident delta results match independent Python evaluation exactly.
- Failure between Gold products and failure before activation leave active checkpoints and previous products unchanged for each engine.
- Retrying succeeds; old/current replay returns `already_committed`; new publications preserve older consumer pins.

Canonical acceptance evidence: `data/poc-verification-final/verification.json`. Its implementation fingerprint is `b9b5b648bde3d90cc9b530ba5245ff910223e1c371cc9f910b60fff4a87c8ab9`. Subsequent changes hardened custom size labeling, synthetic duplicate-only deliveries, tiered-meter accounting and exact incremental-report allocation weights; they did not change either engine's business SQL or publication implementation. The successful comparative run freezes implementation `4f1b2a5564f93a1fb51b8fb8376120c542195b683e0d11edd49aff3ce9a248e5` and criteria SHA-256 `fa64f10f6baf577aca295676b7ca5e85d2e55db41b244f843481a3321c3707cf`. The final image also pins MarkupSafe 2.1.5, exactly the transitive version used during those experiments; its cross-read/fingerprint probe passed at implementation `d04b46d9883be192d28abc3d2e4916b9643f9edc6db5658adfce008b3f88cb4b` (`data/poc-compatibility-pinned/`).

## 4. Capacity and measurement boundaries

Before tier processing, the local acceptance specification froze a 64 MiB raw-input preflight budget. Full-history Python materialization and exact validation share memory with the catalog, object store, Trino, Spark and optional Airflow. This is a conservative development resource policy, not a measured engine maximum.

Small calibration generated 100,000 incidents, 200,000 snapshots and 394,788 events, plus complete references, across 64 initial source files. Raw logical input was **113,412,943 bytes**; generation took **5.78 seconds**. It exceeded the **67,108,864-byte** budget and is recorded `resource_limited` with `engine_execution: not_run`. No compressed-table, runtime or cost claim is inferred. Evidence: `data/poc-small-calibration/calibration.json` and its frozen criteria/source manifest.

A three-repeat 10,000-incident custom comparison was attempted on the same frozen profile. Preparation completed, and the first Spark W1–W3 results matched expected rows. During Spark W4, **Trino was OOM-killed**: Docker recorded `OOMKilled: true`, exit 137. The common-consumer validation then failed with a DNS error because Trino was no longer running. That experiment remains **failed**, with insufficient repetitions for a ranking. The 64 MiB input preflight is not a guarantee of adequate total memory or a measured engine limit.

Evidence: `data/poc-custom-10000/run_results.json`, `data/poc-custom-trino-state.json`, `data/poc-custom-trino.log` and `data/poc-custom-resource-samples.jsonl`. This is an observed limit of the combined 6 GiB setup, not proof of an intrinsic 10,000-row/incident engine limit. Individual heap settings do not bound total native/Python/shared-service memory. Do not use its partial timings to rank the engines.

Restoring Trino made Spark's already committed 10,000-incident publication readable, with zero differences against the independent evaluator (`recovered-consumer.json`). The failed comparison was not changed to passed. A subsequent intentional PostgreSQL/object-store/catalog/Trino restart preserved all four initial/delta publications from the acceptance suite: eight exact pinned product reads passed (`data/poc-verification-final/restart.json`). Local service recovery still requires an operator; this is not automatic failover or the required Medium/Large fault suite.

The required Small/Medium/Large definitions remain 100,000 / 1,000,000 / 10,000,000 incidents, provisional pending complete calibration. Neither the 1,000-incident Smoke suite nor a 10,000-incident custom experiment substitutes for them. Medium/Large, true 1/2/4 workers, competing workloads and distributed W5 faults remain unassessed on this resource profile. The full matrix retains workload variants and faults separately; no missing row silently passes.

Read workloads share the same immutable Trino-prepared physical Silver input tables. Shared preparation is one separate report row. Spark W1–W3 use one application, with one separate startup/setup/shutdown overhead row; application-wide task metrics must not be summed three times. W4 completion includes independent consumer validation. Cache state is uncontrolled, and no cold-cache claim or p95 from three observations is made. These local timings cannot be treated as Azure runtime or a production placement decision.

## 5. Measured local comparisons and orchestration

The canonical 1,000-incident source contains **1,138,550 logical bytes**. Airflow DAG run `plan09-local-verified` passed both `execute` and `validate`, each on its first attempt. The common runner took **197.66 seconds** and retained 28 records: one shared preparation, three Spark application-overhead rows, and three repetitions of W1–W4 per engine. Source, query/job, publication and criteria evidence is under `data/poc-airflow-verified/`.

| Workload | Trino median seconds (range) | Spark median seconds (range) |
|---|---:|---:|
| W1 scan/filter | 0.025 (0.022–0.031) | 3.331 (2.944–3.344) |
| W2 join | 0.125 (0.112–0.136) | 1.743 (1.717–1.869) |
| W3 window/duration | 0.083 (0.073–0.086) | 0.522 (0.498–0.536) |
| W4 complete BSG + independent consumer validation | 20.459 (20.184–22.278) | 25.480 (23.494–28.186) |

Shared preparation took 26.83 seconds once. Spark W1–W3 application overhead was a separate median 3.99 seconds; W4 already includes its own application lifecycle. These are Smoke measurements on this layout/cache/resource profile. No required-size material Spark advantage or monetary placement conclusion follows.

The first scheduled experiment (`plan09-local-final`) caught a report-construction bug: summing repeating Decimal fractions for equal weights could differ from the multiplication used to calculate the final remainder. Reporting refused the invalid pool. Fixed-unit decimal weights now conserve exactly; regression coverage checks incremental report sizes 1–40. The failed evidence remains in `data/poc-airflow-final/`. Airflow made two task attempts against **one coordinator job**, demonstrating duplicate-request reuse rather than duplicate benchmark execution. The corrected run used a new workspace/run ID. Task attempt/state/timing evidence is in `data/poc-airflow-task-attempts.json`.

After restoring Trino, the isolated W2 concurrency diagnostic used the committed 10,000-incident shared input with explicit `--allow-failed-experiment`. It verified all 21 query results across nine randomized blocks:

| Concurrent queries | Repeated blocks | Median queries/second | Median attempt completion seconds |
|---|---:|---:|---:|
| 1 | 3 | 0.944 | 1.059 |
| 2 | 3 | 2.265 | 0.824 |
| 4 | 3 | 3.591 | 1.047 |

The four-way ratios were 3.81× throughput and 0.99× completion time. These are host-client, read-only Trino diagnostics on a custom size after restart, with uncontrolled caches; they do not satisfy the required Medium/cross-engine or concurrent-publication tests. Evidence: `data/poc-local-concurrency/concurrency.json`.

All **64 automated tests** passed on Python 3.12 and the documented dependency-free Python 3.9.6 command. A durable-event audit after restart found **245/245** referenced canonical Trino query IDs in the persisted collector, including dbt transformations. Spark application event logs and resource sampler artifacts remain alongside the phase evidence. Full details, hashes, timings, OOM state and unavailable costs are captured in the checked-in [compact result summary](../../benchmarks/results/poc-local-2026-10-22.json); raw generated evidence remains ignored by Git.

## 6. Accounting outputs and confidence

Every measured workload emits `economics/workload_costs.csv` and child `technology_costs.csv` rows. The component view includes Trino, Spark, dbt, Airflow, catalog/backend, object storage, residual container infrastructure, monitoring, network and optional current-state reference. `cost_ledger.jsonl`, allocation details, standing costs, billing reconciliation, `demand_sensitivity.csv` and Markdown reports retain the accounting basis and unavailable fields.

The real local experiment basis is `local_measured_usage`. **Its marginal, standing, fully allocated and cost/TB monetary values remain null/unavailable.** Wall time is diagnostic, not billed VM time; scan bytes do not price storage operations. Shared/idle cost is explicitly unavailable rather than zero. Missing required components prevent a complete workload total.

The independent arithmetic demo is deliberately `modeled_cloud` with a fictional provider, rates and quantities: nine component-hours × $2 = **$18**, allocated **$9 per engine**. It proves arithmetic and report conservation, not actual platform economics. Another hand-computed test verifies monthly demand arithmetic: the adopted marginal caps imply $525.60/month at 24 runs/day, with a separately supplied $720 standing scenario totaling $1,245.60. These are test scenarios, not observed costs.

Validation covers finite/nonnegative quantities, explicit credits, units/effective intervals, progressive tiers, duplicate charge identities, required components, retries, allocation weights, idle-only windows, zero-input denominators, source/mapping hashes, and actual-versus-amortized billing separation. Tiered usage sharing a resource/meter/window must be aggregated before allocation; the calculator refuses fragments that would restart the tier threshold. A reconciled label requires a successful comparable settled billing-scope check. Complete projected monthly totals require all tier prices and a priced standing window.

No Azure rate retrieval, provider usage collection or real bill was performed. The importer accepts explicitly normalized/mapped CSVs; it is not a live Cost Management collector. Operator labor is not converted into an infrastructure bill. Retention/maintenance/orphan costs remain missing measurements and cannot be hidden by the working accounting formulas.

## 7. Original hypothesis and gate status

| Requirement | Local evidence added | Full original verdict |
|---|---|---|
| H1 / G1 | Two engines, open tables and synthetic end-to-end BSG | Inconclusive / not assessed for Azure |
| H2 | Repeated local dbt/Trino workloads, correctness/recovery | Inconclusive pending required sizes, operations and economics |
| H3 | Equivalent selected Spark workloads available for paired comparison | Inconclusive; no decision-grade material benefit established |
| H4 / G2 / G3 | Both engines pass governed fixtures and the same pinned consumer | Local equivalence demonstrated; full matrix/Azure remain open |
| H5 / G6 | Tested conserved accounting, component outputs and explicit missing costs | Inconclusive / not assessed without measured/reconciled charges |
| G4 | Smoke publication failures, retry/replay and protected pins | Distributed M/L faults and bounded recovery remain open |
| G5 | Local repetitions, engine metrics, sampling and honest tier preflight | Full size, scale, concurrency and saturation evidence remain open |
| G7 | Traceable scorecard and operational limitations | Incomplete; no production recommendation |

The consolidated package at `data/poc-evidence-final/` retains **89 required cells**, all `not_assessed` at their required scope. Small preflight exclusions explicitly retain `resource_limited`; the custom failed comparison remains separate.

The eleven original deliverables are mapped in `benchmarks/specs/coverage.v1.json`. Working Azure environment, complete benchmark/economic comparison, M/L recovery, recurring operational effort and final production recommendation remain incomplete. BR-01–BR-15 retain separate applicability status; no production CDC, enterprise security or disaster recovery is implied.

## 8. Reproduction and continuing execution

```sh
make test
make local-poc-build
make local-poc-up
make local-poc-verify POC_WORKSPACE=data/poc-verify-new
make local-poc-benchmark POC_WORKSPACE=data/poc-compare-new
make local-airflow-up
```

Use fresh workspaces. The [benchmark/economics guide](../guides/poc_benchmarks_and_economics.md) documents Airflow triggering, restart verification, pricing/usage imports and evidence interpretation. A manually enabled comparative CI job is defined; hosted CI was not executed during this work.

The next local step is a separate resource profile with explicit aggregate memory headroom/service budgets, followed by rerunning the preserved 10,000-incident case and Small tier without changing their labels or acceptance limits. Check host capacity before choosing that profile; a larger heap alone does not address every process. Record ingestion/file-layout tuning separately from the preserved comparison. Decision-grade work also needs complete utilization/storage/maintenance/operations measurement, and separately authorized Azure compatibility and billing validation. Preserve prior acceptance versions and failed/resource-limited experiments. Only close H/G entries when their full acceptance evidence exists; the current recommendation remains **incomplete**.
