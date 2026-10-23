# Local Capacity and Representative Benchmark Plan

Date: 2026-10-22. Implementation baseline: `3963645`. Continues [10 — POC execution record](10-poc_benchmark_and_economics_execution.md) and the remaining local work in [09](09-poc_hypothesis_benchmark_and_economics_validation_plan.md), against [01 — original requirements](01-enterprise_data_processing_poc_validation_plan.md).

**Next milestone: reliably complete repeated Trino/Spark comparisons at 100,000 incidents, explain the resource limits, and produce traceable modeled costs by workload and technology.** Work proceeds locally. This document defines proposed changes and experiments; it does not report them as implemented or executed.

## 1. Preserve the baseline and freeze the next experiment

The existing evidence establishes correctness and portability at Smoke scale, with important capacity and economics gaps:

| Baseline fact | Consequence for this phase |
|---|---|
| 64 automated tests and all 53 independent rule fixtures pass | Preserve v1 business semantics, quality policy and both Gold contracts |
| Three paired 1,000-incident repetitions pass through Airflow | Preserve this as the fast regression path |
| Trino was OOM-killed during the 10,000-incident comparison | Diagnose aggregate memory pressure; retain the failed comparison unchanged |
| Small generation produces 113,412,943 raw bytes, exceeding the old 64 MiB preflight | Introduce an explicit resource profile rather than bypassing or overwriting the historical limit |
| Local workload/component reports contain unavailable monetary amounts | Complete usage capture and a separately labeled price model |
| All 89 required full-scope matrix cells remain unassessed | Add evidence only to the cells actually exercised; preserve remaining gaps |

Retain the [compact baseline summary](../../benchmarks/results/poc-local-2026-10-22.json), original source manifests, failed attempts, snapshots and acceptance v1. Existing generated evidence stays under its original `data/` paths. Use fresh experiment workspaces and retain hashes for every new phase.

Keep the existing runtime, correctness, recovery and economic decision limits. Small remains 100,000 incidents; 10,000 remains `custom`. A faster result after changing resources or layout must identify that change. Performance targets must not be relaxed after observing a failure.

**Exit condition:** the new experiment manifest identifies baseline revision, source/configuration hashes, rules/contracts, resources, cache policy, repetitions and unchanged decision limits.

## 2. Create an isolated local capacity profile

The development Mac has 32 GiB physical memory and 10 CPU cores. Start with a separate **4-CPU / 16-GiB** runtime profile, proposed name `dpe-capacity`. Keeping CPU capacity fixed isolates the initial memory change. Check current host memory pressure and available disk before starting it; physical capacity alone does not establish available headroom.

Preserve the original `dpe` profile, volumes and configuration. Stop the original stack/profile before using the capacity profile so both environments do not compete for memory or host ports. Build fresh object-store/catalog state in the new runtime; source files and saved evidence may be reused by checksum, but publication metadata must point to tables that exist in that runtime. Do not replace or delete baseline volumes.

Start with these calibration budgets, expressed in GiB:

| Component | Initial container ceiling | JVM heap, where applicable |
|---|---:|---:|
| Trino | 3.00 | 2.00 |
| Spark gateway and its single active application | 3.00 | 2.00 |
| Airflow | 1.50 | — |
| Nessie catalog | 1.25 | 0.75 |
| PostgreSQL | 0.50 | — |
| Object storage | 1.50 | — |
| Benchmark coordinator | 1.50 | — |
| Direct runner | 1.50 | — |
| Completed-query collector | 0.25 | — |
| **Container ceilings combined** | **14.00** | |
| **VM/kernel/runtime headroom** | **2.00** | |

These are initial bounds to validate, not a capacity guarantee. JVM heaps exclude native buffers, stacks, Python workers and other processes. Account for the Spark application within its gateway container; the standalone Spark service must not run alongside it in this profile. Avoid image builds and unrelated verification jobs during measurements.

Allow only one benchmark experiment at a time across direct and Airflow submissions, including different workspace names. The current coordinator serializes its own queue but does not exclude a separate direct runner. Add shared admission control, persist its owner, and verify that retries and process exit cannot leave an experiment falsely active. This is local benchmark coordination, separate from the future distributed publication mechanism.

Capture actual container limits, JVM settings, CPU availability, VM memory, disk space, runtime identity and active services. If calibration requires different budgets, create a new named profile revision before its next measured run and preserve the earlier outcome.

**Exit condition:** health checks and the Smoke fixture/publication suite pass within documented service limits, with the complete stack present and no overlapping experiments.

## 3. Make acceptance and resource selection explicit

The runner, calibration, scorecard and status commands currently load `acceptance.poc.v1.json` directly. Add explicit selection before larger experiments:

1. Introduce a versioned capacity specification, proposed `benchmarks/specs/acceptance.poc.v2.json`, retaining the v1 business/engineering limits and recording the new resource profile.
2. Use a provisional **128 MiB raw-input preflight** for this profile. The existing Small source fits that bound. Admission must also check measured memory amplification, service health and available capacity; raw bytes alone did not prevent the earlier OOM.
3. Carry the selected specification and hash through CLI, calibration, coordinator, Airflow request identity and reports. Existing commands retain their historical default until callers explicitly select the new profile.
4. Save the complete specification before source processing. Scorecards and later `poc-status` inspection must use that experiment's frozen criteria, even if repository defaults subsequently change.
5. Separate execution completion, correctness, performance and cost completeness. A completed benchmark that misses a runtime limit must retain a failed performance assessment. Three technically successful repetitions do not override failed decision criteria.
6. Record rejected preflights, startup/source failures, OOMs, timeouts and failed-only logical workloads before attempting publication. Reports must remain inspectable if optional costing or telemetry collection fails.

Freeze operational watchdogs separately from business targets: initially 5 minutes per isolated Small read and 30 minutes per preparation/full-pipeline attempt. These bound diagnostic execution; the original 60-second W1, 120-second W2/W3 and 180-second Small W4 targets still determine performance outcomes. Retain elapsed work and all-attempt costs on interruption.

**Exit condition:** direct and Airflow paths freeze the same selected criteria, unsupported profiles fail clearly, and missing or failed metrics cannot silently pass a required check.

## 4. Integrate stage-level resource and storage observation

Make observation part of the experiment lifecycle rather than a separately started optional process. Start collectors before preparation, retain them through final validation, and stop them on success, failure or timeout.

| Measurement | Required detail |
|---|---|
| Time | Queue/startup, source verification/landing, ingestion, Bronze/Silver/Gold, validation, publication and usable-output time |
| Memory | Per-container usage/limits, producer RSS, JVM heap/native indicators where available, Python/Spark worker processes and VM pressure/OOM events |
| CPU/capacity | Sampled utilization, task/query CPU time, allocated resources and collector duration, with distinct units |
| Data layout | Logical input bytes, compressed table bytes, file count/size distribution, partitioning and repeated scan/write activity |
| Storage | Raw, intermediate, published, historical and failed/orphan objects; byte-hours over a declared observation interval |
| Requests/network | Available storage operation classes/counts, retries and transfers; explicitly unsupported measurements remain unavailable |
| Operations | Manual interventions, tuning changes, recovery actions and hands-on time; keep implementation labor separate from recurring operation |

Correlate measurements with experiment, logical run, attempt, engine application/query/dbt invocation and publication IDs. Include shared preparation, application startup, validation and collector overhead. Application-wide Spark task totals remain one application total across W1–W3.

A two-second sampling target is suitable for the first profile; record actual timestamps, gaps and collector overhead. Sample maxima are not exact peaks. Engine scan bytes are not storage request counts, and Docker memory counters are not a complete VM memory inventory.

Inventory protected publications before any cleanup. This phase may measure candidate cleanup/retention policies and perform explicitly scoped temporary-object cleanup, but must retain committed pins, required lineage and failed-run evidence. Record retained/orphan costs even when cleanup is deferred.

**Exit condition:** the next failure or slowdown can be located by component and stage, and required missing measurements are visible in the report.

## 5. Reproduce the 10,000-incident comparison on the new profile

Use the preserved logical source/configuration and frozen engine versions, SQL, ingestion settings and layout. First change only the runtime resource envelope; the existing Trino source loader's batches of 200 records remain part of this baseline.

1. Verify source hashes, table state and complete-stack health in a fresh workspace.
2. Run the existing shared-input preparation and exact W1–W4 comparisons on both engines, with three measured repetitions and deterministic interleaved order.
3. Keep caches labeled uncontrolled; do not claim a cold run without implementing and verifying a cache policy. Record any declared warmup separately.
4. Confirm no OOM, loss of the common consumer, partial publication or unexplained field/key/disposition differences.
5. Exercise both publication failpoints, retry, old/current replay and older pins. Restart storage/catalog/Trino and repeat pinned reads.
6. Repeat the host-client W2 concurrency diagnostic at 1/2/4, preserving its separate client boundary. Do not relabel it as distributed worker scaling or concurrent W4 publication.

Compare the 6-GiB failure and 16-GiB outcome as different configurations. The failed baseline remains failed even if its sources or committed products are readable after recovery. Preserve unsuccessful repetitions and stop further submissions if shared services become unhealthy.

**Exit condition:** the custom comparison completes repeatably with exact outputs and bounded resources, or produces a documented component-level failure requiring a new profile/implementation revision.

## 6. Measure Small, then improve the demonstrated bottlenecks

After the custom run establishes a usable envelope, run the fixed **100,000-incident Small** source. Preserve its seed/distribution, cutoff, rules and full-rebuild strategy. Collect three measured repetitions of W1–W4 for both engines, including optional Spark W1 for continuity. Both read engines must use the same immutable physical inputs.

Attempt the unchanged implementation first where admission permits. A slow or failed baseline is useful evidence. If it cannot complete safely, retain that outcome before changing the implementation. Prioritize improvements using the measured stage/resource data:

| Candidate improvement | Evidence needed before selecting it | Required validation |
|---|---|---|
| Bounded source/archive validation and incremental hashing | Producer RSS or retained Python histories dominate | Same checksum/refusal behavior and raw provenance |
| Bounded expected-result comparison, streamed or externally sorted | Full output collection/comparison dominates memory | Exact key/field comparison, duplicates, nulls and missing-field distinctions preserved |
| Bulk/batched ingestion or a Parquet landing/import path | Many small SQL submissions or file creation dominate ingestion | Same raw membership, provenance, types, duplicates and dispositions |
| File size/layout changes | Small files, planning/metadata or scan overhead dominate | Fixed-layout baseline retained; both read engines receive the same revised files |
| Shuffle/partition/session changes | Query/job evidence identifies a specific execution bottleneck | Record changed settings, memory/spill and tuning effort |
| Isolated short-lived producer processes | Retained memory between repetitions accumulates | Retry identity, publication safety and evidence survive process exit |

Do not attribute the earlier OOM to one of these causes without measurement. Change one factor per comparison where practical. Use fresh namespaces; hash implementation and layout versions. Run relevant independent fixtures and publication/reconciliation tests after changes, then repeat the custom and Small comparisons.

Small technical success requires exact outputs, three successful repetitions, no OOM and retained usable publications. Assess its original runtime targets separately. If correctness/capacity passes and latency fails, report both outcomes; continue measured tuning without rewriting the failed result or moving the limits.

**Exit condition:** the report contains a complete repeated Small comparison, reproducible tuning findings and explicit correctness/performance verdicts. Medium/Large and distributed scale remain separate work.

## 7. Add representative Small delivery and recovery cases

Once initial Small processing is stable, add separately identified W4 cases for sparse deltas, open-heavy cutoff advancement, late corrections and duplicate-only deliveries. Include a cutoff-only empty delivery and future-record eligibility as correctness controls. Preserve v1 semantics and independently known expected changes.

Use full rebuilds for the equivalent Trino/Spark comparison. Report targeted Trino as an additional strategy with its own validation, selected population, immutable-copy overhead and complete workload denominator. Spark has no targeted incremental implementation in this phase.

Exercise both existing publication failures at Small, including the cost of failed attempts and recovery. Record detection, time to usable output, consumer availability, old pins and manual actions. Inspect behavior when the query/consumer service restarts; restoring readability must not rewrite a failed experiment as successful.

**Exit condition:** representative delivery cases and local recovery have exact independent results, all-attempt telemetry and stable pins. These are additional local findings; they do not close the required Medium/Large distributed W5 cells.

## 8. Bind captured usage to real public-price scenarios

Continue producing unpriced `local_measured_usage` reports. Add separate `modeled_cloud` scenarios using preserved public prices and explicit deployment/usage assumptions. Public price retrieval is read-only and does not require Azure deployment. Microsoft's [Azure Retail Prices API documentation](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices) describes regional/SKU rates, billing units and pagination; public prices are not negotiated invoice charges.

Use **USD / East US / Linux pay-as-you-go** as the initial planning scenario, separate from any eventual deployment selection. Select the actual VM/storage/service meters during implementation and freeze them before calculating costs. Start with a self-hosted shared-capacity scenario; model an AKS topology separately with its additional resources and overhead. A local 4-vCPU/16-GiB shape is a sizing assumption, not proof of equivalent Azure CPU architecture or runtime.

Implement these steps in order:

1. Retrieve all relevant price pages with exact filters. Preserve raw responses, URLs/query/API version, retrieval time, provider, region, meter/product/SKU IDs, purchase model, currency, effective dates and checksums.
2. Normalize units and progressive tiers into the existing rate catalog. Reject ambiguous SKU matches, wrong OS/region/currency and accidental reservation/spot mixing. Never select the first similarly named meter without checking its identity.
3. Define the proposed resource inventory and lifetime: engine capacity, dbt/producer, Airflow, catalog/backend, storage/retention/transactions, residual infrastructure, monitoring and applicable network resources.
4. Bind each quantity to measured evidence or a separately marked assumption. Local S3 operation counts require an explicit mapping/uncertainty model before pricing ADLS transactions; the protocols are not interchangeable meters.
5. Allocate each shared node/service charge exactly once. A fixed warm node's active compute allocation differs from additional cash cost for a run. Include standing/idle capacity, failed attempts, immutable copies, maintenance and the seven-day retention scenario.
6. Compare a Trino-only scenario with one retaining Spark. Recompute the resource inventory rather than charging the same shared VM twice under engine and infrastructure headings.
7. Show low/base/high runtime, rate, retention and allocation sensitivity, labeled as assumptions rather than statistical confidence bounds. Local elapsed time multiplied by a public VM rate is a scenario, not measured Azure workload cost.

Every modeled workload must expose marginal, attributed active compute, allocated standing, fully allocated and per-logical-TB costs, with child technology/resource rows and provenance. Separate measured, assumed and unavailable quantities. An explicit assumption may complete a modeled scenario without making measurement coverage complete; a missing required rate/quantity keeps the affected measured estimate incomplete.

Keep the adopted 6/24/96 runs/day and 90/9/1% size mix. Small observations cannot establish Medium/Large prices or performance. Leave the full observed mix projection unavailable until those tier measurements exist, or publish a separately labeled hypothetical demand scenario with every extrapolation stated. Actual Azure provisional/reconciled labels remain unavailable without Azure usage/billing evidence.

**Exit condition:** an independent reviewer can reconstruct each modeled Small workload/component cost, allocations conserve the declared scenario total, and estimates remain visibly distinct from local measurements and bills.

## 9. Assemble the evidence and assess the milestone

Retain existing artifact names where possible and add the following proposed outputs:

| Artifact | Purpose |
|---|---|
| Frozen resource/acceptance specifications | Reconstruct the baseline, capacity-only change and later tuned configurations |
| Stage resource timeline and failure diagnostics | Explain memory/CPU/IO pressure, OOMs, timeouts and observer gaps |
| Input/table/file inventories | Preserve logical equivalence and explain physical-layout changes |
| Repeated custom/Small reports | Exact correctness, median/range, runtime targets and unsuccessful attempts |
| Delivery/recovery/restart reports | Checkpoints, old pins, recovery duration and operator actions |
| Public price snapshots and scenario specifications | Preserve meter identity, assumptions and normalized rates |
| Workload/technology/resource cost views | Marginal/active/standing/total costs and uncertainty without double counting |
| Updated coverage and comparison summary | Identify the exact cells assessed and the evidence still missing |

Separate matrix coverage from acceptance: sufficient repetitions can establish coverage while runtime/cost limits still fail. Do not combine configurations, layouts, source distributions or acceptance versions into one ranking. Keep per-engine and complete-platform boundaries explicit.

Record execution in the next chronological document, proposed `12-local_capacity_and_representative_benchmark_execution.md`, with a compact checked-in result summary and raw artifacts under new ignored `data/` workspaces. Link the resulting reports from the README and retain the previous incomplete/failed packages.

The local milestone is achieved when the custom and Small comparisons are repeatable and correct, representative delivery/recovery checks pass, resource limits are explained, and complete declared-scope modeled Small costs are reconstructable. Report any missed performance/economic target as a failure even if the implementation milestone is otherwise complete.

H1–H5/G1–G7 remain assessed against their full original scope. This phase adds local evidence for correctness, capacity, recovery, technology fit and cost-model mechanics. Azure/ADLS behavior, required Medium/Large distributed faults, true 1/2/4-worker scale-out, complete concurrency coverage, settled billing and the final production recommendation remain open.

## 10. Implementation order and verification

The paths below identify proposed changes, not completed files or supported flags:

| Order | Implementation area | Verification |
|---|---|---|
| 1 | Versioned runtime/Compose resource overlay and profile startup helpers | Actual limits, aggregate headroom, healthy services and preserved baseline volumes |
| 2 | Acceptance selection in CLI, runner, calibration, coordinator/DAG and reports | Frozen-criteria replay, profile hashes and separate outcome types |
| 3 | Shared experiment admission and durable early failure records | Direct/Airflow overlap, duplicate submissions, process exit and missing-source cases |
| 4 | Integrated resource/stage/storage observers | Success, interruption, unsupported metrics, counter resets and retained diagnostics |
| 5 | Capacity-only custom comparison and initial Small attempt | Independent W1–W4 results, three repetitions, publication/restart invariants |
| 6 | Evidence-selected ingestion/validation/layout improvements | Relevant fixtures, exact streaming comparisons and measured baseline/tuned experiments |
| 7 | Small delivery/recovery cases | Correct population changes, complete dispositions, retry/replay and old pins |
| 8 | Public-price import and usage-to-scenario binding | Units/tiers, pagination/identity, missing values, allocations and assumption provenance |
| 9 | Reports, documentation and optional CI extensions | Traceable evidence, correct failed/incomplete statuses and no false Azure closure |

Reuse the existing 64-test and 53-fixture foundations. Add targeted tests for the new behavior rather than duplicating implementation details. Expand hosted CI only for appropriately bounded profiles, and distinguish a workflow definition from an actual hosted run. No cloud resources are provisioned by this plan.
