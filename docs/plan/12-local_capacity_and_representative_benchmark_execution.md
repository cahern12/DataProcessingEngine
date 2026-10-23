# Local capacity and representative benchmark execution

Dates: 2026-10-22–23 (America/Denver). Executes [11 — capacity plan](11-local_capacity_and_representative_benchmark_plan.md). Original requirements remain [01](01-enterprise_data_processing_poc_validation_plan.md). Baseline commit: `3963645`.

This record is being populated from real local experiments. A passed local experiment does not close the Azure, distributed scale, Medium/Large or billing gates.

## 1. Preserved baseline and isolated runtime

The original `dpe` Colima VM was stopped with its volumes retained. A separate `dpe-capacity` VM runs four CPUs and 16 GiB, using a fresh Docker daemon, catalog and object-store volumes. Existing pinned image layers were imported locally. The development host has 32 GiB physical memory and ten CPU cores. No Azure resources were provisioned.

The capacity helper is `infra/local/capacity.py`. The read-only host resource agent exposes fixed observation operations on loopback port 18113; workloads receive no Docker socket. A global filesystem lock excludes overlapping direct/coordinator producers. Kernel lock release after a producer crash is tested. Actual cross-container exclusion and duplicate request identity passed in `data/capacity-admission-integration.json`.

## 2. Chronological experiments

| Order | Experiment | Frozen selection | Observed result |
|---|---|---|---|
| 1 | `data/capacity-smoke-verification` | Acceptance v2, initial 14-GiB ceiling sum | Both engines pass all 53 fixtures, both publication failpoints, retry/replay, exact delta results and older pins |
| 2 | `data/capacity-custom-10000` | v2; Trino 3-GiB container / 2-GiB heap; 200-record inserts | **Failed.** Trino OOM-killed, exit 137, during the first paired Trino W4 ingestion. Preparation and the first Spark W4 had completed. Increasing VM memory alone did not remove the container limit |
| 3 | `data/capacity-custom-10000-trino4g` | v3; Trino ceiling raised to 4 GiB; same heap, input, SQL and 200-record inserts | **Passed.** All three paired W1–W4 repetitions; real Airflow DAG state `success`. Trino W4: 74.26 / 72.84 / 72.14 seconds; Spark W4: 37.64 / 38.94 / 37.38 seconds, including independent output validation |
| 4 | `data/capacity-small-unmodified` | v3, fixed 100,000 incidents | **Failed.** Producer killed with code -9 during preparation; its cgroup reached the 1.5-GiB ceiling. Captured `memory.events` confirms `oom=1`, `oom_kill=1`. Shared services remained running |
| 5 | `data/capacity-bounded-smoke-verification` | v4, stricter readiness admission | **Rejected before processing.** Airflow lacked a Compose health check. Added its verified database/scheduler/DAG-processor readiness check; the rejection is retained |
| 6 | `data/capacity-bounded-smoke-ready` | v4, bounded I/O, all service health checks | **Passed.** Both engines, fixtures, publication failures and replay |
| 7 | `data/capacity-custom-bounded-10000` | v4 | **Passed.** Three paired repetitions through Airflow. Sampled producer peak 128.76 MiB. Trino W4 42.44 / 46.86 / 50.80 seconds; Spark W4 37.78 / 40.28 / 42.17 seconds |
| 8 | `data/capacity-small-bounded-100000` | v4 | **Failed.** Producer stayed bounded, but Trino exhausted its 2-GiB JVM heap during preparation after 345 seconds. Exit 3, `OOMKilled=false`; retained logs explicitly report `java.lang.OutOfMemoryError: Java heap space` |
| 9 | `data/capacity-history-smoke` | v5; completed-query history 25, minimum age 5 seconds | **Passed.** Both engines, fixtures, publication failures and replay |
| 10 | `data/capacity-custom-history-10000` | v5 | **Passed.** 28 rows, including three paired repetitions through Airflow |
| 11 | `data/capacity-small-history-100000` | v5 | **Operator-aborted diagnostic.** Preparation published, but sustained major GC continued. An explicit interrupt ended the producer with exit 130; this is neither a watchdog timeout nor evidence of a cgroup OOM |
| 12 | `data/capacity-parquet-smoke` | v6; typed bounded Parquet landing and Iceberg `add_files` | **Passed.** Both engines, all 53 fixtures, cross-engine reads, initial/delta publication, both failpoints, retry/replay and older pins |
| 13 | `data/capacity-custom-parquet-10000` | v6 | **Passed.** 28 rows, including three paired repetitions; exact correctness passed. Benchmark lifecycle 319.59 seconds; this is not a single W4 timing |
| 14 | `data/capacity-small-parquet-100000` | v6 | **Rejected before processing.** Measured custom calibration projected 2,013,499,660 bytes of producer RSS against a 1,610,612,736-byte ceiling. The rejection and frozen preflight are retained |
| 15 | `data/capacity-producer2500-smoke` | v7; producer ceiling 2.5 GiB | **Passed.** Both engines' 53 fixtures, publication failpoints, retry/replay and old pins |
| 16 | `data/capacity-custom-producer2500-10000` | v7 | **Passed.** 28 rows, including three paired repetitions through Airflow |
| 17 | `data/capacity-small-producer2500-100000` | v7 | **Failed.** Admission passed, producer sampled peak stayed below 182 MiB, but Trino was OOM-killed during concurrent staging CTAS. Exit 137, `OOMKilled=true`; last sampled Trino usage 3.795 GiB against 4 GiB. State and logs retained |
| 18 | `data/capacity-native-staging` | Fresh 4-GiB Trino / 2-GiB heap, NMT summary | **Diagnostic setup failure.** Compiled dbt SQL retained a trailing terminator unsupported by the Python client. Corrected before the next diagnostic; this is not an engine capacity result |
| 19 | `data/capacity-native-staging-02` through `-05` | Same fresh/reused NMT-enabled engine, two concurrent staging statements per iteration | **Passed.** Both statements each iteration. Sampled RSS maxima rose approximately 2,549 / 2,924 / 3,097 / 3,212 MiB. These are staging-only diagnostics, not W4 repetitions |
| 20 | `data/capacity-native6g-smoke` | v8; 20-GiB VM, Trino 6-GiB ceiling / unchanged 2-GiB heap, NMT summary | **Passed.** Both engines, fixtures, publication failures, retry/replay and older pins |
| 21 | `data/capacity-custom-native6g-10000` | v8 | **Passed.** 28 rows, including three paired repetitions through Airflow |
| 22 | `data/capacity-small-native6g-100000` | v8 | **Passed.** 28 rows, three paired W1–W4 repetitions, exact correctness and Small runtime assessment passed; Airflow DAG success |
| 23 | `data/capacity-small-native6g-100000/disposition_audit.json` | Additional host-client audit, outside benchmark timings | **Passed.** 4,164,454 disposition rows across seven committed publications; exact dataset/ordinal/incident/disposition/rule-ID comparison, excluding free-text reason wording |
| 24 | `data/capacity-custom-native6g-concurrency` and `data/capacity-small-native6g-concurrency` | Host-client Trino W2, 1/2/4 clients, three repetitions per level | **Correct outputs.** Custom four-way throughput/completion ratios 1.760 / 2.101. Small ratios 1.177 / 3.094 miss the adopted 1.5 minimum throughput / 3.0 maximum completion reference targets; required Medium/cross-engine gate remains unassessed |
| 25 | `data/capacity-native6g-targeted-edges` | v8 Parquet/persisted-membership targeted implementation | **Passed.** Existing correction/quarantine/future/cutoff/duplicate scenarios, all targeted failpoints, exact independent results, replay, audit and reference-mutation refusal |
| 26 | `data/capacity-native6g-deliveries-full` | v8, 100,000 open-heavy incidents, full Trino/Spark strategies | **Failed on cutoff advancement.** Initial, sparse, late-correction, duplicate-only and future-deferred cases passed on both engines. Both publication failpoints preserved old pins and recovered on both full engines. Trino's cutoff-only daily-contribution aggregation exceeded its 768-MB per-node query allowance; Spark's cutoff case was not reached |
| 27 | `data/capacity-cutoff-memory-diagnostic` | Unchanged failed contribution SQL, session-scoped 1-GB request | **Failed.** The request did not override the server's 768-MB per-node ceiling. This executed diagnostic followed the earlier proposal whose approval review could not complete |
| 28 | `data/capacity-cutoff-memory1g-diagnostic` | v9 server per-node allowance 1 GB; same heap, unchanged contribution SQL, fresh destination | **Passed diagnostic.** 746,049 contribution rows in 25.645 seconds, reported query-memory peak 814,870,520 bytes (777.12 MiB), zero spill. This is an unpublished contribution-table probe, not a complete delivery or benchmark repetition |
| 29 | `data/capacity-query1g-smoke` and `data/capacity-query1g-targeted-edges` | v9 | **Passed.** Both-engine Smoke publication/fixture suite and targeted correction/quarantine/future/cutoff/duplicate/failpoint controls |
| 30 | `data/capacity-query1g-deliveries-full` | v9, original rule-ID SQL | **Gold/recovery passed, disposition audit failed.** All six deliveries passed on both full engines, including cutoff-only Trino 106.3 / Spark 137.5 seconds. The additional audit found duplicate reference dispositions missing `BR-002`, despite correct disposition counts and Gold outputs. This experiment is retained with its failed audit |
| 31 | `data/capacity-ruleids-smoke` and `data/capacity-ruleids-targeted-edges` | v9 resources, corrected rule-ID projection | **Passed.** Both engines pass all 54 fixtures and the Smoke publication suite; targeted edge/failpoint controls pass |
| 32 | `data/capacity-ruleids-deliveries-full` | Same v9 resources, distinct corrected implementation/image | **Passed.** All six cases on both full engines, both publication failures/recovery checks and replay/old pins. Every successful W4 meets 180 seconds. Additional disposition audit passes 6,742,626 rows across twelve publications |
| 33 | `data/capacity-ruleids-deliveries-targeted` | v9, corrected implementation | **Passed.** Six independently checked deliveries and replay/old pins, every W4 below 180 seconds. Selected populations: 100,000 initial / 1 sparse / 999 late / 0 duplicate / 1 future-deferred / 90,037 cutoff-only. Additional disposition audit passes 3,371,313 rows across six publications |

The initial table of ceilings remains in acceptance v2. Revision v3/v4 raises only Trino's container ceiling, making the sum of all declared ceilings 15 GiB with a nominal 1-GiB reserve. Direct and coordinator experiments cannot overlap; actual VM headroom and sampled usage are recorded separately. These bounds are calibration settings, not proof that simultaneous maxima are safe. No business runtime or economic target was relaxed.

### Repeated Small measurements

The v8 comparison uses identical shared physical inputs for isolated reads. Each row below summarizes three successful repetitions. Spark W1 is an additional observation. Spark application startup is recorded separately; W4 includes the complete processing path and independent output validation.

| Workload | Trino median seconds | Spark median seconds | Frozen Small limit seconds |
|---|---:|---:|---:|
| W1 scan/filter | 0.110 | 2.996 | 60 |
| W2 join | 1.139 | 3.219 | 120 |
| W3 window | 2.181 | 3.314 | 120 |
| W4 full processing | 117.818 | 136.026 | 180 |

Full-processing repetitions were Trino 120.590 / 117.818 / 117.790 seconds and Spark 136.026 / 133.486 / 136.762 seconds. The entire comparison lifecycle was 990.816 seconds. Sampled producer RSS peaked at 1,142.76 MiB; sampled Docker Trino memory peaked at 3,841 MiB against its 6-GiB ceiling. No service was OOM-killed in this experiment. Sample maxima do not establish the smallest safe container limit, and the earlier 4-GiB failure remains failed.

### Representative deliveries under v9

The corrected full and targeted campaigns have matching cumulative input-manifest checksums, implementation revision and v9 resource/acceptance hashes. Each cell is one observation, including independent output validation; these are separate from the repeated v8 comparison. Additional replay/pin and row-level disposition audits are outside these timings.

| Delivery | Full Trino seconds | Full Spark seconds | Targeted Trino seconds | Targeted selected incidents |
|---|---:|---:|---:|---:|
| initial | 126.663 | 144.459 | 133.636 | 100,000 |
| sparse_delta | 92.994 | 135.402 | 57.146 | 1 |
| late_correction | 103.558 | 144.965 | 67.162 | 999 |
| duplicate_only | 93.210 | 132.676 | 66.957 | 0 |
| future_deferred | 103.275 | 143.272 | 64.876 | 1 |
| open_heavy_cutoff | 104.757 | 140.225 | 93.403 | 90,037 |

All successful W4 deliveries meet the unchanged 180-second Small target. Gold checks, known changed-key checks, replay and older pins pass. Exact disposition audits compare 6,742,626 full-engine rows and 3,371,313 targeted rows across eighteen committed publications. Free-text reason wording is excluded.

Full-engine sparse publication failures preserve active pointers and old pins. Final injected error to independently validated retry is 96.411 seconds for Trino and 139.395 seconds for Spark. Earlier-failure intervals are 166.670 / 255.207 seconds because they include the next deliberate failure. Detection is a synchronous injected error; these are not infrastructure/distributed W5 or Medium/Large recovery results. Targeted failpoints are covered by the separate edge suite, not Small failure repetitions.

Targeted sparse and late selections are narrow, but landing, immutable retained-copy work and independent validation remain charged. The duplicate-only case selects zero incidents yet takes 66.957 seconds. Cutoff advancement selects 90,037 incidents despite adding no incident records. One matched observation does not establish a repeatable strategy ranking.

## 3. Implemented controls and tuning

Acceptance selection now flows through CLI, calibration, coordinator request identity, Airflow's prepare task and scorecards. Reports use the experiment's frozen criteria. Execution completion, exact correctness, runtime acceptance and cost completeness are distinct outcomes. Operational watchdogs are 300 seconds per isolated read and 1,800 seconds for preparation/full attempts; the Small 60/120/120/180-second business limits are unchanged.

The coordinator launches each experiment in a separate producer process and retains interrupted attempts. Spark gateway cancellation terminates its process group. SQL cancellation requests are sent when execution is interrupted. Source/startup failures and admission rejections remain inspectable.

Revision v4 changes source handling and transport, preserving v1 rule SQL and both Gold contracts:

- Repeatable file-backed source/provenance views avoid retaining several complete Python copies of archived histories. Integrity, row counts, source order and raw provenance remain checked.
- JSON serialization writes incrementally with byte-identical formatting. Independent read expectations are saved and loaded per workload.
- Trino batches up to 1,000 source rows with a 768-KiB estimated statement budget. This is an ingestion/layout change relative to the preserved 200-record baseline.
- Spark reads a file-backed raw JSONL spool and returns disposition counts for full runs; exact Gold rows are validated through the common pinned consumer. Independent fixtures retain their complete results. Read results use partition iteration in this profile.
- Targeted Trino can land bounded unpartitioned batches and create the bucketed delivery once, avoiding one bucketed file set per small INSERT. Its strategy remains separate from full rebuilds.

The tuned phase changes several related memory/transport/ingestion factors. Compare its aggregate results with v3; do not attribute every improvement to a single factor.

Revision v5 isolates a query-retention configuration change after observing sustained major GC and a JVM heap failure in v4. Trino's [query-history properties](https://trino.io/docs/current/admin/properties-query-management.html) retain completed queries for a minimum age, defaulting to 15 minutes. The new profile uses 25 queries / five seconds, keeping heap and ingestion settings unchanged. This tests a retention-pressure hypothesis; no heap dump establishes individual retained object ownership. A read-only query-history diagnostic was attempted during the v4 slowdown and was in flight near failure; its observation overhead is not excluded from that attempt.

Completed events remain in the external collector. If HTTP query history has expired, telemetry reads the persisted event's CPU, elapsed, IO, row and spill metrics. The event's user-memory peak is retained under its own name; total query memory remains unavailable rather than being inferred from that different measurement.

Revision v5's diagnostic histogram includes live and dead objects and cannot establish retained ownership. Its explicit stop is recorded in `observations/diagnostic_stop.json`. Revision v6 replaces large SQL source inserts with typed, bounded Parquet files imported through Iceberg `add_files`. The loader validates file schema and row counts, preserves raw payload, provenance, order and duplicates, and records file checksums. It retains the same business SQL, heaps and runtime targets. The new path requires the optional pinned `requirements-parquet.lock` dependency; legacy VALUES ingestion remains available.

Revision v7 raises the direct/coordinator producer ceilings to 2.5 GiB after v6's recorded admission rejection. Engine heaps and ingestion remain unchanged. Declared ceilings sum to 17 GiB because both producer services are listed; shared admission permits only one active producer. Active workload ceilings total 14.5 GiB, leaving 1.5 GiB nominal VM reserve. Actual headroom is checked independently. This is an explicit capacity experiment, not a bypass of v6's limit or a change to decision targets.

The campaign resume also fixed parsing of Airflow CLI output: startup logs precede the JSON run list. The original custom DAG had already succeeded; parsing its terminal state now allows the next phase to start. A regression test covers prefixed logs and missing results.

Revision v8 preserves CPU count, heap, ingestion and business criteria while increasing VM memory to 20 GiB and Trino's container ceiling to 6 GiB. Active workload ceilings total 16.5 GiB plus 3.5 GiB nominal VM reserve. The capacity VM was stopped/resized/restarted with volumes preserved; the original `dpe` VM remains separate. Filesystem observation records approximately 65 GB available on the capacity VM and 319 GB on the shared repository filesystem at the start of this phase.

Revision v9 raises only the server's per-node query-memory allowance from 768 MB to 1 GB; the global query allowance remains 1 GB, heap remains 2 GiB and heap headroom remains 512 MB. It retains the same business rules, container/VM limits and decision targets. The cutoff probe exceeded the old allowance slightly and completed under the new one. Full delivery, targeted processing and restart validation are being rerun in fresh `data/capacity-query1g-*` workspaces. The primary repeated Small comparison remains the v8 experiment; these configurations must not be pooled into one ranking.

The staging diagnostics show that a fresh engine can complete statements that failed in the preceding warmed campaign. They do not identify a single cause. NMT records heap, compiler, thread and other JVM categories; committed/reserved memory is not RSS, and untracked native allocations remain possible. Oracle's [NMT guidance](https://docs.oracle.com/en/java/javase/25/vm/native-memory-tracking.html) describes its coverage and an estimated 5–10% overhead. NMT is enabled for the entire v8 comparison, with additional read-only `jcmd`/OS samples and per-command collector durations retained under `observations/jvm_samples.jsonl`. Do not pool these timings with an uninstrumented profile. The first custom JVM sample started after the experiment began, so it does not establish a complete JVM-category peak for that run.

Targeted processing now persists large incident-membership sets rather than expanding them into oversized SQL literals. Small selections retain their static filters; reason inserts are bounded, and Parquet profiles can register the initial reason set directly. This changes targeted execution layout/planning, not business rules. Representative fault attempts are separately labeled W5, and recovery completion is recorded only after an independently validated retry. These changes are covered by unit tests and the real-stack delivery results above.

The v9 full-delivery audit exposed a separate annotation defect: both SQL implementations marked repeated reference rows as `duplicate` but emitted a null rule ID. The independent evaluator requires `BR-002`. Both SQL projections now retain that existing rule ID; Gold calculations and rule definitions are unchanged. The 54th fixture duplicates every reference dataset, and both engine fixture checks now reject any duplicate disposition lacking `BR-002`, including when output rows and disposition counts agree. A regression test demonstrates that rejection and the corrected result. Fresh `data/capacity-ruleids-*` runs use the same v9 resource/acceptance profile with a distinct implementation/image fingerprint. The earlier 53-fixture and Gold-only successes are not presented as evidence that the deeper audit passed.

Exact source packages for measured implementation revisions and immutable image IDs are preserved in `data/capacity-source-snapshots/index.json`. This retains intermediate source versions even as implementation work continues. Resource/acceptance versions and engine image/source identities remain separate provenance.

## 4. Observation boundaries

Each observed experiment records actual VM/container identity and limits, cgroup memory events, sampled producer RSS, Docker memory/CPU, stage timestamps, object inventories and protected publication metadata. The sampler targets two seconds; Docker collection adds overhead, and actual timestamps/gaps are retained. Sample maxima are not exact peaks. Storage byte-hours use declared interpolation. Namespace classifications do not prove object reachability; committed pins and failed/orphan artifacts are retained, and no garbage collection was performed.

Partial JVM category/heap summaries are available for v8. Complete native-memory attribution, provider-billed storage requests, cloud transfer quantities and recurring hands-on operating hours remain unavailable. Engine scan bytes are not ADLS transactions. Implementation/tuning work is not presented as recurring operator labor.

## 5. Public-price economics

Immutable public price pages, pagination URLs, retrieval times, exact product/SKU/meter identities and checksums are stored under `benchmarks/pricing/azure-eastus-2026-10-22/`. `dpe.pricing` verifies unique meter selection. `dpe.cloud_model` writes per-workload, technology and resource ledgers, plus twelve VM/AKS × Trino-only/with-Spark × low/base/high scenarios.

The base planning inventory is one shared Linux `Standard_D4as_v5`, two E10 LRS disks, one public IPv4 and same-region Hot LRS HNS storage. The separate AKS inventory adds two D4as_v5 system nodes, their OS disks, Standard control plane and NAT. These are declared planning inventories; Azure x86 performance and deployability have not been measured. Current [AKS system-pool guidance](https://learn.microsoft.com/en-us/azure/aks/use-system-pools) requires at least two system nodes and a VM SKU with at least four vCPUs. This POC inventory is not a production HA/DR quote.

The selected public rates include $0.172/node-hour, $9.60/E10 disk-month, $0.10/AKS-hour, $0.045/NAT-hour and $0.005/public-IP-hour. Capacity tiers, operation units and effective dates are preserved in the raw records. The [Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices) supplies public prices rather than negotiated charges. Azure Blob capacity uses [binary storage units](https://azure.microsoft.com/en-us/pricing/details/storage/blobs/); logical input TB remains decimal, 10¹² bytes.

The 20-GiB local envelope is not priced as the original 16-GiB application VM. Its declared application scenario uses `Standard_E4as_v5`, [4 vCPUs and 32 GiB](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/memory-optimized/easv5-series), with the captured Linux consumption meter at $0.226/hour. Separate AKS system nodes retain D4as_v5 at $0.172/hour. The new snapshot `benchmarks/pricing/azure-eastus-2026-10-23-capacity20/` preserves the new VM response and copies the other original snapshots without changing their retrieval metadata/checksums. The model refuses a stated application memory size smaller than the local envelope or inconsistent with the named SKU. Shape compatibility does not establish Azure performance or deployability.

Warm shared node charges are allocated once. Attributed active compute is a diagnostic within those charges, not an additional cash cost. Retained bytes are observed locally; retention duration, HNS index capacity, transaction mapping, disk operations, network maintenance allowance and local-to-cloud elapsed-time mapping are explicit assumptions. The base retention is seven days. Low/high scenarios vary runtime, prices, retention and request quantities; these are sensitivity assumptions, not statistical confidence bounds.

All attempt costs, including failed attempts and retained failed objects, remain in the declared ledger. Decimal allocation arithmetic conserves the ledger total. Trino-only retains the same application VM rather than assuming an unmeasured smaller shape, so removing Spark does not automatically reduce fixed VM cash cost.

The adopted 6/24/96 runs/day and 90/9/1% size mix remain frozen. Small-only demand tables are hypothetical alternatives. The full observed mix projection is unavailable without Medium/Large measurements. Actual Azure provisional/reconciled costs and recurring operations acceptance remain unavailable.

## 6. Commands and evidence

Use fresh workspaces. Preserve existing failed experiments.

The first three commands record the primary v8 invocation with its preserved source/image. A current image is a new implementation experiment; use a fresh prefix and matching calibration specification. Follow the [capacity guide](../guides/local_capacity.md) for current v9 verification, corrected delivery campaigns and consolidated reports.

```sh
make local-capacity-up POC_ACCEPTANCE=benchmarks/specs/acceptance.poc.v8.json
make local-capacity-verify POC_ACCEPTANCE=benchmarks/specs/acceptance.poc.v8.json POC_WORKSPACE=data/my-capacity-verification
python3 benchmarks/run_capacity_comparisons.py --acceptance benchmarks/specs/acceptance.poc.v8.json --prefix native6g
python3 -m dpe.capacity_reports --workspace data/capacity-custom-10000-trino4g
python3 -m dpe.cloud_model --workspace data/capacity-custom-10000-trino4g --snapshot benchmarks/pricing/azure-eastus-2026-10-22 --output data/my-retail-model
```

`run_capacity_comparisons.py` submits real Airflow runs and verifies DAG success, running custom before Small. It refuses to overwrite a failed or mismatched experiment. Large-run admission uses the named same-ingestion calibration workspace and records its estimated producer memory; it does not claim a capacity guarantee.

Automated verification currently includes 93 passing tests, including the optional Parquet tests, with additional real-stack outcomes retained separately. An additional row-level disposition auditor compares dataset, ordinal, incident ID, disposition and rule ID against the independent evaluator using streamed SQL results; its audit time is separate from timed workload execution. Hosted CI execution is not claimed by local test results.

Plan 11's local implementation/evidence milestone is **complete**. The [consolidated report](../../data/capacity-evidence-final/capacity_results.md) and [checked-in summary](../../benchmarks/results/poc-capacity-2026-10-23.json) retain separate correctness, performance, audit and economic outcomes. The original POC recommendation remains **incomplete**: seven of 89 required matrix cells pass, with 82 unassessed. The additional Small concurrency diagnostic fails its reference targets and does not close the required Medium/cross-engine concurrency scope.

The v8 cutoff failure and initial v9 disposition-audit failure remain visible. The first proposed memory probe was not executed because automatic approval review could not complete after a usage-limit error; subsequent work proceeded after the user's continuation. The executed session probe failed, the server-scoped probe passed, and the fresh corrected full/targeted campaigns subsequently passed. None of those later results rewrites an earlier failure.

The service restart receipt is `data/capacity-ruleids-service-restart.json`. PostgreSQL, object storage, catalog and Trino have verified later startup times and healthy readiness. Post-restart exact Gold audits pass all 54 publications: seven custom, seven Small, ten from the failed v8 campaign, twelve from the earlier v9 campaign, twelve corrected full and six corrected targeted. This establishes local committed-output durability, not continuous availability or distributed recovery acceptance.

The final package contains 180 complete declared planning scenarios across fifteen experiments, 2,358 per-attempt workload cost rows and 23,580 child technology cost rows. Every generated scenario ledger conserves its allocated total exactly. Two historical experiments remain unmodeled because a producer kill or admission rejection left insufficient durable storage inventories; missing quantities are not set to zero. The primary Small comparison has all twelve declared scenarios. Final consolidated CSVs retain a separate disposition-audit status so the initial v9 annotation defect cannot be hidden by passed Gold checks.

Base standing capacity is $185.55/month for the VM and $557.15/month for AKS, using a declared 720-hour month and the observed baseline storage stock. The corresponding low/high ranges are $148.44–$222.66 and $445.72–$668.58. Trino-only and with-Spark retain the same warm application shape; removing Spark therefore does not reduce fixed cash node cost in this inventory.

At 24 Small W4 runs/day, the base with-Spark alternatives allocate approximately $0.262/run on the VM or $0.778/run on AKS, including assumed marginal storage/operation charges. At six runs/day, AKS is approximately $3.10/run and **fails** the $2/run modeled target despite passing the $750 standing monthly target. These are alternative workloads using the entire warm platform, not additive engine workloads. `small_demand_sensitivity.csv` retains every workload/scenario/frequency and assessment. Shared read preparation and isolated-read Spark startup remain separate workloads.

Actual Azure execution, ADLS request/transfer meters, settled bills, recurring operator labor, Medium/Large and distributed worker/concurrency/fault coverage remain unavailable. The original 90/9/1 mixed-size projection is not extrapolated from Small. Its 113,412,943 logical bytes are below the 10-GB minimum for the per-logical-TB acceptance gate. Public-price scenario completeness does not make these measurement gates pass.
