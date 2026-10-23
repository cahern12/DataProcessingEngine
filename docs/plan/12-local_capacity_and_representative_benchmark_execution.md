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
| 14 | `data/capacity-small-parquet-100000` | v6 | **Not completed at this checkpoint.** No terminal Small comparison is available; do not infer Small capacity from the custom result |

The initial table of ceilings remains in acceptance v2. Revision v3/v4 raises only Trino's container ceiling, making the sum of all declared ceilings 15 GiB with a nominal 1-GiB reserve. Direct and coordinator experiments cannot overlap; actual VM headroom and sampled usage are recorded separately. These bounds are calibration settings, not proof that simultaneous maxima are safe. No business runtime or economic target was relaxed.

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

Exact source packages for measured implementation revisions and immutable image IDs are preserved in `data/capacity-source-snapshots/index.json`. This retains intermediate source versions even as implementation work continues. Resource/acceptance versions and engine image/source identities remain separate provenance.

## 4. Observation boundaries

Each observed experiment records actual VM/container identity and limits, cgroup memory events, sampled producer RSS, Docker memory/CPU, stage timestamps, object inventories and protected publication metadata. The sampler targets two seconds; Docker collection adds overhead, and actual timestamps/gaps are retained. Sample maxima are not exact peaks. Storage byte-hours use declared interpolation. Namespace classifications do not prove object reachability; committed pins and failed/orphan artifacts are retained, and no garbage collection was performed.

Per-process native/JVM heap breakdown, provider-billed storage requests, cloud transfer quantities and recurring hands-on operating hours remain unavailable. Engine scan bytes are not ADLS transactions. Implementation/tuning work is not presented as recurring operator labor.

## 5. Public-price economics

Immutable public price pages, pagination URLs, retrieval times, exact product/SKU/meter identities and checksums are stored under `benchmarks/pricing/azure-eastus-2026-10-22/`. `dpe.pricing` verifies unique meter selection. `dpe.cloud_model` writes per-workload, technology and resource ledgers, plus twelve VM/AKS × Trino-only/with-Spark × low/base/high scenarios.

The base planning inventory is one shared Linux `Standard_D4as_v5`, two E10 LRS disks, one public IPv4 and same-region Hot LRS HNS storage. The separate AKS inventory adds two D4as_v5 system nodes, their OS disks, Standard control plane and NAT. These are declared planning inventories; Azure x86 performance and deployability have not been measured. Current [AKS system-pool guidance](https://learn.microsoft.com/en-us/azure/aks/use-system-pools) requires at least two system nodes and a VM SKU with at least four vCPUs. This POC inventory is not a production HA/DR quote.

The selected public rates include $0.172/node-hour, $9.60/E10 disk-month, $0.10/AKS-hour, $0.045/NAT-hour and $0.005/public-IP-hour. Capacity tiers, operation units and effective dates are preserved in the raw records. The [Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices) supplies public prices rather than negotiated charges. Azure Blob capacity uses [binary storage units](https://azure.microsoft.com/en-us/pricing/details/storage/blobs/); logical input TB remains decimal, 10¹² bytes.

Warm shared node charges are allocated once. Attributed active compute is a diagnostic within those charges, not an additional cash cost. Retained bytes are observed locally; retention duration, HNS index capacity, transaction mapping, disk operations, network maintenance allowance and local-to-cloud elapsed-time mapping are explicit assumptions. The base retention is seven days. Low/high scenarios vary runtime, prices, retention and request quantities; these are sensitivity assumptions, not statistical confidence bounds.

All attempt costs, including failed attempts and retained failed objects, remain in the declared ledger. Decimal allocation arithmetic conserves the ledger total. Trino-only retains the same application VM rather than assuming an unmeasured smaller shape, so removing Spark does not automatically reduce fixed VM cash cost.

The adopted 6/24/96 runs/day and 90/9/1% size mix remain frozen. Small-only demand tables are hypothetical alternatives. The full observed mix projection is unavailable without Medium/Large measurements. Actual Azure provisional/reconciled costs and recurring operations acceptance remain unavailable.

## 6. Commands and evidence

Use fresh workspaces. Preserve existing failed experiments.

```sh
make local-capacity-up POC_ACCEPTANCE=benchmarks/specs/acceptance.poc.v6.json
make local-capacity-verify POC_ACCEPTANCE=benchmarks/specs/acceptance.poc.v6.json POC_WORKSPACE=data/my-capacity-verification
python3 benchmarks/run_capacity_comparisons.py --acceptance benchmarks/specs/acceptance.poc.v6.json --prefix parquet
python3 -m dpe.capacity_reports --workspace data/capacity-custom-10000-trino4g
python3 -m dpe.cloud_model --workspace data/capacity-custom-10000-trino4g --snapshot benchmarks/pricing/azure-eastus-2026-10-22 --output data/my-retail-model
```

`run_capacity_comparisons.py` submits real Airflow runs and verifies DAG success, running custom before Small. It refuses to overwrite a failed or mismatched experiment. Large-run admission uses the named same-ingestion calibration workspace and records its estimated producer memory; it does not claim a capacity guarantee.

Automated verification currently includes 86 passing tests, including the optional Parquet tests, with additional real-stack outcomes retained separately. An additional row-level disposition auditor compares dataset, ordinal, incident ID, disposition and rule ID against the independent evaluator using streamed SQL results; its audit time is separate from timed workload execution. Hosted CI execution is not claimed by local test results.

Plan 11 remains incomplete: a successful repeated 100,000-incident comparison, representative delivery/recovery cases at that scale, post-restart audits, concurrency measurements and the consolidated final assessment are still pending. The representative runner and evidence assembler are implemented, but their existence does not establish those results. Public-price scenarios are modeled economics; actual Azure execution, bills and the original Medium/Large gates remain unassessed.
