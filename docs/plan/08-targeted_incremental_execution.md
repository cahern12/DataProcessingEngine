# Targeted Incremental Execution Record

Date: 2026-10-22. Implements [07 — Targeted Incremental Processing Plan](07-targeted_incremental_processing_plan.md), phase 4 of [05](05-local_platform_improvement_plan.md). Baseline commits: `3470c90` (plan), `023e3f5` (local lakehouse). Exact implementation/criteria hashes and input manifests are retained in the verification evidence.

## 1. Storage and comparison contract

Preserved the full SQL rebuild as `lake-run --strategy full`. The targeted strategy is the `lake-run` default. Both use the existing v1 business rules, quality policy, reporting interval, and Gold contracts. Independent Python evaluation remains a second comparison strategy.

The real Trino/Nessie probe rejected the previous snapshot after UPDATE. [ADR 003](../decisions/003-targeted-immutable-publications.md) therefore chooses immutable delivery and publication tables. A publication seals source membership, manifest checksums, dataset row counts, cutoff, compact state, quarantine/dispositions, per-incident daily contributions, and both Gold snapshots. Only the atomic active-pointer replacement commits those checkpoints. Failed namespaces are excluded, and the workspace writer lock remains in force.

Incoming JSON/provenance is stored once in Parquet partitioned by dataset and 32 incident-ID buckets. Static incident predicates reach the delivery tables before JSON parsing and event-history reconstruction. Historical raw-byte audits are explicit separate operations. Unchanged output/cache rows are copied to isolated publication tables, with read/write/file overhead measured rather than omitted.

## 2. Candidate discovery and selective transformations

Candidate tables retain incident IDs and multiple reasons. Discovery detects novel source records, event-identity conflicts across incidents, previously future-dated records, advancing open/hold and missing-response clocks, and exact-cutoff reporting boundaries. Repeated identical records with an unchanged cutoff select no incident histories. V1 reference mutations fail before transformation.

Selected IDs receive complete committed histories, preserving creation priority, dimension corrections, holds, reopen cycles, late events, and quarantine semantics. Quarantining a previously valid incident removes its old Gold/contributions. A corrected event-only quarantine becomes valid when its missing snapshot arrives. Quality thresholds use the complete population; incoming and full-history dispositions reconcile separately.

Daily SQL now produces reusable unrounded per-incident contributions. The union of selected incidents' previous/new date/service/priority keys identifies affected groups. Each group includes every cached contributor. Old groups losing their last contributor disappear, and empty SLA denominators remain null. Unaffected groups are copied without recalculating their histories.

## 3. Correctness, recovery, and restart

The deterministic integration sequence independently asserts candidate IDs for corrections, cutoff-only processing, future-only repeated deliveries, and duplicate-only delivery. It covers late reopen/re-resolution, service/priority and earlier creation-date corrections, a disappearing group, unknown-to-valid and valid-to-quarantined membership, and unchanged contributors sharing affected groups.

All four failpoints are exercised: after input persistence, during selective writes, between Gold writes, and before activation. Each preserves the previous active checkpoint and pinned products. Retry succeeds, old/current replays remain idempotent, and unsupported reference mutations preserve the active publication. Full raw-history audit runs separately.

Verification completed on the local stack, with **zero field/key differences** against both independent Python and preserved full SQL rebuilds for both workloads. The deterministic five-batch sequence also matches full recomputation after every publication. All 38 unit tests and all 53 Python/SQL rule fixtures pass.

Four publication failpoints preserve the previous products/checkpoints; retry and replay pass. After restarting PostgreSQL, object storage, Nessie, and Trino, all nine committed targeted publications (18 product reads) match their independent expected results. The three active input checkpoints also retain exact delivery membership, population indexes, and disposition counts after restart. Host-to-container replay also returns `already_committed` without moving the active publication.

Implementation fingerprint: `ebd6cbf5c82c14bd5bcdedd101050638e2e9d69206b0f59336b5d48a74c3b5ec`. Frozen criteria SHA-256: `16d1999f280e9e962f9b96f2daeebd91bce9ec863a5226e1b4edbb28f7e0643b`. Environment: Trino 484, Nessie 0.109.0, PostgreSQL 17.9, RustFS 1.0.0-alpha.81; Colima 4 CPUs / 6 GiB, Trino heap 2 GiB, dbt 2 threads. OS/cache conditions are uncontrolled.

## 4. Measured processing and publication work

Targets were frozen before execution: zero differences, controlled candidate fraction <=3%, physical processing-read ratio <1.0, and complete query telemetry. Processing includes candidate discovery, history selection, incident transformations, daily aggregation, and checkpoint maintenance. Required validation and unchanged-output copying are included in total bytes/runtime.

| Workload / strategy | Processing bytes read | Total Trino bytes read | Trino bytes written | Scan-file reads | Written data files | Seconds |
|---|---:|---:|---:|---:|---:|---:|
| controlled / targeted | 875,855 | 1,252,950 | 247,811 | 371 | 72 | 19.20 |
| controlled / full | 4,454,027 | 4,600,024 | 1,102,205 | 124 | 46 | 18.80 |
| open_heavy / targeted | 6,994,471 | 8,218,691 | 1,079,458 | 1,166 | 71 | 19.68 |
| open_heavy / full | 4,229,946 | 4,360,158 | 1,042,172 | 122 | 44 | 19.11 |

- **controlled:** 11/1,001 selected incidents, 22 affected groups, processing-read ratio 0.197. Daily group recomputation aggregates 1,476 cached contribution rows from 674 incidents, including 663 unchanged contributors.
- **open_heavy:** 907/1,001 selected incidents, 576 affected groups, processing-read ratio 1.654. Daily group recomputation aggregates 7,491 cached contribution rows from 1,001 incidents, including 94 unchanged contributors.

The controlled workload demonstrates reduced data processing. It opens more small files than the full rebuild, and its extra publication/index work remains visible in the measurements. The open-heavy workload legitimately revisits most histories and costs more than a full rebuild. Runtime is reported as measured; no general runtime speedup is claimed. The existing generator emits at least one new incident even with `new_fraction: 0`, so each 1,000-incident initial workload has 1,001 identities after the delta; 10 existing incidents receive source changes. Candidate/quality accounting uses that actual population.

Scan-file reads sum actual Iceberg scan counters, including repeat reads of the same file. Written-file inventory includes every remaining data file in each attempt namespace, including intermediates and copied results. Physical bytes can come from cache; cache/external bytes are recorded separately. Per-stage query memory, CPU, spill, runtime, source manifests, file inventories, and daily expansion are retained. Both measured workloads required zero retries/operator interventions. The separate failure experiment deliberately injects four failures.

Trino byte totals exclude Python file validation and raw S3 landing; incoming source size is recorded separately, and those operations remain included in end-to-end runtime. Full raw audit and independent comparison/observer inventory costs are outside the measured processing path and reported separately. The isolation strategy still copies unchanged outputs and retains orphan namespaces from failed attempts. Smoke-sized local experiments do not establish larger-scale performance or storage economics.

Canonical evidence: `data/incremental-observed/verification.json`, `file_io.json`, `query_file_io.json`, and `restart.json`. Full SQL fixture evidence: `data/incremental-refactor-fixtures/fixtures.json`. Earlier exploratory runs are retained under `data/incremental-final/`; the observed run is the execution record's measured result. Generated evidence remains ignored by Git.

## 5. Commands and continuing work

```sh
make local-up
make local-build
make local-incremental-verify LOCAL_WORKSPACE=data/incremental-next
# Restart services and wait for health, then:
make local-incremental-restart-check LOCAL_WORKSPACE=data/incremental-next
```

The CI workflow runs reference tests, the existing SQL fixtures/Smoke verification, the incremental/file-IO suite, and parent/current product and input-checkpoint checks after restart. Hosted CI execution has not been initiated here.

Phase 5 remains future work: longer histories, business-hour calendars, effective-dated SLA policies, and expanded assignment rules. Namespace cleanup/compaction and distributed coordination remain separate work. Azure, Airflow, Spark comparison, large-scale capacity, and cloud economics are outside this local experiment.
