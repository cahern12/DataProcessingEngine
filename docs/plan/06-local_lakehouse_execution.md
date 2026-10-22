# Local Lakehouse Execution Record

Date: 2026-10-22. This record covers **phases 1–3**, the immediate implementation scope of [05 — Local Platform Improvement Plan](05-local_platform_improvement_plan.md).

The local milestone now runs source → Bronze → Silver → Gold through dbt, Trino, an Iceberg REST catalog, and object storage. The Python/SQLite reference path remains available. Configuration and rationale are in [decision 002](../decisions/002-local_iceberg_processing_stack.md); startup and commands are in the [local lakehouse guide](../guides/local_lakehouse.md).

## 1. Quality policy and failure evidence

Implemented attempt persistence before manifest validation and API access in the reference harness. Failed source checks retain available identity, stage, timestamps, and error details. Evidence files provide a fallback when the metadata location cannot be used.

The benchmark reads acceptance criteria once before generation/execution, retains that frozen configuration and its hash, distinguishes passed/failed/unevaluated criteria, and returns nonzero after preserving evidence when a required runtime criterion fails. Controlled-result tests verify failure behavior without depending on machine speed.

Synthetic expected outcomes now reside in `expected.v1.json`, outside the normal source manifest. Processing follows `rules/quality_policy.v1.json`, which defaults to a maximum rejected incident fraction of 10%. It can quarantine previously unknown bad records without advance defect declarations. The synthetic validator separately checks expected quarantine rule IDs and duplicate dispositions.

**Verification:** missing manifests, checksum failures, API failures, unavailable metadata locations, independent sidecars, runtime rejection, and strict quality thresholds are covered by regression tests.

## 2. Persistent local stack and compatibility spike

Installed and started a dedicated Colima runtime on the Mac. Added digest-pinned Compose services for PostgreSQL, RustFS, Nessie, and Trino, plus a version-locked Python/dbt runner image. Persistent volumes retain catalog and object data.

Executed the compatibility spike against the actual local stack:

- Created an Iceberg table and inserted records.
- Updated an existing record and merged matched/new records.
- Verified the exact resulting rows through Trino.
- Inspected Iceberg snapshot IDs and Parquet file metadata.
- Built a dbt model through the Trino adapter and passed its key tests.
- Restarted PostgreSQL, object storage, the catalog, and Trino; verified both Gold checksums and the mutated compatibility table after restart.

The same processing acceptance suite passed from the host virtual environment and the runner container. Source fingerprints are deterministic across their filesystem ordering, and committed container batches can be replayed from the host. The checked-in lock and Compose file reproduce the tested versions.

## 3. SQL transformations and consistent publication

Implemented dbt SQL models for normalization, version selection, reference joins, event histories, quarantine, hold/SLA calculations, and both Gold products. Added required-reference/schema checks, immutable v1 SLA-target validation, Gold key tests, and per-dataset reconciliation.

Expanded the independent fixture set from 51 to 53 cases. Exact daily expectations now cover reopened incidents, creation-priority preservation, reassignment/backlog history, late dimension updates, and a correction moving creation to an earlier date. All fixtures pass through both the Python reference and SQL implementations. Comparison reports identify dataset, key, and field, and distinguish missing fields from explicit nulls.

Raw source files and API pages, when used, land in object storage. Bronze records retain batch/file/row provenance. Each SQL attempt writes to an isolated namespace. Publication records both Gold snapshot IDs and changes the active pointer only after validation. Consumers resolve logical product names and may pin one publication ID across reads.

Injected failures between Gold writes and immediately before publication. Both preserved the previous active publication and its consumer-visible outputs. A retry published the delta successfully. Replaying either committed batch did not duplicate output or move the active publication backward. Prepared orphan publication manifests are excluded from committed ancestry.

## 4. Verification evidence

| Check | Result |
|---|---|
| Reference/unit suite | 31 tests pass, including live loopback pagination, publication edge cases, archived-manifest integrity, and portable source fingerprints |
| Independent business-rule fixtures | All 53 pass in Python and dbt/Trino |
| Initial SQL Smoke publication | 1,000 unique incidents |
| SQL delta publication | 1,100 unique incidents |
| Incremental-delivery/full-rebuild comparison | Both Gold products match; current SQL processing is still a full recomputation |
| Publication failures and retry | Previous publication preserved at both failpoints; retry succeeds |
| Replay | Already committed; active publication unchanged |
| Runner image | Builds successfully and passes the complete integration acceptance command |
| Service restart | Both Gold checksums and compatibility-table values unchanged |
| CI | Workflow added for unit and local-stack integration checks; no hosted CI run was initiated |

Canonical generated evidence is in `data/lake-final/verification.json` and `data/lake-final/restart.json`. Source batches, archived input, publications, attempts, query statistics, and dbt artifacts remain under the same workspace. The local reference demo and repeated baseline were also rerun under `data/reference-phase1/` and `data/reference-phase1-benchmark/`. Generated data and evidence are ignored by Git.

## 5. Remaining plan scope

| Plan phase | Status |
|---|---|
| 1 — Quality controls and evidence | Complete for the local milestone |
| 2 — Reproducible local stack | Complete, including restart persistence |
| 3 — Fixture-verified processing path | Complete |
| 4 — Targeted incremental processing | Pending; the SQL path currently recomputes full histories |
| 5 — Extended histories and new rules | Pending; multi-month feeds, calendars, effective-dated policies, and expanded assignment rules remain future work |
| 6 — Operator tooling and continuous verification | Partially implemented: stage/query/dbt evidence, Make targets, and CI workflow exist; dedicated status/run/quarantine/comparison commands and controlled-runner performance gates remain pending |

The next implementation step is phase 4: identify affected incidents and historical daily partitions, process them selectively, and compare each targeted result with a full rebuild. Azure, Airflow, Spark comparison, distributed scale/recovery, and cloud economics still require their own validation before a production recommendation.
