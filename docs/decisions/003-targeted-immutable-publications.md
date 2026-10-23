# ADR 003: Targeted processing with immutable deliveries and publications

Date: 2026-10-22. Baseline: `3470c90` (plan) / `023e3f5` (verified local implementation).

A direct probe on Trino 484 and Nessie 0.109.0 failed to read the original snapshot after UPDATE: `Iceberg snapshot ID does not exists`. Shared mutable Gold tables cannot implement the existing pinned-consumer contract on this REST catalog.

Use an isolated namespace for each attempt. Seal incoming deliveries at their final Iceberg snapshot; never update committed tables. Each publication binds the ordered list of committed delivery tables, snapshots, manifest checksums, cumulative per-dataset row counts, and event-time cutoff. Failed attempts' tables are excluded from that list. Retry can leave orphan storage but cannot duplicate committed membership. The filesystem writer lock and atomic active-manifest replacement remain the local coordinator.

Each delivery stores raw JSON, source file/row, batch, dataset ordinal, normalized incident ID, event ID, source time, and schema validity. Parquet is partitioned by dataset and 32 incident-ID buckets. A separate bucketed event-identity index detects conflicts across incidents. A compact publication state index records future eligibility and time-dependent Gold state. Discovery uses those indexes and static predicates pushed into delivery tables; Python reads only the incoming batch and selected IDs. Full historical raw-byte audits are explicit `lake-audit` operations.

Recalculate the selected histories using the existing SQL business rules. Cache unrounded daily contributions per incident. Affected keys are the union of old/new selected contributions; aggregate every cached contributor to each key. Replace affected groups and remove groups losing their last contributor. Copy unaffected incident Gold, contributions, quarantine, dispositions, and daily Gold into immutable publication tables. This copy overhead is intentionally measured, including retained data reads and writes. It is a local correctness-first compromise, not a claim of zero-copy publication or sublinear total storage.

Full publication validation still reads the complete contracted outputs, population and quarantine indexes. The rejection denominator includes all committed identities. Incoming dispositions and full-history dispositions are reported separately. Identical deliveries with an unchanged cutoff select no incident histories. Reference snapshots must remain identical after normalization under v1; changed references require a new version/workspace.

Code, rules, quality policy, report start, and input identity are pinned. Code/policy changes require a new workspace. Raw deliveries remain available for a full SQL rebuild and independent Python evaluation. The default `lake-run` strategy is targeted; `--strategy full` retains the full-rebuild strategy in a separate workspace. No migration of old full-rebuild workspaces is implicit.

This strategy depends on immutable publication namespaces, not catalog time travel across mutations. Restart verification must read both old and current products through `lake-query` and compare their exact values. Storage cleanup, namespace compaction, distributed concurrency, and a catalog with durable historical snapshot access remain later work.
