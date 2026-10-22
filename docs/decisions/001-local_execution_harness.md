# 001 — Local development harness before distributed integration

Status: adopted for the initial local build, 2026-10-22.

The user requested local execution for now. The workspace has Python but no Docker, Azure CLI, Terraform, or dbt executable. We need reproducible source data and independently known results before validating engine-specific transformations.

Use Python's standard library for source generation, contracts, a mock REST source, and a business-rule reference evaluator. Use SQLite to persist raw records and publish both Gold products in one local transaction. Keep source schemas, business rules, expected-result fixtures, and output contracts separate from this execution implementation.

This choice provides a runnable development loop without external services. It adds no third-party runtime dependencies. Python and SQLite versions and source revision hashes are captured in run evidence. The checkout itself is the supported execution environment for this iteration.

Consequences:

- The local path validates domain behavior, source integrity, reconciliation, replay, and consumer publication semantics.
- The evaluator uses complete histories in memory. It is a correctness reference, not the proposed distributed compute engine.
- Delta publication is logically incremental, but locally it performs a full recomputation. This provides a baseline for future partition-targeted processing.
- SQLite transactionality proves the harness's publication mechanism. Iceberg will need its own snapshot/publication implementation and failure tests.
- No Azure deployment, Iceberg compatibility, Airflow scheduling, Trino/dbt execution, Spark comparison, or cloud economics gate is satisfied by these runs.
- The original architecture hypotheses remain open. No production architecture recommendation is made from this local build.

Next integration: provide a local container runtime, select and pin the catalog/metadata database and compatible Trino/dbt versions, then execute the storage/catalog compatibility spike from step 5. A local object-storage substitute must be identified as such; Azure ADLS compatibility and cost still require Azure validation later.
