# 002 — Local Iceberg processing stack and publication

Status: adopted for the local POC, 2026-10-22.

Use the following stack for phases 2–3 of plan 05:

| Role | Tested component |
|---|---|
| Local Mac runtime | Colima profile `dpe`, Apple virtualization, 4 CPUs, 6 GiB RAM |
| Object storage | RustFS 1.0.0-alpha.81 |
| Iceberg REST catalog | Nessie 0.109.0 |
| Persistent catalog backend | PostgreSQL 17.9, Nessie JDBC2 storage |
| SQL execution | Trino 484 with the Iceberg REST connector and native S3 filesystem |
| Transformation framework | dbt-core 1.12.6 and dbt-trino 1.10.6 |
| Reproducible client runtime | Python 3.12.12 image with the complete dependency lock |

The Compose file pins service image digests and retains catalog/object data in named volumes. RustFS provides local S3-compatible storage; ADLS compatibility is a separate future validation. Apache Polaris's [current local example](https://github.com/apache/polaris/blob/main/site/content/guides/quickstart/docker-compose.yml) uses the same RustFS release. Our selected combination was tested directly rather than inferred from that example.

Nessie exposes the open Iceberg REST interface and persists catalog state in PostgreSQL. The backend configuration follows the [Nessie JDBC2 reference](https://projectnessie.org/nessie-0-109-0/configuration/). Nessie describes its [Iceberg REST integration](https://projectnessie.org/guides/iceberg-rest/) as experimental; the selection establishes a local POC path and leaves production catalog selection open.

The Trino configuration uses the documented [native S3 filesystem](https://trino.io/docs/current/object-storage/file-system-s3.html) with direct local storage credentials. The tested operation set includes table creation, inserts, updates, merges, snapshot queries, Parquet file inspection, dbt execution/tests, and restart persistence. External access is limited to localhost ports; the catalog does not use an external identity provider in this local environment.

Processing uses independent SQL models for source normalization, version selection, event reconstruction, quarantine, SLA measures, and daily products. The Python evaluator remains the comparison baseline. The SQL path currently fully rebuilds a complete batch history; targeted incremental processing remains phase 4.

For publication, use an immutable committed manifest recording both Gold table identities and exact Iceberg snapshots. A local writer lock serializes attempts. Atomic replacement of one local active pointer exposes the validated publication; consumers can pin a publication ID across products. Only the active publication's ancestry is considered committed, so a prepared orphan is not consumable. Every attempt writes to its own Iceberg namespace, preserving previously published snapshots during failures and retries.

This local publication coordinator does not validate a distributed control plane. Airflow integration, distributed concurrency, maintenance/retention, Spark interoperability, Azure access, and workload economics remain outside this milestone.
