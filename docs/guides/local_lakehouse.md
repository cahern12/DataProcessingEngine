# Local Lakehouse Guide

Run commands from the repository root. The Python/SQLite reference path remains available without these services.

## Prerequisites and startup

Provide a Docker-compatible runtime and Compose. The tested Mac uses Colima with 4 CPUs, 6 GiB RAM, a 30 GiB virtual disk allocation, and the Apple virtualization backend:

```sh
brew install colima docker docker-compose
colima start --profile dpe --cpu 4 --memory 6 --disk 30 --vm-type vz --mount-type virtiofs
make local-up
make local-build
```

Existing Docker Desktop or Linux Docker environments can use the same Compose file. If Compose is available as a Docker plugin, run Make targets with `DOCKER_COMPOSE='docker compose'`. Source checkout paths must be shared with the local runtime.

The tested services are Trino 484, Nessie 0.109.0 with a PostgreSQL 17.9 JDBC2 backend, and RustFS 1.0.0-alpha.81. Compose pins their image digests. The runner pins Python 3.12.12 and installs the complete `requirements-local.lock`; direct dependencies are recorded in `requirements-local.in`.

Services bind to localhost: Trino on port 18080, the catalog on 19120, and object storage on 19000. The credentials in Compose are dedicated local development values. Persistent catalog and storage volumes belong to Compose project `dpe-local`.

## Verify the stack

```sh
make local-verify
```

The acceptance command requires a new workspace. To repeat it:

```sh
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-verify --workspace data/lake-acceptance-next --count 1000
```

It verifies table create/insert/update/merge operations, Parquet file metadata, dbt execution/tests, all 54 independent fixtures, 1,000 initial incidents, a delta producing 1,100 incidents, both publication failpoints, replay, and comparison with a full reference rebuild. Original files are archived into the local object store; Bronze rows retain batch/file/row provenance. Generated evidence remains in the mounted `data/` directory.

The pipeline is a full-history SQL implementation at this milestone. Targeted incremental processing, extended histories/rules, and the complete operator command set remain phases 4–6 of plan 05.

## Process individual batches

```sh
docker-compose -f infra/local/compose.yaml run --rm runner \
  generate --out data/lake-custom-source --count 1000
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-run data/lake-custom-source/initial --workspace data/lake-custom
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-run data/lake-custom-source/delta --workspace data/lake-custom
```

Add `--quality-policy data/my-policy.json` to use a different rejection policy. A policy or transformation change requires a fresh workspace for an already committed batch. Synthetic `expected.v1.json` sidecars are validation inputs and are not read by normal publication.

## Consume a consistent publication

```sh
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-query --workspace data/lake-custom --dataset incident_resolution --limit 5
```

The response includes `publication_id`. Use that same ID with `--publication` when reading the other product to pin both reads to the same publication. Only committed publications are accepted; prepared orphan manifests are excluded.

Each publication records both Gold table identities and exact Iceberg snapshot IDs. The consumer resolves these through a local publication manifest. Independent Iceberg table commits become visible together only after the active pointer changes atomically. A writer lock serializes publication within one local workspace. This mechanism has not been validated as a distributed control plane.

## Recovery and persistence

Use a workspace that has committed the initial batch, then inject a failure:

```sh
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-run data/lake-custom-source/delta --workspace data/lake-custom \
  --fail-at between_gold_writes
```

If the delta has already committed, use a fresh workspace and publish its initial batch first. The failure command returns nonzero, stores attempt evidence, and leaves the previous active publication unchanged. Retry without `--fail-at` to publish the delta. `before_publish` is the other supported failure point.

Verify persistence without removing volumes:

```sh
docker-compose -f infra/local/compose.yaml restart postgres objectstore catalog trino
make local-up
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-query --workspace data/lake-custom --dataset incident_resolution --limit 5
```

`make local-down` stops services and preserves volumes. `colima stop --profile dpe` releases the Mac VM's CPU/memory resources. Source archives, table snapshots, and orphan attempt tables are retained; automated maintenance and retention are future work.

## Inspect evidence

The workspace contains `verification.json`, attempt JSON files, an atomic `active.json`, committed publication manifests, archived source batches, API landing pages when used, and dbt artifacts/logs. Attempt reports link Trino query IDs/statistics and dbt invocations to the publication.

The reference harness records early failures in its attempts table and a sibling evidence directory, with `data/evidence/` as the fallback when the metadata location is unavailable. Benchmark evidence distinguishes passed, failed, and unevaluated criteria. The integration workflow in `.github/workflows/validation.yml` is configured to retain container logs and generated evidence.

## Optional host virtual environment

```sh
/opt/homebrew/bin/python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-local.lock
.venv/bin/python -m dpe lake-compat --workspace data/host-compatibility
.venv/bin/python -m dpe lake-query --workspace data/lake-custom \
  --dataset service_daily_metrics --limit 5
```

On Linux, use the available Python 3.12 executable for virtual-environment creation. The default client endpoints target localhost ports. Container execution uses the internal service addresses through Compose environment variables. Source archive paths are relative to the workspace so they can be resolved from either environment.

## Targeted processing

`lake-run` now defaults to `--strategy targeted`; keep `--strategy full` in a separate workspace for recovery/comparison. Code/policy changes require fresh workspaces. The reference SQLite strategy remains a full recomputation.

```sh
make local-build
make local-incremental-verify LOCAL_WORKSPACE=data/incremental-next
```

This target runs `benchmarks/collect_query_io.py`, which starts the acceptance command and observes Trino statistics before query history expires. `verification.json` contains exact SQL/Python equivalence and frozen numeric gates; `file_io.json` contains per-stage file/cache/external reads, all attempt data-file inventories, and daily contributor expansion. Observer/inventory overhead is separate from measured processing. Cache conditions are uncontrolled, and physical scanned bytes can come from Trino's cache. `query_file_io.json` retains the underlying query counters.

Restart and verify every committed parent/current product pair plus active delivery/state/disposition checkpoints:

```sh
docker-compose -f infra/local/compose.yaml restart postgres objectstore catalog trino
docker-compose -f infra/local/compose.yaml up -d --wait postgres objectstore catalog trino
make local-incremental-restart-check LOCAL_WORKSPACE=data/incremental-next
```

Old snapshots of a mutated table fail through this catalog, so publications use isolated immutable tables. Unchanged rows are copied; storage grows with publication history and failed attempts until explicitly cleaned up. Snapshot IDs still pin each sealed table. See [ADR 003](../decisions/003-targeted-immutable-publications.md).

Empty incident/event deliveries use explicit zero-byte JSONL files and zero row counts in the normal six-dataset manifest. Complete unchanged reference snapshots remain required. `lake-audit --workspace ...` separately validates all archived source bytes; normal targeted runs validate only incoming raw bytes and rely on pinned immutable prior delivery tables.
