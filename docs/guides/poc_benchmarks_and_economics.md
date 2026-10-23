# Local comparative benchmarks and economics

Run commands from the repository root. This extends the [local lakehouse guide](local_lakehouse.md). The acceptance targets and required matrix are under `benchmarks/specs/`; the original Azure POC remains incomplete until its required evidence exists.

## Reproduce local acceptance

```sh
make local-poc-build
make local-poc-up
make local-poc-verify POC_WORKSPACE=data/verification-new
make local-poc-benchmark POC_WORKSPACE=data/comparison-new POC_COUNT=1000 POC_REPEATS=3
```

The Spark image pins Spark 3.5.5, Iceberg 1.8.1 runtime/S3 bundles and their SHA-256 digests, plus Jinja2 3.1.6 and MarkupSafe 2.1.5. Its upstream Python is 3.8, while the host/Trino runner uses Python 3.12. It uses UTC, `local[4]`, a 2 GiB driver heap and two shuffle partitions on the same 4 CPU / 6 GiB VM as Trino's 2 GiB heap. This is a local single-process comparison, not equal-shape distributed worker scaling. Current native/Python overhead and other services share that envelope; hardware utilization and billed capacity must be measured before cloud conclusions.

An actual paired experiment found that the pinned Iceberg vectorized reader could not decode a Trino-written `DELTA_LENGTH_BYTE_ARRAY` string column. Spark therefore disables Iceberg vectorization using the documented [Spark configuration property](https://iceberg.apache.org/docs/latest/spark-configuration/). Preserve this setting in comparisons and record the tuning/performance implication; the first failed experiment is retained separately. The compatibility probe is intentionally small; the real W2/W3 datasets also exercise strings, timestamps and joins.

Full Spark processing uses Spark SQL for deduplication, state/event sequencing, SLA calculations, daily aggregates and Gold creation. Python helpers supply the shared normalization/schema policy and an independent verification oracle. Spark outputs are Iceberg tables in immutable attempt namespaces. Both engines activate a two-product publication atomically in the local coordinator and use the same Trino consumer to read snapshot pins. Spark does not claim targeted incremental execution.

After a service restart:

```sh
docker-compose -f infra/local/compose.yaml restart postgres objectstore catalog trino
make local-poc-up
make local-poc-restart-check POC_WORKSPACE=data/verification-new
```

Publication/checkpoint files are local single-writer state. A distributed Azure coordinator and distributed fault injection remain future validation. Failed attempts and orphan tables are retained as evidence; this iteration does not implement automatic retention/compaction or claim its cost is measured.

## Interpret experiment artifacts

`specification.json` freezes code/criteria hashes, source count, resources, cache policy, ordering and orchestration. Engine order is interleaved with a fixed seed. There is no forced cold cache or declared unmeasured warmup; results describe actual uncontrolled-cache runs. Queries/jobs, plans, snapshots, source archives, dbt evidence, independent comparisons and Spark event logs remain in each workspace. A stale Spark image fails the implementation-hash check.

W1 filters/projects snapshots; W2 joins latest snapshots, references and event counts; W3 orders events with windows and durations; W4 builds both complete Gold products. Both read engines use identical physical Silver tables. A separate `PREPARATION` row accounts for preparing shared inputs once. Spark's W1–W3 share an application, so its setup/shutdown is one `APPLICATION_OVERHEAD` row. Do not add the application's task totals three times. W4 wall time includes source ingestion, processing, governed publication and independent consumer validation. Preparation and validation have different boundaries from isolated reads; compare like rows.

`cost_performance.csv` groups repetitions with median/min/max. Three successful repetitions are the minimum for a local ranking, not a basis for a p95 or production architecture decision. `matrix.csv` explicitly lists S/M/L baseline, W4 variants, W5 faults and representative worker/concurrency cells. Smoke is 1,000 incidents; other sub-Small counts remain local development inputs, not renamed Small. `poc_scorecard.json` separates local quality/runtime assessments from the complete H1–H5/G1–G7 verdicts.

Trino's [HTTP completed-query listener](https://trino.io/docs/current/admin/event-listeners-http.html) persists events outside expiring query history in `data/telemetry/trino-events/`. Correlate `metadata.queryId` with `query_id` in attempt/read artifacts and dbt logs. [Spark event logs](https://spark.apache.org/docs/latest/monitoring.html) preserve application/job/stage/task identities and task IO, CPU, shuffle/spill and memory metrics. Event parsing reports explicit unsupported fields. Task peak execution memory is not process RSS. Neither engine's scan bytes are Azure storage transaction counts. An optional host sampler is described below. Billable storage requests, retained/orphan byte-hours and operator labor are not yet comprehensively measured; comparisons requiring those fields remain incomplete.

A `poc-calibrate` preflight generates source files and records actual rows/raw bytes without asserting processing capacity:

```sh
python3 -m dpe poc-calibrate --workspace data/calibration-new --count 100000
```

The frozen 64 MiB raw input budget bounds full-history source materialization, but cannot guarantee sufficient memory for all native processes/services. The attempted 10,000-incident comparison OOM-killed Trino despite passing preflight; its failed result is retained in the execution record. A resource-limited preflight is not an attempted engine failure or a measured maximum. Medium/Large and true 1/2/4-worker profiles require another controlled resource envelope; this implementation does not emulate workers with local Spark threads.

## Use the accounting tools

```sh
python3 -m dpe cost-demo --workspace data/arithmetic-new
python3 -m dpe cost-report --usage path/to/usage.json --rates path/to/rates.json --out data/modeled-report
python3 -m dpe cost-import --csv path/to/cost-details.csv --mapping path/to/mapping.json --out data/imported-usage.json
python3 -m dpe cost-report --usage data/imported-usage.json --out data/billing-report
```

The arithmetic demo uses fabricated prices and quantities, explicitly labeled `modeled_cloud` and `synthetic-test-only`. Its $18 scope / $9 per engine checks conservation; it does not price the real platform. No retail price snapshot has been collected, and no Azure estimate or bill is implied by those numbers.

`usage.json` contains logical runs, charge lines, shared pools and required components. Each line needs a unique line/charge/resource/meter/window identity, component, source reference, start/end and either explicit bill amount or quantity/unit/rate ID. A marginal line names its logical run; a standing line names a pool. Pool weights sum exactly to one, with a declared driver. Empty pools retain idle-only cost and prevent completeness. Retried/failed attempts belong to their logical run. A fixed warm VM's allocated compute is not necessarily incremental cash cost; `active_capacity` reports attributed active compute separately.

Component owners are Trino, Spark, dbt, Airflow, catalog/backend, object storage, residual container infrastructure, monitoring, network and optional current-state reference. Assign each underlying charge once. Split shared-node charges using a documented capacity/allocation model and reconcile the split to the provider total. Do not charge the full node under both the engine and infrastructure. Iceberg/Parquet are formats; their consequences belong to storage/compute/maintenance meters.

A normalized rate snapshot supplies `currency`, `basis`, `provider`, `region`, `effective_from`, `effective_to`, `source` and `rates` (ID, normalized unit and ascending progressive tiers with start/price). Preserve purchase model, meter/SKU IDs, retrieval time, source URL/file and hashes when preparing real prices. Apply unit conversions explicitly; CPU seconds cannot be bound directly to a node-hour rate. Rates outside the interval or with mismatched currency/unit/basis are rejected. Missing rates/quantities remain `null`; known zero is different from missing data. The checked-in synthetic rate catalog is a format/arithmetic example only. Use the [Azure Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices) later for labeled public-price scenarios.

The CSV importer requires explicit mappings, such as the following shape (replace every example identity and complete the scope):

```json
{
  "billing_basis": "actual",
  "currency": "USD",
  "window_id": "export-window",
  "started_at": "2026-10-22T00:00:00Z",
  "finished_at": "2026-10-23T00:00:00Z",
  "allocation_policy_version": "reserved-capacity.v1",
  "columns": {"charge_id": "unique_export_line_id", "resource_id": "resource_id", "meter_id": "meter_id", "currency": "currency", "amount": "cost"},
  "resources": {"example-resource": {"component": "trino", "classification": "marginal", "logical_run_id": "example-run", "category": "compute"}},
  "runs": [{"logical_run_id": "example-run", "workload_id": "W4", "engine": "trino", "logical_input_bytes": 1000000000000}],
  "required_components": ["trino", "dbt", "airflow", "catalog", "object_storage", "container_infrastructure", "monitoring", "network"],
  "pools": []
}
```

An export row may span many workloads. Map it to a standing pool with explicit weights when appropriate; the importer does not infer per-query billing. Unknown resources, duplicate provider charge identities and mixed currencies/billing bases fail. Preserve the original export and mapping hashes. If the provider does not supply a unique charge-row identity, create and document one during normalization before import. The generic importer is not an Azure API collector.

A `billing_scope` contains `currency`, `basis` (`actual` or `amortized`), `total`, `source_ref` and `tolerance` (default `0.02`). Reconcile only comparable settled scope/time/basis, retaining delays, credits and residuals. Actual and amortized representations cannot be added together. Imported values stay `azure_provisional`; an explicitly supplied `azure_reconciled` usage document must pass reconciliation. Reconciliation alone does not close resource attribution or missing-component gaps.

Outputs include the charge ledger, component allocations, workload and technology CSVs, standing ledger, reconciliation and Markdown report. Logical TB uses 10^12 bytes; zero input produces no unit cost. `demand_sensitivity.csv` evaluates 6/24/96 runs/day and the adopted 90/9/1% size mix only when all tier marginal costs and standing prices are complete. Supply `accounting_window_hours` for priced standing-cost extrapolation. This is a fixed-capacity demand scenario, not an autoscaling or Azure runtime forecast. Missing tier measurements keep total projections unavailable.

## Run the same coordinator through Airflow

```sh
make local-airflow-up
docker-compose -f infra/local/compose.yaml -f infra/local/compose.benchmark.yaml exec -T airflow airflow dags unpause dpe_benchmarks
docker-compose -f infra/local/compose.yaml -f infra/local/compose.benchmark.yaml exec -T airflow airflow dags trigger --run-id local-comparison-1 --conf '{"workspace":"airflow-comparison-1","count":1000,"repeats":3}' dpe_benchmarks
```

Use a unique workspace/run ID. DAG `execute` submits to the private HTTP coordinator and waits for completion; `validate` checks the retained report. A repeated identical request reuses its job ID, so an HTTP/Airflow retry does not launch a second simultaneous benchmark. A coordinator restart marks in-flight jobs `interrupted` for inspection; it does not claim transparent process recovery. W5 distributed driver/worker/service fault recovery is still open.

Airflow runs standalone with local metadata, bounded parallelism and persistent home volume. It has no Docker socket, and its data mount is read-only. The coordinator and Spark gateway use loopback host ports 18112/18111 and shared Docker networking; they are local development services, not production authentication boundaries. Generated data/evidence and local standalone credentials stay under ignored data or Docker volumes. `make local-down` stops all profiles while preserving volumes.

The optional manual `comparative-poc` CI job defines the local acceptance/comparison/restart sequence. Editing its workflow does not mean hosted CI has run. Azure infrastructure, identities, remote publication coordination, cost exports, budgets and shutdown are described in `infra/azure/validation-spec.template.json`; no deployment is performed by these local commands.

For host-side utilization sampling during a run, start `python3 benchmarks/sample_local_resources.py --out data/container-samples.jsonl --terminal-report data/comparison-new/run_results.json` before submitting it. This read-only Docker collector preserves timestamped CPU/memory and cumulative network/block IO for all containers, plus its own collection duration and source hash. It stops when that report reaches a terminal state, or fails at its declared 30-minute maximum. Sampling does not replace allocated-capacity or billable-request measurements, and sample maxima are not exact memory peaks. The external collector is optional, and its overhead stays in the system boundary.

To inspect exploratory Trino join concurrency after preparing a comparison, run `.venv/bin/python benchmarks/measure_local_concurrency.py --workspace data/comparison-new --out data/concurrency-new`. It runs randomized 1/2/4-way blocks three times, verifies every result independently, and retains query IDs, completion and throughput. `--allow-failed-experiment` explicitly permits using a committed immutable preparation from an otherwise failed experiment; it preserves that source status and does not turn the failed comparison into a pass. This diagnostic cannot satisfy the required Medium/cross-engine concurrency matrix.

`benchmarks/assemble_local_poc_evidence.py` combines passed comparison/acceptance/restart artifacts, optional retained failed comparisons, capacity preflight and exploratory concurrency into a traceable local scorecard. Pass `--out`, `--verification`, one or more `--comparison`, `--calibration`, and optionally `--restart`, `--failed-comparison` and `--concurrency`. It preserves failed comparisons separately and records Small preflight resource exclusions without marking gates passed.

Run `python3 benchmarks/audit_completed_events.py --workspace data/comparison-new` to audit persisted Trino events referenced by the runner, full attempts and dbt results. It tolerates a short collector delivery delay, preserves missing IDs and fails if referenced events are absent. This audit does not claim coverage of unreferenced metadata/consumer queries.
