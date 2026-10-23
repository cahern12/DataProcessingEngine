# Run the local capacity profile

The [execution record](../plan/12-local_capacity_and_representative_benchmark_execution.md) reports observed successes and failures. [Plan 11](../plan/11-local_capacity_and_representative_benchmark_plan.md) defines scope. Use fresh experiment paths and preserve failed workspaces.

## Runtime

The helper expects Docker CLI, `docker-compose` and the `colima-dpe-capacity` Docker context. It selects that context explicitly. Stop the original profile before starting the isolated capacity VM; keep its volumes:

```sh
colima stop --profile dpe
colima start --profile dpe-capacity --cpu 4 --memory 16 --disk 80 --runtime docker --vm-type vz
make local-capacity-build POC_ACCEPTANCE=benchmarks/specs/acceptance.poc.v6.json
make local-capacity-up POC_ACCEPTANCE=benchmarks/specs/acceptance.poc.v6.json
```

The tested Mac has 32 GiB RAM. Check current memory pressure and disk capacity before starting a VM. The helper starts the loopback read-only resource observer automatically and waits for the complete stack, including Airflow scheduler/database readiness. It does not provision Colima or Azure. Baseline `local-poc-*` commands preserve their historical defaults; use the explicit capacity selection for this environment.

The current v6 profile uses typed Parquet landing; its runner image installs the pinned optional `requirements-parquet.lock`. Install that lock into the host virtual environment to run Parquet unit tests locally. The 10,000-incident comparison has passed; the 100,000-incident comparison and representative campaign remain incomplete. Commands below describe how to run them, not completed evidence.

## Verification and measurements

```sh
make local-capacity-verify POC_ACCEPTANCE=benchmarks/specs/acceptance.poc.v6.json POC_WORKSPACE=data/my-capacity-verify
python3 benchmarks/run_capacity_comparisons.py --acceptance benchmarks/specs/acceptance.poc.v6.json --prefix parquet
```

The comparison script uses fixed experiment names derived from its prefix. It preserves successful matching phases on rerun and refuses failed/mismatched phases. For a new campaign, make a new acceptance JSON naming its own `memory_admission.reference_workspace` and select a new prefix. The custom 10,000-incident calibration precedes the 100,000-incident Small attempt. Both comparisons use real Airflow, three paired repetitions and immutable shared read inputs. Do not build images or submit other experiments during measurements.

Actual limits, memory events, producer/container samples, stages, storage inventories and collector gaps are under `observations/`. Technical success, exact correctness, runtime assessment, required-cell coverage and cost completeness are separate. Caches remain uncontrolled. Watchdogs do not replace business runtime targets.

Additional delivery cases use a separate source distribution and workspace:

```sh
DOCKER_CONTEXT=colima-dpe-capacity docker-compose -f infra/local/compose.yaml -f infra/local/compose.benchmark.yaml -f infra/local/compose.capacity.yaml -f infra/local/compose.capacity.v6.yaml run --rm --entrypoint python runner -m dpe.representative --workspace data/my-delivery-cases --count 100000 --acceptance benchmarks/specs/acceptance.poc.v6.json --strategies trino spark
```

Targeted Trino is an additional strategy (`--strategies targeted`) with its own workspace, candidate population and retained-copy overhead. Single delivery cases do not establish three required controlled repetitions. Spark remains a full rebuild implementation.

After measurements, independent audits can run from the host's pinned `.venv`, separately from workload timings:

```sh
.venv/bin/python benchmarks/audit_dispositions.py --workspace data/capacity-small-parquet-100000
.venv/bin/python benchmarks/verify_capacity_publications.py --workspace data/capacity-small-parquet-100000 --out data/my-pin-check.json
```

Restart PostgreSQL/object storage/catalog/Trino and wait for readiness before claiming a post-restart result. Keep the selected Compose overlay when restarting. Audits read committed ancestry, including old pins in failed experiments; they never relabel a failed comparison as passed.

## Price models and reports

```sh
python3 benchmarks/fetch_public_prices.py --out benchmarks/pricing/my-new-retail-snapshot
python3 -m dpe.capacity_reports --workspace data/capacity-small-parquet-100000
python3 -m dpe.cloud_model --workspace data/capacity-small-parquet-100000 --snapshot benchmarks/pricing/azure-eastus-2026-10-22 --output data/my-capacity-prices
```

The captured snapshot supplies public USD rates, not Azure usage or bills. Keep the valuation interval consistent with its price date. Per-workload and technology CSVs distinguish assumed requests/retention/allocation from measured local elapsed time and object bytes. VM and AKS inventories are separate; shared charges are allocated once. Retained/failed objects stay charged in their declared scope. Original mixed-size demand forecasts remain unavailable without Medium/Large observations.

The final assembler accepts explicit primary comparison paths and keeps configurations/variants separate:

```sh
python3 benchmarks/assemble_capacity_evidence.py --small data/capacity-small-parquet-100000 --custom data/capacity-custom-parquet-10000 --out data/capacity-evidence-final
```

Full raw evidence remains ignored under `data/`; compact summaries and execution documents belong in the repository. No command here deploys Azure resources or automatically commits changes.
