# Run the local capacity profile

The [execution record](../plan/12-local_capacity_and_representative_benchmark_execution.md) reports observed successes and failures. [Plan 11](../plan/11-local_capacity_and_representative_benchmark_plan.md) defines scope. Use fresh experiment paths and preserve failed workspaces.

## Runtime

The helper expects Docker CLI, `docker-compose` and the `colima-dpe-capacity` Docker context. It selects that context explicitly. Stop the original profile before starting the isolated capacity VM; keep its volumes:

```sh
colima stop --profile dpe
colima start --profile dpe-capacity --cpu 4 --memory 20 --disk 80 --runtime docker --vm-type vz
make local-capacity-build POC_ACCEPTANCE=benchmarks/specs/acceptance.poc.v9.json
make local-capacity-up POC_ACCEPTANCE=benchmarks/specs/acceptance.poc.v9.json
```

The tested Mac has 32 GiB RAM. Check current memory pressure and disk capacity before starting a VM. The helper starts the loopback read-only resource observer automatically and waits for the complete stack, including Airflow scheduler/database readiness. It does not provision Colima or Azure. Baseline `local-poc-*` commands preserve their historical defaults; use the explicit capacity selection for this environment.

The v8/v9 profiles use typed Parquet landing; their runner images install the pinned optional `requirements-parquet.lock`. Install that lock into the host virtual environment to run Parquet unit tests locally. Both profiles use a 20-GiB VM, a 6-GiB Trino container with a 2-GiB JVM heap, and a 2.5-GiB producer ceiling. NMT summary instrumentation has overhead. Earlier 16-GiB experiments are retained under their original versions.

The primary repeated comparison passed under v8. Its representative cutoff case exceeded the 768-MB per-node query allowance. The v9 profile raises that allowance to 1 GB with unchanged heaps and decision targets. An initial v9 campaign passed Gold checks but failed the disposition audit because duplicate references lacked `BR-002`. Both SQL projections are corrected, and all 54 fixtures pass on both engines. The fresh `capacity-ruleids-*` full and targeted Small campaigns, exact disposition audits and service-restart pin checks pass. Preserve the earlier failed workspaces. See the [results](../../data/capacity-evidence-final/capacity_results.md) and [compact summary](../../benchmarks/results/poc-capacity-2026-10-23.json).

## Verification and measurements

```sh
make local-capacity-verify POC_ACCEPTANCE=benchmarks/specs/acceptance.poc.v9.json POC_WORKSPACE=data/my-capacity-verify
```

The recorded primary comparison used `run_capacity_comparisons.py --acceptance benchmarks/specs/acceptance.poc.v8.json --prefix native6g` with its preserved source/image revision. Reproducing that measurement requires the matching image and v8 service overlay. A current image starts a distinct implementation experiment; select a new prefix and matching calibration specification. Use v9 for the corrected representative campaign. The comparison script uses fixed experiment names derived from its prefix. It preserves successful matching phases on rerun and refuses failed/mismatched phases. For a new campaign, make a new acceptance JSON naming its own `memory_admission.reference_workspace` and select a new prefix. The custom 10,000-incident calibration precedes the 100,000-incident Small attempt. Both comparisons use real Airflow, three paired repetitions and immutable shared read inputs. Do not build images or submit other experiments during measurements.

Actual limits, memory events, producer/container samples, stages, storage inventories and collector gaps are under `observations/`. Technical success, exact correctness, runtime assessment, required-cell coverage and cost completeness are separate. Caches remain uncontrolled. Watchdogs do not replace business runtime targets.

Additional delivery cases use a separate source distribution and workspace:

```sh
DOCKER_CONTEXT=colima-dpe-capacity docker-compose -f infra/local/compose.yaml -f infra/local/compose.benchmark.yaml -f infra/local/compose.capacity.yaml -f infra/local/compose.capacity.v9.yaml run --rm --entrypoint python runner -m dpe.representative --workspace data/my-delivery-cases --count 100000 --acceptance benchmarks/specs/acceptance.poc.v9.json --strategies trino spark
```

Targeted Trino is an additional strategy (`--strategies targeted`) with its own workspace, candidate population and retained-copy overhead. Single delivery cases do not establish three required controlled repetitions. Spark remains a full rebuild implementation.

After measurements, independent audits can run from the host's pinned `.venv`, separately from workload timings:

```sh
.venv/bin/python benchmarks/audit_dispositions.py --workspace data/capacity-small-native6g-100000
.venv/bin/python benchmarks/verify_capacity_publications.py --workspace data/capacity-small-native6g-100000 --out data/my-pin-check.json
```

Use `python3 benchmarks/restart_capacity_services.py --acceptance benchmarks/specs/acceptance.poc.v9.json --out data/my-service-restart.json` after experiments finish. This restarts PostgreSQL/object storage/catalog/Trino, waits for readiness, and records before/after startup times and health. Supply `--restart-evidence data/my-service-restart.json` to the pin auditor before claiming a post-restart result. Keep the selected Compose overlay when restarting. Audits read committed ancestry, including old pins in failed experiments; they never relabel a failed comparison as passed.

## Price models and reports

```sh
python3 benchmarks/fetch_public_prices.py --out benchmarks/pricing/my-new-retail-snapshot
python3 -m dpe.capacity_reports --workspace data/capacity-small-native6g-100000
python3 -m dpe.cloud_model --workspace data/capacity-small-native6g-100000 --snapshot benchmarks/pricing/azure-eastus-2026-10-23-capacity20 --output data/my-capacity-prices
```

The captured snapshot supplies public USD rates, not Azure usage or bills. Keep the valuation interval consistent with its price date. Per-workload and technology CSVs distinguish assumed requests/retention/allocation from measured local elapsed time and object bytes. VM and AKS inventories are separate; shared charges are allocated once. Retained/failed objects stay charged in their declared scope. Original mixed-size demand forecasts remain unavailable without Medium/Large observations.

The final assembler accepts explicit primary comparison paths and keeps configurations/variants separate:

```sh
python3 benchmarks/assemble_capacity_evidence.py --small data/capacity-small-native6g-100000 --custom data/capacity-custom-native6g-10000 --representative data/capacity-ruleids-deliveries-full --targeted data/capacity-ruleids-deliveries-targeted --concurrency data/capacity-small-native6g-concurrency --out data/capacity-evidence-final
```

`small_demand_sensitivity.csv` exposes every workload/scenario at 6/24/96 runs/day. Under the base assumptions with Spark retained, standing capacity is $185.55/month for a VM or $557.15/month for AKS. AKS Small W4 at six runs/day costs about $3.10/run and misses the $2 target; at 24/day it is about $0.78/run. These are alternative Small-only planning scenarios, not additive workloads, the original mixed-size forecast or Azure bills. The Small input is below the 10-GB minimum for the per-TB acceptance gate.

Full raw evidence remains ignored under `data/`; compact summaries and execution documents belong in the repository. No command here deploys Azure resources or automatically commits changes.
