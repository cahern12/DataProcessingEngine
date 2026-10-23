# Data Processing Engine

A local proof of concept for turning raw enterprise data into trusted analytics through stable output contracts. We use our own synthetic IT service-management data and business rules to compare **dbt + Trino** with **Spark** for correctness, recovery, performance and cost.

```text
Synthetic files / mock API → Bronze (raw) → Silver (validated) → Gold (analytics)
```

The platform publishes two products:

| Product | Contents |
|---|---|
| `incident_resolution` | Incident state, response/resolution times and SLA outcomes |
| `service_daily_metrics` | Daily incident counts, backlog and SLA measures by service and priority |

## Try the Python demo

Requires **Python 3.9+ on macOS/Linux**. Run commands from this repository's root; the demo uses Python's standard library.

```sh
python3 -m dpe check-fixtures
python3 -m dpe demo
python3 -m dpe query --db data/demo/platform.sqlite \
  --sql 'SELECT state, count(*) AS incidents FROM gold_incident_resolution GROUP BY state'
```

The demo processes an initial batch and a delta, publishing 1,100 incidents. Its database and report are `data/demo/platform.sqlite` and `data/demo/evidence.json`. After changing code or rules, use a fresh workspace: `python3 -m dpe demo --workspace data/demo-next`.

## Run the local lakehouse

Requires a **running Docker-compatible runtime, Compose and Make**. This path runs dbt/Trino over Iceberg, with RustFS object storage and a Nessie/PostgreSQL catalog. See the [setup guide](docs/guides/local_lakehouse.md) for Colima and Docker Desktop instructions.

```sh
make local-build
make local-up
make local-verify LOCAL_WORKSPACE=data/lake-quickstart
```

Verification checks the rules, initial/delta processing, safe publication, failure recovery and replay. Read a published product:

```sh
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-query --workspace data/lake-quickstart/platform \
  --dataset incident_resolution --limit 5
```

Use a fresh workspace for each verification run. Generated data and evidence stay under Git-ignored `data/`. Stop services with `make local-down`; storage volumes are retained.

These commands use your active Docker context and the default Smoke profile. For the dedicated **20-GiB benchmark runtime**, follow the [capacity guide](docs/guides/local_capacity.md). If Compose is a Docker plugin, pass `DOCKER_COMPOSE='docker compose'` to Make and use `docker compose` in direct commands.

## Process your own batches

With the lakehouse running:

```sh
docker-compose -f infra/local/compose.yaml run --rm runner \
  generate --out data/my-source --count 1000
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-run data/my-source/initial --workspace data/my-platform
docker-compose -f infra/local/compose.yaml run --rm runner \
  lake-run data/my-source/delta --workspace data/my-platform
```

`lake-run` defaults to targeted incremental processing. Full rebuilds are available with `--strategy full` in a separate workspace. The [lakehouse guide](docs/guides/local_lakehouse.md#targeted-processing) covers candidate selection, audits and recovery; the [reference guide](docs/guides/reference_harness.md) covers custom data, faults and mock API ingestion.

## Benchmarks and current status

The latest recorded local results are from **2026-10-23**:

- Three paired repetitions at 100,000 incidents pass. Full-processing medians: **117.8 seconds for Trino**, **136.0 seconds for Spark**.
- Six delivery scenarios pass on both full engines and targeted Trino. Independent disposition audits and post-restart publication checks pass.
- The original POC remains incomplete: **7 of 89 required cells pass**. The Small concurrency target is missed; Azure/ADLS, Medium/Large, distributed scale and actual billing remain unassessed.

Start with the [checked-in summary](benchmarks/results/poc-capacity-2026-10-23.json) or [execution record](docs/plan/12-local_capacity_and_representative_benchmark_execution.md). Generated local reports include [benchmark results](data/capacity-evidence-final/capacity_results.md), [workload costs](data/capacity-evidence-final/modeled_cost_performance.csv) and [technology costs](data/capacity-evidence-final/technology_costs.csv). Cloud costs use public-retail planning assumptions; actual Azure charges are unmeasured.

## Documentation

| Need | Guide |
|---|---|
| Generate data, run the reference implementation or test recovery | [Python reference and synthetic data](docs/guides/reference_harness.md) |
| Set up the processing stack and run incremental batches | [Local lakehouse](docs/guides/local_lakehouse.md) |
| Run controlled Trino/Spark benchmarks and price scenarios | [Local capacity](docs/guides/local_capacity.md) |
| Understand workload accounting, cost imports and Airflow | [Benchmarks and economics](docs/guides/poc_benchmarks_and_economics.md) |
| Understand the rules and consumer contracts | [Business rules](rules/business_rules.v1.json) · [Output contract](contracts/output.v1.json) |
| Review the project scope and build history | [Original requirements](docs/plan/01-enterprise_data_processing_poc_validation_plan.md) · [Chronological plans](docs/plan/) |

For reference tests, run `make test`. Optional engine dependencies and setup are documented in the guides.
