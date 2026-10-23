.PHONY: test fixtures demo local-up local-down local-build local-verify local-incremental-verify local-incremental-restart-check

DOCKER_COMPOSE ?= docker-compose
LOCAL_WORKSPACE ?= data/lake-check

test:
	python3 -m unittest discover -s tests -v

fixtures:
	python3 -m dpe check-fixtures

demo:
	python3 -m dpe demo

local-up:
	$(DOCKER_COMPOSE) -f infra/local/compose.yaml up -d --wait postgres objectstore bucket-init catalog trino

local-down:
	$(DOCKER_COMPOSE) -f infra/local/compose.yaml -f infra/local/compose.benchmark.yaml --profile benchmark --profile orchestration down

local-build:
	$(DOCKER_COMPOSE) -f infra/local/compose.yaml build runner

local-verify:
	$(DOCKER_COMPOSE) -f infra/local/compose.yaml run --rm runner lake-verify --workspace $(LOCAL_WORKSPACE)

# Use a fresh workspace; freezes criteria before the real-stack experiment.
local-incremental-verify:
	$(DOCKER_COMPOSE) -f infra/local/compose.yaml run --rm --entrypoint python runner benchmarks/collect_query_io.py --workspace $(LOCAL_WORKSPACE)

local-incremental-restart-check:
	$(DOCKER_COMPOSE) -f infra/local/compose.yaml run --rm --entrypoint python runner benchmarks/verify_checkpoint_reads.py --workspace $(LOCAL_WORKSPACE)

POC_WORKSPACE ?= data/poc-check
POC_COUNT ?= 1000
POC_REPEATS ?= 3
POC_COMPOSE = $(DOCKER_COMPOSE) -f infra/local/compose.yaml -f infra/local/compose.benchmark.yaml

.PHONY: local-poc-build local-poc-up local-airflow-up local-poc-verify local-poc-benchmark local-poc-restart-check
local-poc-build:
	$(POC_COMPOSE) build runner spark spark-gateway benchmark-coordinator

local-poc-up:
	$(POC_COMPOSE) up -d --wait postgres objectstore bucket-init catalog telemetry trino spark-gateway benchmark-coordinator

local-airflow-up: local-poc-up
	$(POC_COMPOSE) up -d airflow

local-poc-verify:
	$(POC_COMPOSE) run --rm runner poc-verify --workspace $(POC_WORKSPACE)

local-poc-benchmark:
	$(POC_COMPOSE) run --rm runner poc-benchmark --workspace $(POC_WORKSPACE) --count $(POC_COUNT) --repeats $(POC_REPEATS)

local-poc-restart-check:
	$(POC_COMPOSE) run --rm --entrypoint python runner benchmarks/verify_poc_restart.py --workspace $(POC_WORKSPACE)
