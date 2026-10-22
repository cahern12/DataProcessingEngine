.PHONY: test fixtures demo local-up local-down local-build local-verify

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
	$(DOCKER_COMPOSE) -f infra/local/compose.yaml down

local-build:
	$(DOCKER_COMPOSE) -f infra/local/compose.yaml build runner

local-verify:
	$(DOCKER_COMPOSE) -f infra/local/compose.yaml run --rm runner lake-verify --workspace $(LOCAL_WORKSPACE)
