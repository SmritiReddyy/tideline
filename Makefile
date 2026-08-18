# Tideline — common tasks.
#
#   make simulate    the whole CDC path, no Docker needed
#   make up          the real stack: Postgres + Redpanda + Debezium
#   make test        everything CI runs
#
# Spark needs Java 17. `make setup` checks for it and tells you how to install.

SHELL := /bin/bash
.DEFAULT_GOAL := help

VENV     := .venv
PY       := $(VENV)/bin/python
PIP      := $(VENV)/bin/pip
TIDELINE := $(VENV)/bin/tideline
PYTEST   := $(VENV)/bin/pytest
RUFF     := $(VENV)/bin/ruff
COMPOSE  := docker compose -f docker/docker-compose.yml

# Spark 3.5 runs on Java 8/11/17. Anything newer fails with module access errors.
JAVA_HOME ?= $(shell /usr/libexec/java_home -v 17 2>/dev/null || echo /opt/homebrew/opt/openjdk@17)
export JAVA_HOME

BATCHES ?= 20
TICKS   ?= 40

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# --- setup ----------------------------------------------------------------

.PHONY: check-java
check-java:
	@if [ ! -x "$(JAVA_HOME)/bin/java" ]; then \
		echo "Java 17 not found at $(JAVA_HOME)."; \
		echo "Install it:  brew install openjdk@17"; \
		echo "Then re-run, or pass JAVA_HOME=/path/to/jdk17"; \
		exit 1; \
	fi
	@echo "Java: $$($(JAVA_HOME)/bin/java -version 2>&1 | head -1)"

$(VENV)/bin/activate: pyproject.toml
	python3.10 -m venv $(VENV) 2>/dev/null || python3.11 -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip
	$(PIP) install --quiet -e ".[dev]"
	@touch $(VENV)/bin/activate

.PHONY: setup
setup: check-java $(VENV)/bin/activate ## Create the venv and install dependencies
	@echo "Ready. Try: make simulate"

# --- offline path (no Docker) ---------------------------------------------

.PHONY: simulate
simulate: ## Run the CDC path with synthetic Debezium events
	$(TIDELINE) simulate --batches $(BATCHES) --ticks-per-batch $(TICKS) \
		--evolve-at-batch $$(( $(BATCHES) / 2 ))

.PHONY: benchmark
benchmark: ## Fragment, measure, compact, measure again
	$(PY) scripts/benchmark.py --batches $(BATCHES) --ticks-per-batch $(TICKS)

.PHONY: demo
demo: setup simulate stats maintain stats ## Nothing -> populated, compacted lakehouse
	@echo ""
	@echo "Lakehouse at data/lakehouse. Query it:  make query"

# --- the real stack -------------------------------------------------------

.PHONY: up
up: ## Start Postgres + Redpanda + Debezium Connect
	$(COMPOSE) up -d
	@echo "Waiting for Kafka Connect..."
	@$(TIDELINE) register-connector || \
		(echo "Connector registration failed; check: make logs"; exit 1)
	@echo ""
	@echo "Redpanda console: http://localhost:8085"

.PHONY: up-query
up-query: ## Also start Trino and attach the Delta tables (another JVM; opt-in)
	$(COMPOSE) --profile query up -d trino
	@echo "Waiting for Trino..."
	@until docker ps --filter name=tideline-trino --format '{{.Status}}' | grep -q healthy; do sleep 5; done
	@docker exec tideline-trino trino --execute \
		"create schema if not exists delta.tideline with (location='file:///data/lakehouse')" >/dev/null
	@# Spark creates the tables; Trino only attaches to what already exists.
	@for t in orders order_items inventory customers; do \
		docker exec tideline-trino trino --execute \
			"call delta.system.register_table(schema_name=>'tideline', table_name=>'$$t', \
			 table_location=>'file:///data/lakehouse/$$t')" >/dev/null 2>&1 || true; \
	done
	@echo "Trino: http://localhost:8086 — tables under delta.tideline"
	@echo "  docker exec -it tideline-trino trino"

.PHONY: down
down: ## Stop the stack and delete its volumes
	$(COMPOSE) --profile query down -v

.PHONY: logs
logs: ## Follow the Debezium Connect logs
	$(COMPOSE) logs -f connect

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps

.PHONY: seed
seed: ## Populate the source database
	$(TIDELINE) seed

.PHONY: workload
workload: ## Drive OLTP traffic for 60s
	$(TIDELINE) workload --duration 60 --rate 20

.PHONY: stream
stream: ## Run the streaming job continuously (Ctrl-C to stop)
	$(TIDELINE) stream

.PHONY: stream-once
stream-once: ## Drain what is in the topics, then stop
	$(TIDELINE) stream --once

.PHONY: evolve
evolve: ## Add a column to shop.customers mid-stream
	$(TIDELINE) evolve

.PHONY: connector-status
connector-status: ## Show Debezium connector and task state
	$(TIDELINE) connector-status

# --- lakehouse ------------------------------------------------------------

.PHONY: stats
stats: ## File layout of each Delta table
	$(TIDELINE) stats

.PHONY: maintain
maintain: ## Compact, Z-order and vacuum
	$(TIDELINE) maintain --retain-hours 0

.PHONY: query
query: ## Run the analytical query set
	$(TIDELINE) query --file query/queries/analytics.sql

# --- checks ---------------------------------------------------------------

.PHONY: lint
lint: ## ruff check + format check
	$(RUFF) check src tests scripts
	$(RUFF) format --check src tests scripts

.PHONY: fmt
fmt: ## Auto-format
	$(RUFF) check --fix src tests scripts
	$(RUFF) format src tests scripts

.PHONY: unit
unit: check-java ## Run the test suite
	$(PYTEST) -q

.PHONY: test
test: lint unit ## Everything CI runs

# --- housekeeping ---------------------------------------------------------

.PHONY: clean
clean: ## Delete the lakehouse, checkpoints and Spark artefacts
	rm -rf data spark-warehouse metastore_db derby.log
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

.PHONY: clean-all
clean-all: clean down ## Also remove the venv and containers
	rm -rf $(VENV)
