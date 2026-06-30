# Use the project venv if present, else fall back to system python3.
PYTHON := $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)
COMPOSE := docker compose

.PHONY: help install up down clean wait produce consume dashboard demo test

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## create a venv and install dev dependencies
	python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt

up: ## start Kafka (KRaft) + Kafka UI in the background
	$(COMPOSE) up -d

down: ## stop the containers
	$(COMPOSE) down

clean: ## stop containers and delete their data/volumes
	$(COMPOSE) down -v

wait: ## block until the Kafka broker is reachable
	$(PYTHON) -m enrich.wait_for_kafka

produce: ## publish the sample events to events.in
	$(PYTHON) -m enrich.producer

consume: ## run the enrichment consumer (Ctrl-C to stop)
	$(PYTHON) -m enrich.consumer

dashboard: ## run the live dashboard on http://localhost:8101
	$(PYTHON) -m uvicorn enrich.dashboard:app --host 0.0.0.0 --port 8101

demo: up wait produce ## up + produce + consume (drains, then exits)
	$(PYTHON) -m enrich.consumer --max-idle 8
	@echo ""
	@echo "Enriched -> events.out, failures -> events.dlq."
	@echo "Kafka UI:        http://localhost:8080"
	@echo "Live dashboard:  make dashboard   (http://localhost:8101)"

test: ## run the hermetic test suite (no Kafka needed)
	$(PYTHON) -m pytest
