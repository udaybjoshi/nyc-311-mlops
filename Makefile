SHELL := /bin/bash
DOCKER_COMPOSE := docker-compose

.PHONY: demo reset wait-for-services

demo: reset wait-for-services backfill

reset:
	@echo "🚀 Spinning up NYC 311 MLOps stack..."
	$(DOCKER_COMPOSE) down -v
	$(DOCKER_COMPOSE) up -d --build

wait-for-services:
	@echo "⏳ Waiting for all services to be healthy..."
	@$(MAKE) wait-for-mysql
	@$(MAKE) wait-for-prefect
	@$(MAKE) wait-for-api

wait-for-mysql:
	@echo "Waiting for mysql..."
	@timeout 60 bash -c 'until $(DOCKER_COMPOSE) exec -T mysql bash -c "mysqladmin ping -h 127.0.0.1 -uroot -proot --silent"; do sleep 2; done' \
	|| (echo "❌ MySQL did not become ready in time" && exit 1)
	@echo "✅ MySQL is healthy."

wait-for-prefect:
	@echo "Waiting for Prefect Orion..."
	@timeout 60 bash -c 'until $(DOCKER_COMPOSE) exec -T prefect bash -c "curl -sSf http://localhost:4200 > /dev/null"; do sleep 2; done' \
	|| (echo "❌ Prefect did not become ready in time" && exit 1)
	@echo "✅ Prefect is healthy."

wait-for-api:
	@echo "Waiting for API..."
	@timeout 60 bash -c 'until $(DOCKER_COMPOSE) exec -T api bash -c "curl -sSf http://localhost:8000/docs > /dev/null"; do sleep 2; done' \
	|| (echo "❌ API did not become ready in time" && exit 1)
	@echo "✅ API is healthy."

backfill:
	@echo "📥 Running 60-day backfill..."
	$(DOCKER_COMPOSE) exec api python ingestion/fetch_api_data.py --backfill 60


