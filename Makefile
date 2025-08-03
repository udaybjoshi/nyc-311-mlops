# Default MySQL port (falls back to 3307 if 3306 is busy)
MYSQL_PORT := $(shell \
	if ! lsof -i:3306 >/dev/null 2>&1; then echo 3306; else echo 3307; fi \
)

.PHONY: demo reset wait-for-services wait-for-mysql wait-for-api wait-for-prefect logs banner \
        bronze-backfill forecast test docker-logs-api clean

# 🧪 Demo environment setup
demo: reset wait-for-services banner

reset:
	@echo "🚀 Spinning up NYC 311 MLOps stack..."
	docker-compose down -v
	docker-compose up -d --build

wait-for-services: wait-for-mysql wait-for-api wait-for-prefect

wait-for-mysql:
	@echo "Waiting for MySQL (up to 120s)..."
	@for i in `seq 1 60`; do \
		if docker-compose exec -T mysql mysqladmin ping -uroot -proot --silent >/dev/null 2>&1; then \
			echo "✅ MySQL is healthy."; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "❌ MySQL did not become ready in time"; \
	exit 1

wait-for-api:
	@echo "Waiting for API (up to 120s)..."
	@for i in `seq 1 60`; do \
		if curl -sSf http://localhost:8000/docs >/dev/null 2>&1; then \
			echo "✅ API is healthy."; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "❌ API did not become ready in time"; \
	exit 1

wait-for-prefect:
	@echo "Waiting for Prefect Orion (up to 180s)..."
	@for i in `seq 1 90`; do \
		if curl -sSf http://localhost:4200 >/dev/null 2>&1; then \
			echo "✅ Prefect Orion is healthy."; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "❌ Prefect Orion did not become ready in time"; \
	exit 1

logs:
	@echo "🔍 Fetching logs for MySQL, API, Prefect (for troubleshooting)..."
	docker-compose logs mysql api prefect

docker-logs-api:
	@echo "📜 Streaming logs from the API container..."
	docker-compose logs -f api

clean:
	@echo "🧹 Cleaning up logs, output, and containers..."
	rm -rf logs/*
	rm -rf output/*
	docker-compose down -v

banner:
	@echo "=================================================="
	@echo "🎉 NYC 311 MLOps stack is up and running!"
	@echo ""
	@echo "🔗 MySQL Connection:"
	@echo "    Host: localhost"
	@echo "    Port: $(MYSQL_PORT)"
	@echo "    User: root"
	@echo "    Password: root"
	@echo ""
	@echo "🌐 Access your services:"
	@echo "    API Docs:     http://localhost:8000/docs"
	@echo "    Prefect UI:   http://localhost:4200"
	@echo "    MLflow UI:    http://localhost:5000"
	@echo "    Dashboard (UI): http://localhost:8501"
	@echo "=================================================="

# 🥇 Custom project targets

bronze-backfill:
	@echo "📥 Running initial 2-year Bronze layer backfill..."
	python scripts/initial_bronze_load.py

forecast:
	@echo "📈 Running forecast from Gold layer..."
	python orchestration/forecast_from_gold_flow.py

test:
	@echo "🧪 Running unit and integration tests..."
	pytest tests/ -v










