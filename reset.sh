#!/bin/bash
echo "Stopping and removing containers & volumes..."
docker-compose down -v

echo "Rebuilding stack..."
docker-compose up -d --build

echo "Checking service health..."
sleep 10

for svc in mysql api dashboard mlflow prefect; do
  if ! docker ps --format '{{.Names}}' | grep -q nyc311-$svc; then
    echo "⚠️  Service $svc failed. Logs:"
    docker logs nyc311-$svc || true
  else
    echo "✅ $svc is running."
  fi
done

echo "Stack is up. Access at:"
echo "Prefect UI:   http://localhost:4200"
echo "MLflow UI:    http://localhost:5001"
echo "Dashboard:    http://localhost:8501"
echo "API:          http://localhost:8000"


