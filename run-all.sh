#!/usr/bin/env bash
# Migrates the databases, resets the demo products, then starts every API and worker in the
# background. Logs and pids go to .run/. Stop everything with ./stop-all.sh.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p .run

for service in orders inventory payments; do
  uv sync -q --directory "$service-service"
  uv run -q --directory "$service-service" alembic upgrade head
done
uv run -q --directory inventory-service python -m inventory.seed

start() {  # start <name> <service directory> <python -m arguments...>
  local name=$1 directory=$2
  shift 2
  cd "$directory"
  nohup .venv/bin/python -m "$@" > "../.run/$name.log" 2>&1 < /dev/null &
  echo $! > "../.run/$name.pid"
  cd ..
}
start orders-api orders-service uvicorn orders.api:app --port 8001
start inventory-api inventory-service uvicorn inventory.api:app --port 8002
start payments-api payments-service uvicorn payments.api:app --port 8003
start orders-worker orders-service orders.worker
start inventory-worker inventory-service inventory.worker
start payments-worker payments-service payments.worker

for port in 8001 8002 8003; do
  for _ in $(seq 60); do
    curl -sf "http://localhost:$port/docs" > /dev/null && break
    sleep 0.5
  done
  curl -sf "http://localhost:$port/docs" > /dev/null || { echo "API on :$port did not start; see .run/" >&2; exit 1; }
done
echo "Running: orders :8001, inventory :8002, payments :8003, plus three workers. Logs in .run/"
