# Shop workspace

A small shop split into services. Each service is its own git repository, cloned into this directory.

## Services

| Directory | Owns | HTTP | Consumes | Publishes |
| --- | --- | --- | --- | --- |
| orders-service | orders and their status | :8001 | StockRejected, PaymentFailed, PaymentCaptured, PaymentRefunded | OrderPlaced, OrderCancelled |
| inventory-service | products, stock, reservations | :8002 | OrderPlaced, PaymentFailed, OrderCancelled | StockReserved, StockRejected |
| payments-service | payments, through a fake gateway | :8003 | StockReserved, OrderCancelled | PaymentCaptured, PaymentFailed, PaymentRefunded |
| contracts | event schemas and Kafka helpers (package `shop_contracts`) | | | |

orders-service also calls inventory-service over HTTP (`GET /products/{sku}`) for prices.

## Order flow

`POST /orders` publishes OrderPlaced. Inventory reserves the stock and publishes StockReserved. Payments captures the money and publishes PaymentCaptured, and orders-service marks the order CONFIRMED. If a step fails, inventory publishes StockRejected or payments publishes PaymentFailed, and orders-service marks the order REJECTED. When the payment fails, inventory also gives the reserved stock back.

Cancelling a PENDING or CONFIRMED order publishes OrderCancelled. Inventory gives the stock back and payments refunds a captured payment, publishing PaymentRefunded. orders-service records `refunded_at`. Each service remembers cancelled orders, so a late OrderPlaced or StockReserved does nothing.

## Rules

* A service never reads another service's database. It uses events or HTTP.
* Event schemas live only in contracts/. Change them there first, following contracts/VERSIONING.md.
* Every consumer is idempotent: it records handled event ids in its `processed_events` table.

## Commands

* `./up.sh` starts Kafka in Docker and creates the topics. `docker compose down` stops it.
* `./run-all.sh` starts every API and worker, and refuses if the shop is already running. `./stop-all.sh` stops them. Logs are in `.run/`.
* `uv run pytest e2e` runs the whole flow. It needs the two commands above. `uv run pytest` alone tests the scripts.
* `./checkout-lesson.sh NN` puts every repository at the start of lesson NN, and `--end` at its end. It deletes the services' SQLite databases; `./run-all.sh` creates them again.
* Inside a service: `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy src`.
