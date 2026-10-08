# Shop workspace

A small shop split into services. Each service is its own git repository, cloned into this directory.

## Services

| Directory | Owns | HTTP | Consumes | Publishes |
| --- | --- | --- | --- | --- |
| orders-service | orders and their status | :8001 | StockRejected, PaymentFailed | OrderPlaced |
| inventory-service | products, stock, reservations | :8002 | OrderPlaced | StockReserved, StockRejected |
| payments-service | payments, through a fake gateway | :8003 | StockReserved | PaymentCaptured, PaymentFailed |
| contracts | event schemas and Kafka helpers (package `shop_contracts`) | | | |

orders-service also calls inventory-service over HTTP (`GET /products/{sku}`) for prices.

## Order flow

`POST /orders` publishes OrderPlaced. Inventory reserves the stock and publishes StockReserved. Payments captures the money and publishes PaymentCaptured. If a step fails, inventory publishes StockRejected or payments publishes PaymentFailed, and orders-service marks the order REJECTED. Nothing consumes PaymentCaptured yet, so a successful order stays PENDING.

## Rules

* A service never reads another service's database. It uses events or HTTP.
* Event schemas live only in contracts/. Change them there first, following contracts/VERSIONING.md.
* Every consumer is idempotent: it records handled event ids in its `processed_events` table.

## Commands

* `./up.sh` starts Kafka in Docker and creates the topics. `docker compose down` stops it.
* `./run-all.sh` starts every API and worker. `./stop-all.sh` stops them. Logs are in `.run/`.
* `uv run pytest tests/test_e2e.py` runs the whole flow. It needs the two commands above.
* `./checkout-lesson.sh NN` puts every repository at the start of lesson NN.
* Inside a service: `uv run pytest`, `uv run ruff check .`, `uv run mypy src`.
