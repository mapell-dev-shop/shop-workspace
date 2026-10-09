---
name: new-consumer
description: Steps to make orders-service consume a new Kafka event type with an idempotent handler, the way PaymentCaptured was added. Use when orders-service must react to an event it doesn't consume yet, e.g. "handle StockReserved in orders" or "add a consumer for X".
---

# Add an idempotent consumer

Work inside the orders-service repository. All paths and commands below are relative to it; if the session started at the workspace root, `cd orders-service` first.

The reference is commit `fbb366e` ("feat: confirm orders on PaymentCaptured"), with `0bdc761` for the model change. Read them with `git show` before starting.

## 1. Check the contract

The event type must already be in `EVENTS` in `../contracts/src/shop_contracts/events.py`, which maps it to a topic and a schema in `schemas/events/`. Read the schema to see which fields `envelope["data"]` carries.

If the event or a field you need is missing, stop and say so. A schema change is its own change in `../contracts` (see VERSIONING.md), never part of this task. Never hard-code a topic name or payload shape here.

## 2. Write the tests first

In `tests/test_handlers.py`, add an envelope builder next to the others, built with `event()` from `tests/conftest.py` so it passes the contract:

    def payment_captured(order_id: str = "o-1") -> dict[str, object]:
        return event("PaymentCaptured", {"order_id": order_id, ...})

Then add at least these three tests, reusing `pending_order(session)`:

* the event moves a `PENDING` order to its new state,
* a redelivered envelope (same `event_id`, handled twice) is applied once, and `processed_events` has one row,
* the event leaves an order that is no longer `PENDING` (e.g. already `REJECTED`) unchanged.

Run them and watch them fail.

## 3. Write the handler

In `src/orders/handlers.py`, add `handle_<event_in_snake_case>(session, envelope) -> None` with this shape:

    def handle_payment_captured(session: Session, envelope: dict[str, Any]) -> None:
        if session.get(ProcessedEvent, envelope["event_id"]) is not None:
            return
        order = session.get(Order, envelope["data"]["order_id"])
        if order is not None and order.status == OrderStatus.PENDING:
            order.status = OrderStatus.CONFIRMED
        session.add(ProcessedEvent(event_id=envelope["event_id"]))
        session.commit()

The rules:

* Return early if the `event_id` is already in `processed_events`.
* Change state only for a `PENDING` order. A late or duplicate event never touches `CONFIRMED` or `REJECTED`.
* For an unknown order, record the event id and change nothing.
* Add the `ProcessedEvent` and the state change in the same single commit.
* Don't catch exceptions or retry. A handler that raises stops the worker, and the event is redelivered on restart.

If the event rejects the order, call `reject_order(session, envelope, reason)` like `handle_stock_rejected` does, instead of repeating the shape.

## 4. Change the model, if needed

A new status goes in `OrderStatus` in `src/orders/models.py`. A new column needs an Alembic revision too. Follow `.claude/rules/migrations.md`, as `0bdc761` did with `confirmed_at`.

## 5. Register it in the worker

In `src/orders/worker.py`, import the handler and add `"<EventType>": with_session(handle_...)` to the dict passed to `run_consumer`. The key is the event type, and `shop_contracts` resolves its topic.

## 6. Document it

Add a line under the consumed events in `README.md`, e.g. "Consumes `PaymentCaptured` and marks a `PENDING` order `CONFIRMED`." Tell the user that the "Consumes" column of the workspace `../CLAUDE.md` needs the event too, since it lives outside this repo.

## 7. Verify

    uv run pytest
    uv run ruff check . && uv run ruff format --check .
    uv run mypy src

If Kafka and the shop are running (`../up.sh`, `../run-all.sh`), also run `uv run pytest e2e` from the workspace root.
