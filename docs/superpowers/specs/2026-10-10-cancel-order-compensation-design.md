# Cancel an order: give the stock back and refund the payment

Date: 2026-10-10. Scope: contracts, orders-service, inventory-service, payments-service, workspace e2e.

Follows `orders-service/docs/superpowers/specs/2026-10-09-cancel-pending-order-design.md`, which added `POST /orders/{id}/cancel` in orders-service only and left inventory and payments unaware of it.

## Goal

Customers cancel by mistake or change their mind after paying. When they do, the shop puts the reserved stock back on sale and returns any captured money.

### Agreed with the product owner

* A customer can cancel a `PENDING` or a `CONFIRMED` order. A `REJECTED` order cannot be cancelled: nothing was reserved or charged.
* The order becomes `CANCELLED` at once. The order also records when the refund happened, so the customer can check that the money came back.
* A cancelled order never ends up holding stock or money, whatever order the events arrive in.
* Each service learns about the cancel from an event and decides from its own data (approach "each consumer remembers cancelled orders"). No orchestrator, no commands.

### Assumptions

* The fake gateway's refund always succeeds.
* `CANCELLED` is final. Late or duplicate events never move a `CANCELLED` order.
* There is no shipping step, so a `CONFIRMED` order can always be cancelled.
* Publishing stays after the database commit. No outbox in this iteration.

### Success criteria

* Cancelling a `PENDING` or a `CONFIRMED` order ends, in every event arrival order, with the order `CANCELLED`, no reservations for it, its stock back on sale, and no `CAPTURED` payment for it.
* When money was captured, the payment ends `REFUNDED` and the order's `refunded_at` is set.
* Redelivered events and repeated cancels change no state beyond the first. A repeated cancel only re-publishes events.
* The e2e suite shows the confirmed and the pending case through real Kafka.

## Events (contracts)

Two new event types. Both are new schema files on new topics, so they are additive under `contracts/VERSIONING.md`.

| Event | Topic | `data`, all required | Published by | Consumed by |
| --- | --- | --- | --- | --- |
| `OrderCancelled` | `shop.orders.order-cancelled.v1` | `order_id` (string) | orders-service | inventory-service, payments-service |
| `PaymentRefunded` | `shop.payments.payment-refunded.v1` | `order_id`, `payment_id` (strings), `amount_cents` (integer, minimum 0) | payments-service | orders-service |

* Schema files `order-cancelled.v1.schema.json` and `payment-refunded.v1.schema.json` in `contracts/src/shop_contracts/schemas/events/`, in the style of `payment-captured.v1.schema.json`.
* Two entries in `EVENTS` in `contracts/src/shop_contracts/events.py`.
* Both events are keyed by `order_id`.
* `OrderCancelled` carries no previous status and no lines. Inventory knows what to give back from its reservations, payments knows what to refund from its payment row.
* `up.sh` derives its topics from `EVENTS`, so it needs no edit. It must be run once more so that the new topics exist before the workers start.

## orders-service

### Status changes: `orders/status.py`

`move_pending` becomes:

    move_order(session, order_id, new_status, *, from_statuses=(OrderStatus.PENDING,), reason=None, **fields) -> bool

* It runs `UPDATE orders SET status = :new_status, status_reason = :reason, <fields> WHERE id = :order_id AND status IN (:from_statuses)` with `synchronize_session=False`.
* Everything else is unchanged: it returns `rowcount == 1`, expires the in-session `Order` with that id, and does not commit.
* All callers move to the new name. The handlers keep the default `from_statuses`, so they still move only `PENDING` orders. Only the cancel endpoint passes `(PENDING, CONFIRMED)`.

### `POST /orders/{order_id}/cancel`

| Order state | Response | Publishes |
| --- | --- | --- |
| `PENDING` or `CONFIRMED` | `200`, `OrderOut` with `status: "CANCELLED"`, `status_reason: "cancelled by customer"` | `OrderCancelled`, after the commit |
| `CANCELLED` | `200`, the same body | `OrderCancelled` again |
| `REJECTED` | `409`, `"order is REJECTED and can no longer be cancelled"`; the order is unchanged | nothing |
| unknown id | `404`, `"order not found"` | nothing |

The endpoint calls `move_order(..., CANCELLED, from_statuses=(PENDING, CONFIRMED), reason="cancelled by customer")` and commits. If the move did not win, it reads the order and picks 404, 200 or 409 from its state. The decision still comes from the guarded update, never from a read made before it.

**Why a repeated cancel publishes again.** With no outbox, a crash or Kafka failure after the commit leaves a `CANCELLED` order whose event never went out, and its stock and money would stay held. Cancelling again re-publishes `OrderCancelled` and repairs it. This is safe because inventory and payments are idempotent by state as well as by event id (see below).

If publishing fails, the endpoint answers 500, as `POST /orders` does today, and the order stays `CANCELLED`.

### Races

* Cancel against `PaymentCaptured`: if the cancel wins, `PaymentCaptured` matches no `PENDING` row and changes nothing, and payments refunds on `OrderCancelled`. If `PaymentCaptured` wins, the order is `CONFIRMED` and the cancel then moves it from `CONFIRMED`.
* Cancel against `StockRejected` or `PaymentFailed`: if the reject wins, the cancel answers 409. If the cancel wins, the reject changes nothing, and inventory and payments still clean up on `OrderCancelled`.

### Refund record

* New nullable column `orders.refunded_at`, added by `migrations/versions/0003_add_refunded_at_to_orders.py`.
* `OrderOut` gains `refunded_at: datetime | None`. Adding a response field is compatible for clients.
* New handler `handle_payment_refunded(session, envelope)`:
  1. Return if the event id is in `processed_events`.
  2. `UPDATE orders SET refunded_at = :now WHERE id = :order_id AND refunded_at IS NULL`. A later duplicate keeps the first timestamp. The status is not touched.
  3. Record the event id and commit once.
* `orders/worker.py` subscribes to `PaymentRefunded`.

### README

* Remove the limitation "a cancelled order may still be charged", and the warning not to offer cancelling to customers.
* Describe cancelling `PENDING` and `CONFIRMED` orders and `refunded_at`.
* Add `OrderCancelled` to the publish-after-commit limitation, and say that cancelling again re-publishes it.
* Link this spec instead of the 2026-10-09 one.

## inventory-service

### Model

New table `cancelled_orders (order_id String(36) primary key)`, added by `migrations/versions/0002_create_cancelled_orders.py`. It records that an order was cancelled, so a late `OrderPlaced` can be refused.

### Handlers: `inventory/handlers.py`

* Extract `release_reservations(session, order_id)` from `handle_payment_failed`: add each reservation's quantity back to its product's stock and delete the reservation. It does not commit, and does nothing when the order has no reservations. `handle_payment_failed` keeps its behaviour.
* New `handle_order_cancelled(session, envelope)`:
  1. Return if the event id is in `processed_events`.
  2. `release_reservations(order_id)`.
  3. Add `order_id` to `cancelled_orders` unless it is already there. A repeated cancel brings a new `OrderCancelled` with a new event id.
  4. Record the event id and commit once. Publish nothing.
* `handle_order_placed`: right after the idempotency check, if `order_id` is in `cancelled_orders`, record the event id, commit and return. Reserve nothing, publish nothing. orders-service already holds the order as `CANCELLED`, and payments never receives a `StockReserved`.
* A `PaymentFailed` after a cancel finds no reservations and changes no stock. No code change.

`inventory/worker.py` subscribes to `OrderCancelled`.

## payments-service

### Model

* `PaymentStatus` gains `REFUNDED`. `payments.status` is a `String(20)` without a check constraint, so no migration is needed for it.
* New table `cancelled_orders (order_id String(36) primary key)`, added by `migrations/versions/0002_create_cancelled_orders.py`.

### Refund

The fake gateway's refund always succeeds. Refunding changes the payment's status from `CAPTURED` to `REFUNDED`. The payment keeps its id and amount. No new column.

### Handlers: `payments/handlers.py`

* New `handle_order_cancelled(session, envelope, publisher)`:
  1. Return if the event id is in `processed_events`.
  2. Add `order_id` to `cancelled_orders` unless it is already there.
  3. Look up the order's payment (at most one, by `uq_payments_order_id`):
     * `CAPTURED`: set it to `REFUNDED`, record the event id, commit, then publish `PaymentRefunded {order_id, payment_id, amount_cents}`.
     * `REFUNDED`: record the event id, commit, then publish `PaymentRefunded` again. This repairs a `PaymentRefunded` lost after its commit when the customer cancels again. orders' `refunded_at IS NULL` guard makes the duplicate harmless.
     * `FAILED`, or no payment: record the event id, commit, publish nothing.
* `handle_stock_reserved`: right after the idempotency check, if `order_id` is in `cancelled_orders`, record the event id, commit and return. Create no payment, capture nothing, publish nothing.

`payments/worker.py` subscribes to `OrderCancelled`.

## Event arrival orders

`OrderCancelled` travels on its own topic, and keying by `order_id` orders events only within one topic. Every service reaches the same end state either way:

| Service | Arrival order | Result |
| --- | --- | --- |
| inventory | `OrderPlaced`, then `OrderCancelled` | reserves, then releases |
| inventory | `OrderCancelled`, then `OrderPlaced` | marks cancelled, then reserves nothing |
| payments | `StockReserved`, then `OrderCancelled` | captures, then refunds |
| payments | `OrderCancelled`, then `StockReserved` | marks cancelled, then captures nothing |
| payments | inventory skipped reserving | no `StockReserved` arrives, no payment |
| orders | `PaymentCaptured` or a reject event after the cancel | matches no `PENDING` row, order stays `CANCELLED` |

Each worker handles its topics one event at a time in a single consumer loop, so in inventory and payments the "is it cancelled?" check and the reservation or capture that follows cannot interleave with `handle_order_cancelled`. This relies on running one worker per service, as today.

## Known limitations

These go into the services' READMEs.

* Publishing still happens after the commit. A lost `OrderCancelled` or `PaymentRefunded` is repaired by cancelling again. The existing gaps for `OrderPlaced`, `StockReserved` and `PaymentCaptured` remain.
* Run one worker per service. The cancelled-order checks depend on it.
* Out of scope: inventory overselling under concurrent reservations, authentication on cancel, cancelling part of an order, failing refunds, and a cutoff after which an order can no longer be cancelled.

## Testing

Test-first in every service, with the existing in-memory publishers and fixtures. No Kafka except in e2e. Each service also runs `uv run ruff check .`, `uv run ruff format --check .` and `uv run mypy src`.

### contracts: `tests/test_events.py`

* A valid `OrderCancelled` and a valid `PaymentRefunded` pass `validate_event`, and `topic_for` returns their topics.
* `OrderCancelled` without `order_id` fails. `PaymentRefunded` with a negative `amount_cents` fails.

### orders-service

* `tests/test_status.py`: `move_order` with `from_statuses=(PENDING, CONFIRMED)` moves a `CONFIRMED` order; with the default it does not; the stale-session case still holds.
* `tests/test_api.py`:
  * Cancelling a `PENDING` order and cancelling a `CONFIRMED` order each return 200 `CANCELLED` and publish exactly one `OrderCancelled` with the order id.
  * Cancelling a `CANCELLED` order returns 200 and publishes one more `OrderCancelled`.
  * Cancelling a `REJECTED` order returns 409 and publishes nothing. An unknown id returns 404 and publishes nothing.
  * `GET /orders/{id}` includes `refunded_at`, `null` until a refund.
* `tests/test_handlers.py`:
  * `PaymentRefunded` sets `refunded_at` and leaves the status alone. A redelivered event changes nothing. A second `PaymentRefunded` with a new event id keeps the first timestamp.
  * The existing tests pass unmodified.
* `tests/test_migrations.py`: models and migrations still agree.

### inventory-service: `tests/test_handlers.py`, `tests/test_migrations.py`

* `OrderPlaced`, then `OrderCancelled`: stock is back to its starting level, no reservations remain, the order is in `cancelled_orders`.
* `OrderCancelled`, then `OrderPlaced`: stock unchanged, no reservations, nothing published.
* A redelivered `OrderCancelled` changes nothing. A second `OrderCancelled` with a new event id changes nothing and does not fail.
* `OrderCancelled` for an order without reservations only records it as cancelled.
* The existing `PaymentFailed` tests pass unmodified.
* Models and migrations still agree.

### payments-service: `tests/test_handlers.py`, `tests/test_migrations.py`

* Captured payment, then `OrderCancelled`: status `REFUNDED`, exactly one `PaymentRefunded` with the payment's id and amount.
* `OrderCancelled`, then `StockReserved`: no payment row, nothing published.
* A redelivered `OrderCancelled` changes nothing and publishes nothing.
* A second `OrderCancelled` with a new event id, for a `REFUNDED` payment, publishes `PaymentRefunded` again and leaves the status alone.
* `OrderCancelled` for a `FAILED` payment publishes nothing.
* The existing capture and failure tests pass unmodified.
* Models and migrations still agree.

### e2e: `e2e/test_e2e.py`

Needs `./up.sh` (once more, for the new topics) and `./run-all.sh`.

* Cancelling a confirmed order: place `PEN-1`, wait for `CONFIRMED`, cancel (200, `CANCELLED`), then wait until the payment is `REFUNDED`, the order's `refunded_at` is set, and `PEN-1` stock is back to its level before the order.
* Cancelling right after placing: place `PEN-1` and cancel at once. The cancel may find the order `PENDING` or already `CONFIRMED`; both must end alike. Wait until stock is back to its level before the order and the payment is absent or `REFUNDED`. If a payment exists, `refunded_at` is set.
* Cancelling a rejected order: a `BOOK-1` order beyond the stock becomes `REJECTED`, and cancelling it returns 409.
* The module docstring notes that the cancel tests give their `PEN-1` back.

Arrival orders that real Kafka cannot be forced into (`OrderCancelled` first) are covered by the inventory and payments handler tests.

## Order of work

contracts first, since every service depends on it through `../contracts`. Then payments-service and inventory-service, which consume `OrderCancelled`. Then orders-service, which starts publishing it and consumes `PaymentRefunded`. e2e last.
