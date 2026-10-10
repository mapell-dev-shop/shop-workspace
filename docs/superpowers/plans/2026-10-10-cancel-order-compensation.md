# Cancel an Order with Stock Release and Refund Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cancelling a `PENDING` or `CONFIRMED` order puts its reserved stock back on sale and refunds a captured payment, whatever order the events arrive in.

**Architecture:** orders-service publishes a new `OrderCancelled` event. inventory-service and payments-service each record cancelled order ids in their own `cancelled_orders` table. They release or refund what they already hold, and refuse to reserve or capture for that order later. payments-service publishes `PaymentRefunded`, and orders-service records it as `refunded_at` on the order.

**Tech Stack:** Python 3, FastAPI, SQLAlchemy 2 (SQLite), Alembic, confluent-kafka through `shop_contracts`, JSON Schema, pytest, ruff, mypy (strict), uv.

**Spec:** `docs/superpowers/specs/2026-10-10-cancel-order-compensation-design.md` (workspace repo). Read it before starting any task.

## Global Constraints

* Each service directory is its own git repository, on branch `lesson-13`. Commit inside the repository you changed (`git -C <service> ...`), never in the workspace repo, except Task 6 (e2e) and this plan.
* Order of work: contracts (Task 1) first, because every service installs `../contracts` as an editable dependency. Then payments (Task 2), inventory (Task 3), orders (Tasks 4 and 5), and e2e (Task 6).
* New topics: `shop.orders.order-cancelled.v1` and `shop.payments.payment-refunded.v1`. Never hard-code a topic name in a service. Use `new_envelope` and `publisher.publish(envelope, key=order_id)`.
* Every event is keyed by `order_id`.
* Every handler returns early if `envelope["event_id"]` is in `processed_events`, and records the id in the same commit as its state change.
* Publish after the commit, as today. No outbox, no retries, no try/except around publish.
* A service never reads another service's database.
* Every model change gets an Alembic revision, and `tests/test_migrations.py` must pass. Keep batch operations (`render_as_batch=True`).
* Cancel reason text: `"cancelled by customer"`. The 409 text: `f"order is {order.status} and can no longer be cancelled"`.
* Inside every service, before each commit: `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy src`. All must pass.
* Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. Kafka refuses `OrderCancelled` after the cancel committed: the request fails, the order stays `CANCELLED`, and cancelling again publishes the event. Pinned in Task 4, `test_a_cancel_whose_event_was_lost_is_repaired_by_cancelling_again`.
2. A worker not subscribed to its new event: every handler test passes, but in production nothing is released or refunded. Pinned by a worker test in Tasks 2, 3 and 5.
3. `PaymentFailed` and `OrderCancelled` both reach inventory for the same order (the cancel won the race against the failed payment): the stock comes back exactly once. Pinned in Task 3, `test_a_payment_failure_and_a_cancel_give_the_stock_back_once`.
4. `PaymentRefunded` for an order orders-service does not know: it is recorded as handled and does not crash the worker. Pinned in Task 5, `test_a_refund_for_an_unknown_order_is_recorded_and_ignored`.
5. A cancelled order with several lines: every line's stock comes back, not only the first. Pinned in Task 3, where the cancel tests use a two-line order.

---

### Task 1: contracts — `OrderCancelled` and `PaymentRefunded`

**Files:**
- Create: `contracts/src/shop_contracts/schemas/events/order-cancelled.v1.schema.json`
- Create: `contracts/src/shop_contracts/schemas/events/payment-refunded.v1.schema.json`
- Modify: `contracts/src/shop_contracts/events.py:13-19` (`EVENTS`)
- Test: `contracts/tests/test_events.py`

**Interfaces:**
- Consumes: nothing.
- Produces: event types `"OrderCancelled"` (`data: {order_id: str}`) and `"PaymentRefunded"` (`data: {order_id: str, payment_id: str, amount_cents: int >= 0}`), usable with `new_envelope`, `validate_event`, `topic_for`, and the `InMemoryEventPublisher` used in every service's tests.

- [ ] **Step 1: Write the failing tests**

Append to `contracts/tests/test_events.py`:

```python
def test_order_cancelled_carries_the_order_id() -> None:
    validate_event(new_envelope("OrderCancelled", {"order_id": "o-1"}))
    assert topic_for("OrderCancelled") == "shop.orders.order-cancelled.v1"


def test_order_cancelled_without_an_order_id_is_rejected() -> None:
    with pytest.raises(ValidationError):
        validate_event(new_envelope("OrderCancelled", {}))


def test_payment_refunded_carries_the_payment_and_amount() -> None:
    validate_event(
        new_envelope(
            "PaymentRefunded", {"order_id": "o-1", "payment_id": "p-1", "amount_cents": 5000}
        )
    )
    assert topic_for("PaymentRefunded") == "shop.payments.payment-refunded.v1"


def test_payment_refunded_with_a_negative_amount_is_rejected() -> None:
    envelope = new_envelope(
        "PaymentRefunded", {"order_id": "o-1", "payment_id": "p-1", "amount_cents": -1}
    )
    with pytest.raises(ValidationError):
        validate_event(envelope)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd contracts && uv run pytest tests/test_events.py -v`
Expected: the four new tests FAIL with `KeyError: 'OrderCancelled'` / `KeyError: 'PaymentRefunded'`.

- [ ] **Step 3: Add the schemas**

`contracts/src/shop_contracts/schemas/events/order-cancelled.v1.schema.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "OrderCancelled v1",
  "description": "Published by orders-service when a customer cancels a PENDING or CONFIRMED order. Published again when the customer cancels an already cancelled order.",
  "type": "object",
  "required": ["event_id", "type", "version", "occurred_at", "data"],
  "properties": {
    "event_id": {"type": "string", "format": "uuid"},
    "type": {"const": "OrderCancelled"},
    "version": {"const": 1},
    "occurred_at": {"type": "string", "format": "date-time"},
    "data": {
      "type": "object",
      "required": ["order_id"],
      "properties": {
        "order_id": {"type": "string"}
      }
    }
  }
}
```

`contracts/src/shop_contracts/schemas/events/payment-refunded.v1.schema.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "PaymentRefunded v1",
  "description": "Published by payments-service when the captured payment of a cancelled order is refunded. Published again when the order is cancelled again.",
  "type": "object",
  "required": ["event_id", "type", "version", "occurred_at", "data"],
  "properties": {
    "event_id": {"type": "string", "format": "uuid"},
    "type": {"const": "PaymentRefunded"},
    "version": {"const": 1},
    "occurred_at": {"type": "string", "format": "date-time"},
    "data": {
      "type": "object",
      "required": ["order_id", "payment_id", "amount_cents"],
      "properties": {
        "order_id": {"type": "string"},
        "payment_id": {"type": "string"},
        "amount_cents": {"type": "integer", "minimum": 0}
      }
    }
  }
}
```

- [ ] **Step 4: Register them in `EVENTS`**

In `contracts/src/shop_contracts/events.py`, add two entries after `"PaymentFailed"`:

```python
    "OrderCancelled": ("shop.orders.order-cancelled.v1", "order-cancelled.v1.schema.json"),
    "PaymentRefunded": ("shop.payments.payment-refunded.v1", "payment-refunded.v1.schema.json"),
```

- [ ] **Step 5: Run the whole contracts suite and checks**

Run: `cd contracts && uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all PASS. The parametrized `test_every_event_type_has_a_valid_schema_that_names_it` and `test_topic_names_follow_the_convention` now also cover the two new events.

- [ ] **Step 6: Commit**

```bash
git -C contracts add src/shop_contracts/schemas/events/order-cancelled.v1.schema.json \
  src/shop_contracts/schemas/events/payment-refunded.v1.schema.json \
  src/shop_contracts/events.py tests/test_events.py
git -C contracts commit -m "feat: OrderCancelled and PaymentRefunded events

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: payments-service — refund on cancel, never capture for a cancelled order

**Files:**
- Modify: `payments-service/src/payments/models.py` (add `REFUNDED`, add `CancelledOrder`)
- Create: `payments-service/migrations/versions/0002_create_cancelled_orders.py`
- Modify: `payments-service/src/payments/handlers.py` (guard in `handle_stock_reserved`, new `handle_order_cancelled`)
- Modify: `payments-service/src/payments/worker.py`
- Modify: `payments-service/README.md`
- Test: `payments-service/tests/test_handlers.py`, create `payments-service/tests/test_worker.py`

**Interfaces:**
- Consumes: `"OrderCancelled"` and `"PaymentRefunded"` from Task 1.
- Produces: `PaymentStatus.REFUNDED = "REFUNDED"`; `CancelledOrder` model (table `cancelled_orders`, `order_id: str` primary key); `handle_order_cancelled(session: Session, envelope: dict[str, Any], publisher: EventPublisher) -> None`; `PaymentRefunded` events `{order_id, payment_id, amount_cents}`; `GET /payments/{order_id}` now can return `status: "REFUNDED"` (used by Task 6).

- [ ] **Step 1: Write the failing handler tests**

In `payments-service/tests/test_handlers.py`, change the imports and the `stock_reserved` helper, then append the tests:

```python
from shop_contracts.messaging import InMemoryEventPublisher
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from payments.handlers import CAPTURE_LIMIT_CENTS, handle_order_cancelled, handle_stock_reserved
from payments.models import CancelledOrder, Payment, ProcessedEvent
from tests.conftest import event


def stock_reserved(total_cents: int) -> dict[str, object]:
    return event("StockReserved", {"order_id": "o-1", "total_cents": total_cents})


def order_cancelled(order_id: str = "o-1") -> dict[str, object]:
    return event("OrderCancelled", {"order_id": order_id})
```

(keep the four existing tests unchanged between the helpers and the new tests)

```python
def captured_payment(session: Session, publisher: InMemoryEventPublisher) -> Payment:
    handle_stock_reserved(session, stock_reserved(5000), publisher)
    publisher.published.clear()
    payment = session.scalar(select(Payment))
    assert payment is not None and payment.status == "CAPTURED"
    return payment


def test_cancelling_a_captured_order_refunds_it(
    session: Session, publisher: InMemoryEventPublisher
) -> None:
    payment = captured_payment(session, publisher)

    handle_order_cancelled(session, order_cancelled(), publisher)

    assert payment.status == "REFUNDED"
    assert payment.amount_cents == 5000
    [envelope] = publisher.published
    assert envelope["type"] == "PaymentRefunded"
    assert envelope["data"] == {"order_id": "o-1", "payment_id": payment.id, "amount_cents": 5000}


def test_a_cancel_before_the_stock_is_reserved_means_nothing_is_ever_charged(
    session: Session, publisher: InMemoryEventPublisher
) -> None:
    handle_order_cancelled(session, order_cancelled(), publisher)
    late = stock_reserved(5000)

    handle_stock_reserved(session, late, publisher)

    assert session.scalar(select(func.count()).select_from(Payment)) == 0
    assert publisher.published == []
    assert session.get(CancelledOrder, "o-1") is not None
    assert session.get(ProcessedEvent, late["event_id"]) is not None


def test_a_redelivered_cancel_refunds_once(
    session: Session, publisher: InMemoryEventPublisher
) -> None:
    captured_payment(session, publisher)
    envelope = order_cancelled()

    handle_order_cancelled(session, envelope, publisher)
    handle_order_cancelled(session, envelope, publisher)

    assert len(publisher.published) == 1


def test_cancelling_again_announces_the_refund_again(
    session: Session, publisher: InMemoryEventPublisher
) -> None:
    payment = captured_payment(session, publisher)

    handle_order_cancelled(session, order_cancelled(), publisher)
    handle_order_cancelled(session, order_cancelled(), publisher)

    assert payment.status == "REFUNDED"
    assert [e["type"] for e in publisher.published] == ["PaymentRefunded", "PaymentRefunded"]
    assert publisher.published[0]["data"] == publisher.published[1]["data"]
    assert session.scalar(select(func.count()).select_from(CancelledOrder)) == 1


def test_cancelling_an_order_whose_payment_failed_refunds_nothing(
    session: Session, publisher: InMemoryEventPublisher
) -> None:
    handle_stock_reserved(session, stock_reserved(CAPTURE_LIMIT_CENTS + 1), publisher)
    publisher.published.clear()

    handle_order_cancelled(session, order_cancelled(), publisher)

    payment = session.scalar(select(Payment))
    assert payment is not None and payment.status == "FAILED"
    assert publisher.published == []
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd payments-service && uv run pytest tests/test_handlers.py -v`
Expected: collection ERROR, `ImportError: cannot import name 'handle_order_cancelled'`.

- [ ] **Step 3: Add the model changes**

In `payments-service/src/payments/models.py`:

```python
class PaymentStatus(StrEnum):
    CAPTURED = "CAPTURED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"
```

and after `Payment`:

```python
class CancelledOrder(Base):
    """Orders cancelled by the customer, so a late StockReserved never charges them."""

    __tablename__ = "cancelled_orders"

    order_id: Mapped[str] = mapped_column(String(36), primary_key=True)
```

- [ ] **Step 4: Add the migration**

`payments-service/migrations/versions/0002_create_cancelled_orders.py`:

```python
"""create cancelled_orders

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cancelled_orders",
        sa.Column("order_id", sa.String(36), primary_key=True),
    )


def downgrade() -> None:
    op.drop_table("cancelled_orders")
```

- [ ] **Step 5: Implement the handlers**

In `payments-service/src/payments/handlers.py`, add `from sqlalchemy import select` and import `CancelledOrder`. Then make `handle_stock_reserved` refuse cancelled orders, right after its idempotency check:

```python
    if session.get(ProcessedEvent, envelope["event_id"]) is not None:
        return
    order_id = envelope["data"]["order_id"]
    if session.get(CancelledOrder, order_id) is not None:
        session.add(ProcessedEvent(event_id=envelope["event_id"]))
        session.commit()
        return
    amount = envelope["data"]["total_cents"]
```

(remove the original `order_id = ...` line further down so it is assigned once). Append:

```python
def handle_order_cancelled(
    session: Session, envelope: dict[str, Any], publisher: EventPublisher
) -> None:
    """Refund the order's captured payment, and remember the order so it is never charged.

    A repeated cancel arrives with a new event id. It finds the payment REFUNDED and
    announces the refund again, which repairs a PaymentRefunded lost after its commit.
    """
    if session.get(ProcessedEvent, envelope["event_id"]) is not None:
        return
    order_id = envelope["data"]["order_id"]
    if session.get(CancelledOrder, order_id) is None:
        session.add(CancelledOrder(order_id=order_id))
    payment = session.scalar(select(Payment).where(Payment.order_id == order_id))
    if payment is not None and payment.status == PaymentStatus.CAPTURED:
        payment.status = PaymentStatus.REFUNDED
    session.add(ProcessedEvent(event_id=envelope["event_id"]))
    session.commit()

    if payment is not None and payment.status == PaymentStatus.REFUNDED:
        publisher.publish(
            new_envelope(
                "PaymentRefunded",
                {
                    "order_id": order_id,
                    "payment_id": payment.id,
                    "amount_cents": payment.amount_cents,
                },
            ),
            key=order_id,
        )
```

- [ ] **Step 6: Run the handler and migration tests**

Run: `cd payments-service && uv run pytest tests/test_handlers.py tests/test_migrations.py -v`
Expected: all PASS, including the four tests that existed before.

- [ ] **Step 7: Write the failing worker test**

`payments-service/tests/test_worker.py`:

```python
from typing import Any

import pytest
from shop_contracts.messaging import InMemoryEventPublisher

from payments import worker


def test_the_worker_listens_for_every_event_it_handles(monkeypatch: pytest.MonkeyPatch) -> None:
    subscribed: dict[str, Any] = {}
    monkeypatch.setattr(worker, "KafkaEventPublisher", lambda servers: InMemoryEventPublisher())
    monkeypatch.setattr(
        worker, "run_consumer", lambda servers, group, handlers: subscribed.update(handlers)
    )

    worker.main()

    assert set(subscribed) == {"StockReserved", "OrderCancelled"}
```

Run: `cd payments-service && uv run pytest tests/test_worker.py -v`
Expected: FAIL, the set lacks `"OrderCancelled"`.

- [ ] **Step 8: Subscribe the worker**

In `payments-service/src/payments/worker.py`, import `handle_order_cancelled` and change `main()`:

```python
def main() -> None:
    publisher = KafkaEventPublisher(settings.KAFKA_BOOTSTRAP_SERVERS)

    def on_stock_reserved(envelope: dict[str, Any]) -> None:
        with SessionLocal() as session:
            handle_stock_reserved(session, envelope, publisher)

    def on_order_cancelled(envelope: dict[str, Any]) -> None:
        with SessionLocal() as session:
            handle_order_cancelled(session, envelope, publisher)

    run_consumer(
        settings.KAFKA_BOOTSTRAP_SERVERS,
        "payments-service",
        {"StockReserved": on_stock_reserved, "OrderCancelled": on_order_cancelled},
    )
```

Run: `cd payments-service && uv run pytest tests/test_worker.py -v`
Expected: PASS.

- [ ] **Step 9: Update the README**

In `payments-service/README.md`, in the section that lists the events, state that it consumes `OrderCancelled`, refunds a `CAPTURED` payment (status `REFUNDED`) and publishes `PaymentRefunded`, re-publishing it when the order is cancelled again, and that a `StockReserved` for a cancelled order captures nothing. Under "Known limitations" add:

```markdown
* Run one worker at a time. A late `StockReserved` is refused only because the worker handles `OrderCancelled` and `StockReserved` one after the other.
* A lost `PaymentRefunded` is published again when the customer cancels the order again.
```

- [ ] **Step 10: Run all checks**

Run: `cd payments-service && uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all PASS.

- [ ] **Step 11: Commit**

```bash
git -C payments-service add src/payments/models.py src/payments/handlers.py src/payments/worker.py \
  migrations/versions/0002_create_cancelled_orders.py tests/test_handlers.py tests/test_worker.py README.md
git -C payments-service commit -m "feat: refund a cancelled order and never charge one

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: inventory-service — give stock back on cancel, never reserve for a cancelled order

**Files:**
- Modify: `inventory-service/src/inventory/models.py` (add `CancelledOrder`)
- Create: `inventory-service/migrations/versions/0002_create_cancelled_orders.py`
- Modify: `inventory-service/src/inventory/handlers.py` (extract `release_reservations`, guard in `handle_order_placed`, new `handle_order_cancelled`)
- Modify: `inventory-service/src/inventory/worker.py`
- Modify: `inventory-service/README.md`
- Test: `inventory-service/tests/test_handlers.py`, create `inventory-service/tests/test_worker.py`

**Interfaces:**
- Consumes: `"OrderCancelled"` from Task 1.
- Produces: `CancelledOrder` model (table `cancelled_orders`, `order_id: str` primary key); `release_reservations(session: Session, order_id: str) -> None` (does not commit); `handle_order_cancelled(session: Session, envelope: dict[str, Any]) -> None` (publishes nothing).

Seeded stock in tests (`inventory/seed.py`): `BOOK-1` 10, `PEN-1` 100, `LAPTOP-1` 5.

- [ ] **Step 1: Write the failing handler tests**

In `inventory-service/tests/test_handlers.py`, change the imports:

```python
from inventory.handlers import handle_order_cancelled, handle_order_placed, handle_payment_failed
from inventory.models import CancelledOrder, ProcessedEvent, Product, Reservation
```

and append:

```python
def order_cancelled(order_id: str = "o-1") -> dict[str, Any]:
    return event("OrderCancelled", {"order_id": order_id})


def test_cancelling_a_reserved_order_gives_every_line_back(
    session: Session, publisher: InMemoryEventPublisher
) -> None:
    handle_order_placed(session, order_placed(("BOOK-1", 2), ("PEN-1", 5)), publisher)
    publisher.published.clear()

    handle_order_cancelled(session, order_cancelled())

    assert stock(session, "BOOK-1") == 10
    assert stock(session, "PEN-1") == 100
    assert reservations(session) == []
    assert session.get(CancelledOrder, "o-1") is not None
    assert publisher.published == []


def test_a_cancel_before_the_order_arrives_means_nothing_is_ever_reserved(
    session: Session, publisher: InMemoryEventPublisher
) -> None:
    handle_order_cancelled(session, order_cancelled())
    late = order_placed(("BOOK-1", 2), ("PEN-1", 5))

    handle_order_placed(session, late, publisher)

    assert stock(session, "BOOK-1") == 10
    assert stock(session, "PEN-1") == 100
    assert reservations(session) == []
    assert publisher.published == []
    assert session.get(ProcessedEvent, late["event_id"]) is not None


def test_a_redelivered_cancel_gives_the_stock_back_once(
    session: Session, publisher: InMemoryEventPublisher
) -> None:
    handle_order_placed(session, order_placed(("BOOK-1", 2)), publisher)
    envelope = order_cancelled()

    handle_order_cancelled(session, envelope)
    handle_order_cancelled(session, envelope)

    assert stock(session, "BOOK-1") == 10


def test_cancelling_again_changes_nothing(
    session: Session, publisher: InMemoryEventPublisher
) -> None:
    handle_order_placed(session, order_placed(("BOOK-1", 2)), publisher)

    handle_order_cancelled(session, order_cancelled())
    handle_order_cancelled(session, order_cancelled())

    assert stock(session, "BOOK-1") == 10
    assert session.scalar(select(func.count()).select_from(CancelledOrder)) == 1


def test_cancelling_an_order_without_reservations_only_remembers_it(session: Session) -> None:
    handle_order_cancelled(session, order_cancelled("o-unknown"))

    assert stock(session, "BOOK-1") == 10
    assert session.get(CancelledOrder, "o-unknown") is not None


def test_a_payment_failure_and_a_cancel_give_the_stock_back_once(
    session: Session, publisher: InMemoryEventPublisher
) -> None:
    handle_order_placed(session, order_placed(("BOOK-1", 2), ("PEN-1", 5)), publisher)

    handle_payment_failed(session, payment_failed())
    handle_order_cancelled(session, order_cancelled())

    assert stock(session, "BOOK-1") == 10
    assert stock(session, "PEN-1") == 100
```

Also change `from sqlalchemy import select` to `from sqlalchemy import func, select`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cd inventory-service && uv run pytest tests/test_handlers.py -v`
Expected: collection ERROR, `ImportError: cannot import name 'handle_order_cancelled'`.

- [ ] **Step 3: Add the model**

In `inventory-service/src/inventory/models.py`, after `Reservation`:

```python
class CancelledOrder(Base):
    """Orders cancelled by the customer, so a late OrderPlaced never reserves stock for them."""

    __tablename__ = "cancelled_orders"

    order_id: Mapped[str] = mapped_column(String(36), primary_key=True)
```

- [ ] **Step 4: Add the migration**

`inventory-service/migrations/versions/0002_create_cancelled_orders.py`:

```python
"""create cancelled_orders

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cancelled_orders",
        sa.Column("order_id", sa.String(36), primary_key=True),
    )


def downgrade() -> None:
    op.drop_table("cancelled_orders")
```

- [ ] **Step 5: Implement the handlers**

In `inventory-service/src/inventory/handlers.py`, import `CancelledOrder`. In `handle_order_placed`, right after the idempotency check:

```python
    if session.get(ProcessedEvent, envelope["event_id"]) is not None:
        return
    order_id = envelope["data"]["order_id"]
    if session.get(CancelledOrder, order_id) is not None:
        session.add(ProcessedEvent(event_id=envelope["event_id"]))
        session.commit()
        return
    lines = envelope["data"]["lines"]
```

(remove the original `order_id = ...` line so it is assigned once). Replace `handle_payment_failed` and add the new functions:

```python
def release_reservations(session: Session, order_id: str) -> None:
    """Give back the stock held for the order and drop its reservations. Does not commit."""
    held = list(session.scalars(select(Reservation).where(Reservation.order_id == order_id)))
    for reservation in held:
        product = session.get(Product, reservation.sku)
        if product is not None:
            product.stock += reservation.quantity
        session.delete(reservation)


def handle_payment_failed(session: Session, envelope: dict[str, Any]) -> None:
    """Give back the stock reserved for an order whose payment failed."""
    if session.get(ProcessedEvent, envelope["event_id"]) is not None:
        return
    release_reservations(session, envelope["data"]["order_id"])
    session.add(ProcessedEvent(event_id=envelope["event_id"]))
    session.commit()


def handle_order_cancelled(session: Session, envelope: dict[str, Any]) -> None:
    """Give back the stock of a cancelled order, and remember it so it is never reserved."""
    if session.get(ProcessedEvent, envelope["event_id"]) is not None:
        return
    order_id = envelope["data"]["order_id"]
    release_reservations(session, order_id)
    if session.get(CancelledOrder, order_id) is None:
        session.add(CancelledOrder(order_id=order_id))
    session.add(ProcessedEvent(event_id=envelope["event_id"]))
    session.commit()
```

- [ ] **Step 6: Run the handler and migration tests**

Run: `cd inventory-service && uv run pytest tests/test_handlers.py tests/test_migrations.py -v`
Expected: all PASS, including every existing `PaymentFailed` test, unmodified.

- [ ] **Step 7: Write the failing worker test**

`inventory-service/tests/test_worker.py`:

```python
from typing import Any

import pytest
from shop_contracts.messaging import InMemoryEventPublisher

from inventory import worker


def test_the_worker_listens_for_every_event_it_handles(monkeypatch: pytest.MonkeyPatch) -> None:
    subscribed: dict[str, Any] = {}
    monkeypatch.setattr(worker, "KafkaEventPublisher", lambda servers: InMemoryEventPublisher())
    monkeypatch.setattr(
        worker, "run_consumer", lambda servers, group, handlers: subscribed.update(handlers)
    )

    worker.main()

    assert set(subscribed) == {"OrderPlaced", "PaymentFailed", "OrderCancelled"}
```

Run: `cd inventory-service && uv run pytest tests/test_worker.py -v`
Expected: FAIL, the set lacks `"OrderCancelled"`.

- [ ] **Step 8: Subscribe the worker**

In `inventory-service/src/inventory/worker.py`, import `handle_order_cancelled`, add next to `on_payment_failed`:

```python
    def on_order_cancelled(envelope: dict[str, Any]) -> None:
        with SessionLocal() as session:
            handle_order_cancelled(session, envelope)
```

and pass it to `run_consumer`:

```python
    run_consumer(
        settings.KAFKA_BOOTSTRAP_SERVERS,
        "inventory-service",
        {
            "OrderPlaced": on_order_placed,
            "PaymentFailed": on_payment_failed,
            "OrderCancelled": on_order_cancelled,
        },
    )
```

Run: `cd inventory-service && uv run pytest tests/test_worker.py -v`
Expected: PASS.

- [ ] **Step 9: Update the README**

In `inventory-service/README.md`, in the section that lists the events, state that it consumes `OrderCancelled`, gives the order's reserved stock back, and refuses a later `OrderPlaced` for that order without publishing anything. Extend the "Run one worker at a time" limitation:

```markdown
* Run one worker at a time: two workers could both read the same stock level and oversell, and a late `OrderPlaced` is refused only because one worker handles it after the `OrderCancelled`.
```

- [ ] **Step 10: Run all checks**

Run: `cd inventory-service && uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all PASS.

- [ ] **Step 11: Commit**

```bash
git -C inventory-service add src/inventory/models.py src/inventory/handlers.py src/inventory/worker.py \
  migrations/versions/0002_create_cancelled_orders.py tests/test_handlers.py tests/test_worker.py README.md
git -C inventory-service commit -m "feat: give a cancelled order's stock back and never reserve for it

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: orders-service — cancel `CONFIRMED` orders and publish `OrderCancelled`

**Files:**
- Modify: `orders-service/src/orders/status.py` (`move_pending` → `move_order`)
- Modify: `orders-service/src/orders/handlers.py`, `orders-service/src/orders/api.py` (callers, cancel endpoint)
- Modify: `orders-service/tests/test_status.py`, `orders-service/tests/test_handlers.py` (rename), `orders-service/tests/test_api.py`
- Modify: `orders-service/README.md`, `orders-service/CLAUDE.md`

**Interfaces:**
- Consumes: `"OrderCancelled"` from Task 1.
- Produces: `move_order(session: Session, order_id: str, new_status: OrderStatus, *, from_statuses: tuple[OrderStatus, ...] = (OrderStatus.PENDING,), reason: str | None = None, **fields: Any) -> bool`. `move_pending` is removed. `POST /orders/{id}/cancel` publishes `OrderCancelled {order_id}`.

- [ ] **Step 1: Rename `move_pending` to `move_order` (refactor, no behaviour change)**

In `orders-service/src/orders/status.py`, replace the module docstring and the function:

```python
"""The one way an order changes status. The database decides, not the session."""
```

```python
def move_order(
    session: Session,
    order_id: str,
    new_status: OrderStatus,
    *,
    from_statuses: tuple[OrderStatus, ...] = (OrderStatus.PENDING,),
    reason: str | None = None,
    **fields: Any,
) -> bool:
    """Move the order to new_status if the database still has it in one of from_statuses.

    Does not commit. Returns whether this call moved it. Whoever won, the session's copy of
    the order is expired, so the next read shows what the database holds.
    """
    result = cast(
        CursorResult[Any],
        session.execute(
            update(Order)
            .where(Order.id == order_id, Order.status.in_(from_statuses))
            .values(status=new_status, status_reason=reason, **fields),
            # The default would apply the SET to a stale in-session copy that still looks
            # PENDING, even when the database matched no row.
            execution_options={"synchronize_session": False},
        ),
    )
    held = session.identity_map.get(Session.identity_key(Order, order_id))
    if held is not None:
        session.expire(held)
    return result.rowcount == 1
```

Replace every `move_pending` with `move_order` in `src/orders/handlers.py`, `src/orders/api.py`, `tests/test_status.py` and `tests/test_handlers.py` (imports and calls). Every existing call passes `reason` by keyword or not at all, so the keyword-only change needs no other edit. Check with `grep -rn move_pending src tests`, which must print nothing.

- [ ] **Step 2: Run the suite to verify the refactor changed nothing**

Run: `cd orders-service && uv run pytest && uv run mypy src`
Expected: all PASS.

- [ ] **Step 3: Write the failing `move_order` tests**

Append to `orders-service/tests/test_status.py`:

```python
def test_a_confirmed_order_moves_when_confirmed_is_allowed(session: Session) -> None:
    order = pending_order(session)
    order.status = OrderStatus.CONFIRMED
    session.commit()

    assert move_order(
        session,
        "o-1",
        OrderStatus.CANCELLED,
        from_statuses=(OrderStatus.PENDING, OrderStatus.CONFIRMED),
        reason="cancelled by customer",
    )
    session.commit()

    assert order.status == OrderStatus.CANCELLED
    assert order.status_reason == "cancelled by customer"


def test_a_rejected_order_does_not_move_even_when_confirmed_is_allowed(session: Session) -> None:
    order = pending_order(session)
    order.status = OrderStatus.REJECTED
    session.commit()

    assert not move_order(
        session,
        "o-1",
        OrderStatus.CANCELLED,
        from_statuses=(OrderStatus.PENDING, OrderStatus.CONFIRMED),
    )
    session.commit()

    assert order.status == OrderStatus.REJECTED
```

Run: `cd orders-service && uv run pytest tests/test_status.py -v`
Expected: PASS already (Step 1 added `from_statuses`). These pin the behaviour the endpoint relies on.

- [ ] **Step 4: Write the failing API tests**

In `orders-service/tests/test_api.py`, add imports:

```python
from typing import Any

from shop_contracts.messaging import EventNotDelivered, InMemoryEventPublisher

from orders.api import app, get_publisher
```

Replace `test_cancelling_an_order_that_is_no_longer_pending_is_refused` and `test_cancelling_publishes_nothing` with:

```python
def cancels(publisher: InMemoryEventPublisher) -> list[dict[str, Any]]:
    return [e["data"] for e in publisher.published if e["type"] == "OrderCancelled"]


def test_cancelling_a_rejected_order_is_refused(
    client: TestClient, session: Session, publisher: InMemoryEventPublisher
) -> None:
    order_id = str(place(client)["id"])
    set_status_elsewhere(session, order_id, OrderStatus.REJECTED, "insufficient stock: BOOK-1")

    response = client.post(f"/orders/{order_id}/cancel")

    assert response.status_code == 409
    assert response.json()["detail"] == "order is REJECTED and can no longer be cancelled"
    after = client.get(f"/orders/{order_id}").json()
    assert after["status"] == "REJECTED"
    assert after["status_reason"] == "insufficient stock: BOOK-1"
    assert cancels(publisher) == []


def test_cancelling_a_confirmed_order(
    client: TestClient, session: Session, publisher: InMemoryEventPublisher
) -> None:
    order_id = str(place(client)["id"])
    set_status_elsewhere(session, order_id, OrderStatus.CONFIRMED, None)

    response = client.post(f"/orders/{order_id}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "CANCELLED"
    assert response.json()["status_reason"] == "cancelled by customer"
    assert cancels(publisher) == [{"order_id": order_id}]


def test_cancelling_a_pending_order_tells_the_shop(
    client: TestClient, publisher: InMemoryEventPublisher
) -> None:
    order_id = place(client)["id"]

    client.post(f"/orders/{order_id}/cancel")

    assert cancels(publisher) == [{"order_id": order_id}]


def test_cancelling_again_tells_the_shop_again(
    client: TestClient, publisher: InMemoryEventPublisher
) -> None:
    order_id = place(client)["id"]

    client.post(f"/orders/{order_id}/cancel")
    client.post(f"/orders/{order_id}/cancel")

    assert cancels(publisher) == [{"order_id": order_id}, {"order_id": order_id}]


def test_cancelling_a_missing_order_tells_nobody(
    client: TestClient, publisher: InMemoryEventPublisher
) -> None:
    client.post("/orders/does-not-exist/cancel")

    assert cancels(publisher) == []


class RefusingPublisher:
    """Kafka refuses every event until refuse is set to False."""

    def __init__(self) -> None:
        self.refuse = True
        self.published: list[dict[str, Any]] = []

    def publish(self, envelope: dict[str, Any], key: str) -> None:
        if self.refuse:
            raise EventNotDelivered("Kafka did not confirm")
        self.published.append(envelope)


def test_a_cancel_whose_event_was_lost_is_repaired_by_cancelling_again(
    client: TestClient,
) -> None:
    order_id = place(client)["id"]
    kafka = RefusingPublisher()
    app.dependency_overrides[get_publisher] = lambda: kafka

    with pytest.raises(EventNotDelivered):
        client.post(f"/orders/{order_id}/cancel")
    assert client.get(f"/orders/{order_id}").json()["status"] == "CANCELLED"

    kafka.refuse = False
    response = client.post(f"/orders/{order_id}/cancel")

    assert response.status_code == 200
    assert [e["type"] for e in kafka.published] == ["OrderCancelled"]
```

(`TestClient` re-raises server exceptions by default, which is why the first call is wrapped in `pytest.raises`. The `client` fixture clears `dependency_overrides` at teardown.)

- [ ] **Step 5: Run them to verify they fail**

Run: `cd orders-service && uv run pytest tests/test_api.py -v`
Expected: `test_cancelling_a_confirmed_order` FAILS with 409; the publish tests FAIL because nothing is published; `test_a_cancel_whose_event_was_lost...` FAILS with `DID NOT RAISE`.

- [ ] **Step 6: Implement the endpoint**

In `orders-service/src/orders/api.py`, replace `cancel_order`:

```python
@app.post("/orders/{order_id}/cancel")
def cancel_order(
    order_id: str,
    session: SessionDep,
    publisher: Annotated[EventPublisher, Depends(get_publisher)],
) -> OrderOut:
    """Cancel a PENDING or CONFIRMED order, and tell inventory and payments.

    Cancelling a CANCELLED order publishes OrderCancelled again. That repairs an event lost
    between the commit and the publish, and inventory and payments ignore the repeat.
    """
    moved = move_order(
        session,
        order_id,
        OrderStatus.CANCELLED,
        from_statuses=(OrderStatus.PENDING, OrderStatus.CONFIRMED),
        reason="cancelled by customer",
    )
    session.commit()
    order = session.get(Order, order_id)
    if order is None:
        raise HTTPException(404, "order not found")
    if not moved and order.status != OrderStatus.CANCELLED:
        raise HTTPException(409, f"order is {order.status} and can no longer be cancelled")
    publisher.publish(new_envelope("OrderCancelled", {"order_id": order.id}), key=order.id)
    return to_out(order)
```

- [ ] **Step 7: Run the API tests**

Run: `cd orders-service && uv run pytest tests/test_api.py -v`
Expected: all PASS, including `test_cancelling_a_pending_order`, `test_cancelling_twice_returns_the_same_order`, `test_cancelling_a_missing_order_gives_404` and `test_cancelling_does_not_need_inventory`, unmodified.

- [ ] **Step 8: Update the docs**

`orders-service/README.md`:
* In "Events", add "Publishes `OrderCancelled` when an order is cancelled" and replace the cancel bullet with: "`POST /orders/{id}/cancel` moves a `PENDING` or `CONFIRMED` order to `CANCELLED` and publishes `OrderCancelled`, so inventory gives the stock back and payments refunds the money. Cancelling a `CANCELLED` order publishes it again. A `REJECTED` order cannot be cancelled (409). Events never move a `CANCELLED` order."
* In "Known limitations", delete the bullet that starts "`POST /orders/{id}/cancel` cancels a `PENDING` order in orders-service only". Change the first bullet to cover both events: "`OrderPlaced` and `OrderCancelled` are published after the database commit. ... If `OrderCancelled` is lost, cancelling the order again publishes it again." Add: "The design of cancelling is in `../docs/superpowers/specs/2026-10-10-cancel-order-compensation-design.md` in the shop workspace."

`orders-service/CLAUDE.md`, "Architecture rules": change "Events only move a `PENDING` order. A late or duplicate event must never change a `CONFIRMED` or `REJECTED` order." to "Events only move a `PENDING` order. A late or duplicate event must never change the status of a `CONFIRMED`, `REJECTED` or `CANCELLED` order. Every status change goes through `move_order()`." and "`OrderPlaced` is published after the commit" to "`OrderPlaced` and `OrderCancelled` are published after the commit".

- [ ] **Step 9: Run all checks**

Run: `cd orders-service && uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all PASS.

- [ ] **Step 10: Commit**

```bash
git -C orders-service add src/orders/status.py src/orders/handlers.py src/orders/api.py \
  tests/test_status.py tests/test_handlers.py tests/test_api.py README.md CLAUDE.md
git -C orders-service commit -m "feat: cancel confirmed orders too and publish OrderCancelled

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: orders-service — record the refund

**Files:**
- Modify: `orders-service/src/orders/models.py` (`refunded_at`)
- Create: `orders-service/migrations/versions/0003_add_refunded_at_to_orders.py`
- Modify: `orders-service/src/orders/api.py` (`OrderOut`, `to_out`)
- Modify: `orders-service/src/orders/handlers.py` (new `handle_payment_refunded`)
- Modify: `orders-service/src/orders/worker.py`
- Modify: `orders-service/README.md`
- Test: `orders-service/tests/test_handlers.py`, `orders-service/tests/test_api.py`, create `orders-service/tests/test_worker.py`

**Interfaces:**
- Consumes: `"PaymentRefunded"` from Task 1; `cancelled_order(session, order_id="o-1")` and `pending_order(...)` helpers already in `tests/test_handlers.py`.
- Produces: `Order.refunded_at: datetime | None`; `OrderOut.refunded_at: datetime | None` (JSON: ISO string or `null`, used by Task 6); `handle_payment_refunded(session: Session, envelope: dict[str, Any]) -> None`.

- [ ] **Step 1: Write the failing handler tests**

In `orders-service/tests/test_handlers.py`, add `handle_payment_refunded` to the `orders.handlers` import, and append:

```python
def payment_refunded(order_id: str = "o-1") -> dict[str, object]:
    return event(
        "PaymentRefunded", {"order_id": order_id, "payment_id": "p-1", "amount_cents": 5000}
    )


def test_a_refund_is_recorded_on_the_cancelled_order(session: Session) -> None:
    order = cancelled_order(session)

    handle_payment_refunded(session, payment_refunded())

    assert order.refunded_at is not None
    assert order.status == OrderStatus.CANCELLED
    assert order.status_reason == "cancelled by customer"


def test_a_redelivered_refund_is_handled_once(session: Session) -> None:
    cancelled_order(session)
    envelope = payment_refunded()

    handle_payment_refunded(session, envelope)
    handle_payment_refunded(session, envelope)

    assert session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1


def test_a_repeated_refund_keeps_the_first_time(session: Session) -> None:
    order = cancelled_order(session)
    handle_payment_refunded(session, payment_refunded())
    first = order.refunded_at

    handle_payment_refunded(session, payment_refunded())

    assert order.refunded_at == first
    assert session.scalar(select(func.count()).select_from(ProcessedEvent)) == 2


def test_a_refund_for_an_unknown_order_is_recorded_and_ignored(session: Session) -> None:
    envelope = payment_refunded("missing")

    handle_payment_refunded(session, envelope)

    assert session.get(Order, "missing") is None
    assert session.get(ProcessedEvent, envelope["event_id"]) is not None
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd orders-service && uv run pytest tests/test_handlers.py -v`
Expected: collection ERROR, `ImportError: cannot import name 'handle_payment_refunded'`.

- [ ] **Step 3: Add the column and migration**

In `orders-service/src/orders/models.py`, after `confirmed_at`:

```python
    refunded_at: Mapped[datetime | None]
```

`orders-service/migrations/versions/0003_add_refunded_at_to_orders.py`:

```python
"""add refunded_at to orders

Revision ID: 0003
Revises: 0002
"""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.add_column(sa.Column("refunded_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_column("refunded_at")
```

- [ ] **Step 4: Implement the handler**

In `orders-service/src/orders/handlers.py`, add `from sqlalchemy import update`, change the models import to `from orders.models import Order, OrderStatus, ProcessedEvent`, and append:

```python
def handle_payment_refunded(session: Session, envelope: dict[str, Any]) -> None:
    """Record when the money came back. The status stays as it is; the first refund time wins."""
    if session.get(ProcessedEvent, envelope["event_id"]) is not None:
        return
    session.execute(
        update(Order)
        .where(Order.id == envelope["data"]["order_id"], Order.refunded_at.is_(None))
        .values(refunded_at=datetime.now(UTC)),
        execution_options={"synchronize_session": False},
    )
    session.add(ProcessedEvent(event_id=envelope["event_id"]))
    session.commit()
```

(The commit expires every object in the session, so the next read of the order shows `refunded_at`.)

- [ ] **Step 5: Run the handler and migration tests**

Run: `cd orders-service && uv run pytest tests/test_handlers.py tests/test_migrations.py -v`
Expected: all PASS.

- [ ] **Step 6: Write the failing API test**

Append to `orders-service/tests/test_api.py`:

```python
def test_an_order_shows_when_its_refund_came_back(client: TestClient, session: Session) -> None:
    order_id = str(place(client)["id"])
    assert client.get(f"/orders/{order_id}").json()["refunded_at"] is None

    client.post(f"/orders/{order_id}/cancel")
    with Session(session.get_bind()) as worker:
        worker.execute(
            update(Order)
            .where(Order.id == order_id)
            .values(refunded_at=datetime(2026, 10, 10, 12, 0))
        )
        worker.commit()

    assert client.get(f"/orders/{order_id}").json()["refunded_at"] == "2026-10-10T12:00:00"
```

Add `from datetime import datetime` to the imports.

Run: `cd orders-service && uv run pytest tests/test_api.py::test_an_order_shows_when_its_refund_came_back -v`
Expected: FAIL with `KeyError: 'refunded_at'`.

- [ ] **Step 7: Expose `refunded_at`**

In `orders-service/src/orders/api.py`, add `refunded_at: datetime | None` as the last field of `OrderOut`, and `refunded_at=order.refunded_at,` to `to_out` after `total_cents`. `datetime` is already imported from `datetime`.

Run: `cd orders-service && uv run pytest tests/test_api.py -v`
Expected: all PASS.

- [ ] **Step 8: Write the failing worker test**

`orders-service/tests/test_worker.py`:

```python
from typing import Any

import pytest

from orders import worker


def test_the_worker_listens_for_every_event_it_handles(monkeypatch: pytest.MonkeyPatch) -> None:
    subscribed: dict[str, Any] = {}
    monkeypatch.setattr(
        worker, "run_consumer", lambda servers, group, handlers: subscribed.update(handlers)
    )

    worker.main()

    assert set(subscribed) == {
        "StockRejected",
        "PaymentFailed",
        "PaymentCaptured",
        "PaymentRefunded",
    }
```

Run: `cd orders-service && uv run pytest tests/test_worker.py -v`
Expected: FAIL, the set lacks `"PaymentRefunded"`.

- [ ] **Step 9: Subscribe the worker**

In `orders-service/src/orders/worker.py`, import `handle_payment_refunded` and add to the handlers mapping:

```python
            "PaymentRefunded": with_session(handle_payment_refunded),
```

Run: `cd orders-service && uv run pytest tests/test_worker.py -v`
Expected: PASS.

- [ ] **Step 10: Update the README**

In `orders-service/README.md`, "Events", add: "Consumes `PaymentRefunded` and records `refunded_at` on the order, which `GET /orders/{id}` shows. The status does not change."

- [ ] **Step 11: Run all checks**

Run: `cd orders-service && uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all PASS.

- [ ] **Step 12: Commit**

```bash
git -C orders-service add src/orders/models.py src/orders/api.py src/orders/handlers.py src/orders/worker.py \
  migrations/versions/0003_add_refunded_at_to_orders.py tests/test_handlers.py tests/test_api.py \
  tests/test_worker.py README.md
git -C orders-service commit -m "feat: record when a cancelled order's money came back

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: e2e — cancelling through real Kafka

**Files:**
- Modify: `e2e/test_e2e.py` (workspace repo)

**Interfaces:**
- Consumes: `POST /orders/{id}/cancel` (Task 4), `refunded_at` on `GET /orders/{id}` (Task 5), `status: "REFUNDED"` on `GET /payments/{order_id}` (Task 2), the release in inventory (Task 3), the topics from Task 1.
- Produces: nothing.

- [ ] **Step 1: Restart the shop with the new topics and code**

Run, from the workspace root:

```bash
./stop-all.sh; ./up.sh && ./run-all.sh
```

Expected: `up.sh` prints a topic list that includes `shop.orders.order-cancelled.v1` and `shop.payments.payment-refunded.v1`. `run-all.sh` applies the new migrations when it starts the services.

- [ ] **Step 2: Write the e2e tests**

In `e2e/test_e2e.py`, change the module docstring's second paragraph to:

```python
Each run uses one PEN-1 from stock; the cancel tests give theirs back.
./stop-all.sh && ./run-all.sh resets the stock.
```

and append:

```python
def cancel(order_id: str) -> httpx2.Response:
    return httpx2.post(f"{ORDERS}/orders/{order_id}/cancel")


def holds(fetch: Callable[[], Any], ok: Callable[[Any], bool], seconds: float = 3) -> None:
    """Check that ok(fetch()) stays true, so a late event cannot undo what was checked."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = fetch()
        assert ok(value), f"no longer holds: {value}"
        time.sleep(0.5)


def test_cancelling_a_confirmed_order_refunds_it_and_gives_the_stock_back() -> None:
    before = stock("PEN-1")
    order_id = place_order("PEN-1", 1)
    wait_for(lambda: order(order_id), lambda o: o["status"] == "CONFIRMED")

    response = cancel(order_id)

    assert response.status_code == 200
    assert response.json()["status"] == "CANCELLED"
    wait_for(lambda: payment(order_id), lambda p: p is not None and p["status"] == "REFUNDED")
    wait_for(lambda: order(order_id), lambda o: o["refunded_at"] is not None)
    assert wait_for(lambda: stock("PEN-1"), lambda level: level == before) == before


def test_cancelling_right_after_placing_leaves_no_stock_or_money_held() -> None:
    before = stock("PEN-1")
    order_id = place_order("PEN-1", 1)

    response = cancel(order_id)

    assert response.status_code == 200
    assert response.json()["status"] == "CANCELLED"

    def settled() -> tuple[int, dict[str, Any] | None]:
        return stock("PEN-1"), payment(order_id)

    def nothing_held(state: tuple[int, dict[str, Any] | None]) -> bool:
        level, paid = state
        return level == before and (paid is None or paid["status"] == "REFUNDED")

    wait_for(settled, nothing_held)
    holds(settled, nothing_held)
    if payment(order_id) is not None:
        wait_for(lambda: order(order_id), lambda o: o["refunded_at"] is not None)


def test_a_rejected_order_cannot_be_cancelled() -> None:
    order_id = place_order("BOOK-1", 11)
    wait_for(lambda: order(order_id), lambda o: o["status"] == "REJECTED")

    response = cancel(order_id)

    assert response.status_code == 409
```

- [ ] **Step 3: Run the e2e suite**

Run: `uv run pytest e2e -v`
Expected: all seven tests PASS. If a cancel test times out, check `.run/` for the inventory, payments and orders worker logs before changing any code.

- [ ] **Step 4: Run the workspace script tests and checks**

Run: `uv run pytest && uv run ruff check e2e && uv run ruff format --check e2e`
Expected: all PASS. (If the workspace has no ruff config, `uv run ruff` still works with defaults; skip only if `ruff` is not a workspace dependency, and say so.)

- [ ] **Step 5: Stop the shop and commit**

```bash
./stop-all.sh
git add e2e/test_e2e.py
git commit -m "test: e2e cancelling gives the stock back and refunds the payment

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
