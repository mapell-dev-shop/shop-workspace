"""The whole order flow through Kafka. Needs ./up.sh and ./run-all.sh first.

Each run uses one PEN-1 from stock; the cancel tests give theirs back.
./stop-all.sh && ./run-all.sh resets the stock.
"""

import time
from collections.abc import Callable
from typing import Any

import httpx2
import pytest

ORDERS = "http://localhost:8001"
INVENTORY = "http://localhost:8002"
PAYMENTS = "http://localhost:8003"


@pytest.fixture(autouse=True, scope="module")
def system_is_running() -> None:
    try:
        httpx2.get(f"{ORDERS}/docs").raise_for_status()
    except httpx2.HTTPError:
        pytest.fail("the shop is not running: start it with ./up.sh && ./run-all.sh")


def wait_for(fetch: Callable[[], Any], done: Callable[[Any], bool], timeout: float = 30) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        value = fetch()
        if done(value):
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout} s; last value: {value}")
        time.sleep(0.5)


def place_order(sku: str, quantity: int) -> str:
    response = httpx2.post(f"{ORDERS}/orders", json={"lines": [{"sku": sku, "quantity": quantity}]})
    response.raise_for_status()
    order_id: str = response.json()["id"]
    return order_id


def payment(order_id: str) -> dict[str, Any] | None:
    response = httpx2.get(f"{PAYMENTS}/payments/{order_id}")
    return response.json() if response.status_code == 200 else None


def stock(sku: str) -> int:
    level: int = httpx2.get(f"{INVENTORY}/products/{sku}").json()["stock"]
    return level


def order(order_id: str) -> dict[str, Any]:
    body: dict[str, Any] = httpx2.get(f"{ORDERS}/orders/{order_id}").json()
    return body


def test_an_order_in_stock_gets_its_payment_captured() -> None:
    order_id = place_order("PEN-1", 1)

    captured = wait_for(lambda: payment(order_id), lambda p: p is not None)

    assert captured["status"] == "CAPTURED"


def test_an_order_beyond_the_stock_is_rejected() -> None:
    order_id = place_order("BOOK-1", 11)

    final = wait_for(lambda: order(order_id), lambda o: o["status"] != "PENDING")

    assert final["status"] == "REJECTED"
    assert final["status_reason"] == "insufficient stock: BOOK-1"


def test_an_order_over_the_payment_limit_is_rejected() -> None:
    order_id = place_order("LAPTOP-1", 1)

    final = wait_for(lambda: order(order_id), lambda o: o["status"] != "PENDING")

    assert final["status"] == "REJECTED"
    assert final["status_reason"] == "payment failed: amount over limit"


def test_a_failed_payment_gives_the_stock_back() -> None:
    before = stock("LAPTOP-1")
    order_id = place_order("LAPTOP-1", 1)

    wait_for(lambda: order(order_id), lambda o: o["status"] == "REJECTED")

    assert wait_for(lambda: stock("LAPTOP-1"), lambda level: level == before) == before


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
