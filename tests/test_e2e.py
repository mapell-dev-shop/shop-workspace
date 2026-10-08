"""The whole order flow through Kafka. Needs ./up.sh and ./run-all.sh first.

run-all.sh resets the demo stock, so run it again before repeating these tests many times.
"""

import time
from collections.abc import Callable
from typing import Any

import httpx2
import pytest

ORDERS = "http://localhost:8001"
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
