"""Tests for the payment system (service layer + HTTP API)."""
from __future__ import annotations

from fastapi.testclient import TestClient

from app.models import PaymentStatus
from app.retry import RetryPolicy
from app.system import build_system


def _system():
    policy = RetryPolicy(retry_limit=4, base_delay=0.001, max_delay=0.01,
                         retry_timeout=1.0)
    return build_system(retry_policy=policy)


# --- service-layer tests ---------------------------------------------------
def test_happy_path_completes():
    s = _system()
    p, created = s.service.create_payment(
        merchant_id="m1", client_id="c_alice", amount_minor=1000,
        currency="USD", idempotency_key="k1")
    assert created
    assert p.status == PaymentStatus.STARTED
    final = s.service.complete_payment(p.payment_id)
    assert final.status == PaymentStatus.COMPLETED
    assert final.bank_reference
    s.stop()


def test_idempotent_create_dedups():
    s = _system()
    p1, c1 = s.service.create_payment(merchant_id="m1", client_id="c_alice",
                                      amount_minor=1000, currency="USD",
                                      idempotency_key="dup")
    p2, c2 = s.service.create_payment(merchant_id="m1", client_id="c_alice",
                                      amount_minor=1000, currency="USD",
                                      idempotency_key="dup")
    assert c1 and not c2
    assert p1.payment_id == p2.payment_id
    s.stop()


def test_retry_then_success():
    s = _system()
    p, _ = s.service.create_payment(merchant_id="m1", client_id="c_bob",
                                    amount_minor=500, currency="USD",
                                    idempotency_key="k2")
    final = s.service.complete_payment(p.payment_id)
    assert final.status == PaymentStatus.COMPLETED
    assert final.attempts == 3  # 2 failures then success
    s.stop()


def test_decline_is_not_retried():
    s = _system()
    p, _ = s.service.create_payment(merchant_id="m1", client_id="c_dave",
                                    amount_minor=500, currency="USD",
                                    idempotency_key="k3")
    final = s.service.complete_payment(p.payment_id)
    assert final.status == PaymentStatus.FAILED
    assert final.attempts == 1  # no retry on a business decline
    assert "declined" in final.failure_reason
    s.stop()


def test_outage_exhausts_retries_and_dlqs():
    s = _system()
    p, _ = s.service.create_payment(merchant_id="m1", client_id="c_erin",
                                    amount_minor=500, currency="USD",
                                    idempotency_key="k4")
    final = s.service.complete_payment(p.payment_id)
    assert final.status == PaymentStatus.FAILED
    assert len(s.bus.dlq) == 1
    s.stop()


def test_complete_is_idempotent():
    s = _system()
    p, _ = s.service.create_payment(merchant_id="m1", client_id="c_alice",
                                    amount_minor=1000, currency="USD",
                                    idempotency_key="k5")
    a = s.service.complete_payment(p.payment_id)
    b = s.service.complete_payment(p.payment_id)  # no double charge
    assert a.status == b.status == PaymentStatus.COMPLETED
    assert a.bank_reference == b.bank_reference
    s.stop()


def test_ledger_balances_and_settlement():
    s = _system()
    p, _ = s.service.create_payment(merchant_id="m1", client_id="c_alice",
                                    amount_minor=1000, currency="USD",
                                    idempotency_key="k6")
    s.service.complete_payment(p.payment_id)
    s.bus.flush()
    entries = s.ledger_db.list(p.payment_id)
    debit = sum(e.amount_minor for e in entries if e.entry_type.value == "DEBIT")
    credit = sum(e.amount_minor for e in entries if e.entry_type.value == "CREDIT")
    assert debit == credit == 1000
    batch = s.settlement.run("m1")
    assert batch is not None and batch.gross_minor == 1000
    s.stop()


# --- HTTP API tests --------------------------------------------------------
def test_http_api_flow():
    from app import main
    with TestClient(main.app) as client:  # triggers lifespan + seeding
        r = client.post("/v1/payments", json={
            "merchant_id": "m_coffee", "client_id": "c_alice",
            "amount_minor": 1500, "currency": "USD",
            "idempotency_key": "http_order_1"})
        assert r.status_code == 201
        pid = r.json()["payment"]["payment_id"]

        r2 = client.post(f"/v1/payments/{pid}/complete")
        assert r2.json()["payment"]["status"] == "COMPLETED"

        r3 = client.get(f"/v1/payments/{pid}")
        assert r3.json()["payment"]["status"] == "COMPLETED"

        # idempotent re-create
        r4 = client.post("/v1/payments", json={
            "merchant_id": "m_coffee", "client_id": "c_alice",
            "amount_minor": 1500, "currency": "USD",
            "idempotency_key": "http_order_1"})
        assert r4.json()["deduped"] is True
