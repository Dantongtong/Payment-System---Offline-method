"""
End-to-end demo of the payment system, with mock data.

Run:  python demo.py

Walks the full flow for each functional requirement and exercises every failure
path (success, retry-then-success, timeout, decline, outage->DLQ), then runs the
settlement process and prints the ledger.
"""
from __future__ import annotations

from app.mock_data import CLIENTS, SAMPLE_PAYMENTS
from app.models import PaymentStatus
from app.retry import RetryPolicy
from app.system import build_system

LINE = "-" * 72


def hr(title: str) -> None:
    print(f"\n{LINE}\n{title}\n{LINE}")


def money(minor: int, cur: str = "USD") -> str:
    return f"{minor / 100:.2f} {cur}"


def main() -> None:
    # fast retries so the demo is snappy
    policy = RetryPolicy(retry_limit=4, base_delay=0.02, max_delay=0.2,
                         retry_timeout=2.0)
    system = build_system(retry_policy=policy)
    svc = system.service

    scenario_by_client = {c["client_id"]: c["scenario"] for c in CLIENTS}

    hr("FR1 + FR2 + FR4  create -> complete -> status   (per client scenario)")
    payment_ids: list[str] = []
    for merchant_id, client_id, amount, currency, idem in SAMPLE_PAYMENTS:
        # FR1: merchant creates a payment request (idempotent on idem key)
        payment, created = svc.create_payment(
            merchant_id=merchant_id, client_id=client_id,
            amount_minor=amount, currency=currency, idempotency_key=idem)
        tag = "created" if created else "DEDUP (idempotent)"
        payment_ids.append(payment.payment_id)

        # FR2: client completes the payment (state machine + bank retry)
        final = svc.complete_payment(payment.payment_id)

        scenario = scenario_by_client.get(client_id, "ok")
        print(f"{payment.payment_id}  {money(amount, currency)}  "
              f"client={client_id:8} [{scenario:24}] "
              f"{tag:18} -> {final.status.value:10} "
              f"attempts={final.attempts}"
              + (f"  reason={final.failure_reason}" if final.failure_reason else ""))

    hr("Idempotency  (same idempotency_key must NOT create a second charge)")
    p1, c1 = svc.create_payment(merchant_id="m_coffee", client_id="c_alice",
                                amount_minor=1299, currency="USD",
                                idempotency_key="order_1001")
    print(f"re-submit order_1001 -> created={c1}  "
          f"same payment_id as original? {p1.payment_id == payment_ids[0]}")

    hr("State machine guard  (illegal transition is rejected)")
    completed = next(svc.get_payment(pid) for pid in payment_ids
                     if svc.get_payment(pid).status == PaymentStatus.COMPLETED)
    try:
        svc.complete_payment(completed.payment_id)
        print(f"completing an already-COMPLETED payment -> no-op (idempotent), "
              f"status still {completed.status.value}")
    except Exception as e:  # pragma: no cover
        print("unexpected:", e)

    # let async Kafka consumers (ledger + settlement) finish
    system.bus.flush()

    hr("FR3  Settlement Process  (net completed payments into payout batches)")
    batches = system.settlement.run_all()
    for b in batches:
        print(f"settlement {b.settlement_id}  merchant={b.merchant_id}  "
              f"payments={len(b.payment_ids)}  gross={money(b.gross_minor)}  "
              f"fee={money(b.fee_minor)}  NET={money(b.net_minor)}")

    hr("Ledger DB  (double-entry; debits == credits)")
    entries = system.ledger_db.list()
    debit = sum(e.amount_minor for e in entries if e.entry_type.value == "DEBIT")
    credit = sum(e.amount_minor for e in entries if e.entry_type.value == "CREDIT")
    print(f"{len(entries)} entries   total debit={money(debit)}   "
          f"total credit={money(credit)}   balanced={debit == credit}")
    print(f"platform fee revenue: {money(system.ledger_db.balance('platform:fees'))}")

    hr("Kafka  (every status change emitted)  +  DLQ")
    counts: dict[str, int] = {}
    for e in system.bus.event_log:
        counts[e.type] = counts.get(e.type, 0) + 1
    for t in sorted(counts):
        print(f"  {t:28} x{counts[t]}")
    print(f"DLQ messages: {len(system.bus.dlq)}")
    for d in system.bus.dlq:
        print(f"  DLQ <- {d.type}: {d.payload.get('reason') or d.payload.get('error')}")

    system.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
