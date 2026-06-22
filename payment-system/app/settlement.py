"""
Downstream Kafka consumers (decoupled from the Payment Server's hot path).

  LedgerConsumer  : on payment.completed -> write double-entry to the Ledger DB.
  SettlementProcessor : accumulates completed payments per merchant and, when
                        run, nets them into a Settlement batch ("Settlement
                        Process" in the design). FR3: "Settlement is needed".

These run as bus subscribers, so the Payment Server never blocks on ledger /
settlement writes -- this is the "decouple the payment server call with the
payment DB write (write-heavy, large batch)" idea from the design.
"""
from __future__ import annotations

import logging
import threading

from .db import LedgerDB, PaymentDB, SettlementStore
from .event_bus import Event, EventBus, TOPIC_PAYMENT_EVENTS
from .ledger import book_double_entry, compute_fee
from .models import Settlement

log = logging.getLogger("settlement")


class LedgerConsumer:
    def __init__(self, ledger_db: LedgerDB, payment_db: PaymentDB):
        self.ledger_db = ledger_db
        self.payment_db = payment_db

    def handle(self, event: Event) -> None:
        if event.type != "payment.completed":
            return
        payment = self.payment_db.get(event.payload["payment_id"])
        if payment is None:
            raise RuntimeError("ledger: payment missing")  # -> DLQ
        entries = book_double_entry(payment)
        # sanity: double entry must balance
        debit = sum(e.amount_minor for e in entries if e.entry_type.value == "DEBIT")
        credit = sum(e.amount_minor for e in entries if e.entry_type.value == "CREDIT")
        assert debit == credit, "ledger not balanced"
        self.ledger_db.append(entries)
        log.info("ledger booked for %s", payment.payment_id)


class SettlementProcessor:
    """Collects completed payments and nets them into payout batches."""
    def __init__(self, payment_db: PaymentDB, store: SettlementStore):
        self.payment_db = payment_db
        self.store = store
        self._lock = threading.Lock()
        # merchant_id -> list[payment_id] awaiting settlement
        self._pending: dict[str, list[str]] = {}

    def handle(self, event: Event) -> None:
        if event.type != "payment.completed":
            return
        with self._lock:
            self._pending.setdefault(event.payload["merchant_id"], []).append(
                event.payload["payment_id"])

    def run(self, merchant_id: str) -> Settlement | None:
        """Net all pending completed payments for a merchant into one batch."""
        with self._lock:
            payment_ids = self._pending.pop(merchant_id, [])
        if not payment_ids:
            return None

        gross = fee = net = 0
        currency = "USD"
        settled_ids: list[str] = []
        for pid in payment_ids:
            p = self.payment_db.get(pid)
            if p is None or p.settlement_id is not None:
                continue
            currency = p.currency
            f = compute_fee(p.amount_minor)
            gross += p.amount_minor
            fee += f
            net += p.amount_minor - f
            settled_ids.append(pid)

        if not settled_ids:
            return None

        settlement = Settlement(
            merchant_id=merchant_id, currency=currency,
            gross_minor=gross, fee_minor=fee, net_minor=net,
            payment_ids=settled_ids,
        )
        self.store.insert(settlement)
        # mark payments as settled
        for pid in settled_ids:
            p = self.payment_db.get(pid)
            p.settlement_id = settlement.settlement_id
            self.payment_db.update(p)
        log.info("settled %d payments for %s: net %d",
                 len(settled_ids), merchant_id, net)
        return settlement

    def run_all(self) -> list[Settlement]:
        with self._lock:
            merchants = list(self._pending.keys())
        out = []
        for m in merchants:
            s = self.run(m)
            if s:
                out.append(s)
        return out


def wire_consumers(bus: EventBus, ledger: LedgerConsumer,
                   settlement: SettlementProcessor) -> None:
    """Subscribe both consumers to the payment events topic (consumer groups)."""
    bus.subscribe(TOPIC_PAYMENT_EVENTS, "ledger", ledger.handle)
    bus.subscribe(TOPIC_PAYMENT_EVENTS, "settlement", settlement.handle)
