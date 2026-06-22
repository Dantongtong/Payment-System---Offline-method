"""
PaymentService -- the heart of the "Payment Server" box.

Responsibilities (mapped to functional requirements):
  FR1 Merchant creates a payment request   -> create_payment()  [idempotent]
  FR2 Client completes the payment          -> complete_payment() [state machine + retry]
  FR4 Merchant checks payment status        -> get_payment()

Cross-cutting concerns from the design:
  - idempotency_key dedup (prevent double charging / fraud)
  - strict state machine transitions (strong consistency / correctness)
  - retry with backoff + jitter + DLQ around the bank call
  - emit every status change to Kafka (decoupled ledger + settlement writes)
"""
from __future__ import annotations

import logging
import threading
from typing import Optional

from .bank_provider import (BankProvider, CardDeclined, RETRYABLE_BANK_ERRORS)
from .db import DuplicateIdempotencyKey, PaymentDB
from .event_bus import EventBus, TOPIC_DLQ
from .models import (InvalidTransition, Payment, PaymentStatus, can_transition,
                     now_ms)
from .retry import RetriesExhausted, RetryPolicy, call_with_retry

log = logging.getLogger("payment")


class PaymentNotFound(Exception):
    pass


class PaymentService:
    def __init__(self, db: PaymentDB, bus: EventBus, bank: BankProvider,
                 retry_policy: RetryPolicy | None = None):
        self.db = db
        self.bus = bus
        self.bank = bank
        self.retry_policy = retry_policy or RetryPolicy()
        # per-payment lock so concurrent /complete calls can't race the bank.
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # -- helpers ------------------------------------------------------------ #
    def _lock_for(self, payment_id: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(payment_id, threading.Lock())

    def _transition(self, payment: Payment, dst: PaymentStatus,
                    **changes) -> Payment:
        if not can_transition(payment.status, dst):
            raise InvalidTransition(f"{payment.status.value} -> {dst.value}")
        src = payment.status
        payment.status = dst
        for k, v in changes.items():
            setattr(payment, k, v)
        payment.updated_at = now_ms()
        self.db.update(payment)
        # emit every status change to Kafka (design: "emit every status change")
        self.bus.emit(
            type=f"payment.{dst.value.lower()}",
            payload={**payment.to_dict(), "from": src.value},
        )
        log.info("payment %s: %s -> %s", payment.payment_id, src.value, dst.value)
        return payment

    # -- FR1: create a payment request (idempotent) ------------------------- #
    def create_payment(self, *, merchant_id: str, client_id: str,
                        amount_minor: int, currency: str,
                        idempotency_key: str) -> tuple[Payment, bool]:
        """Returns (payment, created). created=False means dedup hit."""
        if amount_minor <= 0:
            raise ValueError("amount_minor must be positive")
        payment = Payment(
            merchant_id=merchant_id, client_id=client_id,
            amount_minor=amount_minor, currency=currency,
            idempotency_key=idempotency_key,
        )
        try:
            self.db.insert(payment)
        except DuplicateIdempotencyKey as dup:
            # Idempotent create: return the original, do NOT create a 2nd charge.
            return dup.existing, False
        self.bus.emit(type="payment.started", payload=payment.to_dict())
        return payment, True

    # -- FR2: client completes the payment ---------------------------------- #
    def complete_payment(self, payment_id: str) -> Payment:
        payment = self.db.get(payment_id)
        if payment is None:
            raise PaymentNotFound(payment_id)

        with self._lock_for(payment_id):
            payment = self.db.get(payment_id)  # re-read under lock
            # Idempotent: completing an already-terminal payment is a no-op.
            if payment.status in (PaymentStatus.COMPLETED, PaymentStatus.FAILED):
                return payment
            if payment.status != PaymentStatus.STARTED:
                raise InvalidTransition(
                    f"cannot complete from {payment.status.value}")

            # STARTED -> PROCESSING
            self._transition(payment, PaymentStatus.PROCESSING)

            # Charge the bank provider with retry (backoff + jitter + limit).
            def _do_charge(attempt: int):
                payment.attempts = attempt
                return self.bank.charge(
                    payment_id=payment.payment_id,
                    client_id=payment.client_id,
                    amount_minor=payment.amount_minor,
                    currency=payment.currency,
                )

            try:
                result = call_with_retry(
                    _do_charge, self.retry_policy, RETRYABLE_BANK_ERRORS)
            except CardDeclined as exc:
                # business decline -> FAILED (no retry)
                return self._transition(payment, PaymentStatus.FAILED,
                                        failure_reason=f"declined: {exc}")
            except RetriesExhausted as exc:
                # transient outage exhausted retries -> DLQ + FAILED
                self.bus.emit(
                    type="payment.charge_failed",
                    payload={"payment_id": payment.payment_id,
                             "reason": str(exc.last_error),
                             "attempts": exc.attempts},
                    topic=TOPIC_DLQ,
                )
                return self._transition(payment, PaymentStatus.FAILED,
                                        failure_reason=f"provider_unavailable: {exc.last_error}")

            # PROCESSING -> COMPLETED
            return self._transition(payment, PaymentStatus.COMPLETED,
                                    bank_reference=result.bank_reference)

    # -- FR4: check status -------------------------------------------------- #
    def get_payment(self, payment_id: str) -> Payment:
        payment = self.db.get(payment_id)
        if payment is None:
            raise PaymentNotFound(payment_id)
        return payment

    def list_payments(self, merchant_id: Optional[str] = None) -> list[Payment]:
        return self.db.list(merchant_id)
