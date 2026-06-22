"""
Mock relational databases (stand-ins for MySQL / PostgreSQL in the design).

  - PaymentDB  : the Payment Table. UNIQUE constraint on idempotency_key is the
                 anti-double-charge / dedup mechanism from the design.
  - LedgerDB   : append-only double-entry ledger (Ledger DB box).
  - SettlementStore : settlement batches produced by the Settlement Process.

Everything is in-memory and guarded by a re-entrant lock so the demo can be
hit concurrently the way a real 10K-QPS service would be.
"""
from __future__ import annotations

import threading
from typing import Optional

from .models import LedgerEntry, Payment, Settlement


class DuplicateIdempotencyKey(Exception):
    """Raised when a (merchant_id, idempotency_key) pair already exists."""
    def __init__(self, existing: Payment):
        self.existing = existing
        super().__init__(f"duplicate idempotency_key for payment {existing.payment_id}")


class PaymentDB:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._by_id: dict[str, Payment] = {}
        # UNIQUE INDEX (merchant_id, idempotency_key) -> payment_id
        self._idem_index: dict[tuple[str, str], str] = {}

    def insert(self, payment: Payment) -> Payment:
        """Insert respecting the unique idempotency constraint (order level)."""
        key = (payment.merchant_id, payment.idempotency_key)
        with self._lock:
            existing_id = self._idem_index.get(key)
            if existing_id is not None:
                raise DuplicateIdempotencyKey(self._by_id[existing_id])
            self._by_id[payment.payment_id] = payment
            self._idem_index[key] = payment.payment_id
            return payment

    def get(self, payment_id: str) -> Optional[Payment]:
        with self._lock:
            return self._by_id.get(payment_id)

    def update(self, payment: Payment) -> Payment:
        with self._lock:
            self._by_id[payment.payment_id] = payment
            return payment

    def list(self, merchant_id: Optional[str] = None) -> list[Payment]:
        with self._lock:
            rows = list(self._by_id.values())
        if merchant_id:
            rows = [p for p in rows if p.merchant_id == merchant_id]
        return sorted(rows, key=lambda p: p.created_at)


class LedgerDB:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: list[LedgerEntry] = []

    def append(self, entries: list[LedgerEntry]) -> None:
        with self._lock:
            self._entries.extend(entries)

    def list(self, payment_id: Optional[str] = None) -> list[LedgerEntry]:
        with self._lock:
            rows = list(self._entries)
        if payment_id:
            rows = [e for e in rows if e.payment_id == payment_id]
        return rows

    def balance(self, account: str) -> int:
        """Net minor units for an account (credits - debits)."""
        with self._lock:
            total = 0
            for e in self._entries:
                if e.account != account:
                    continue
                total += e.amount_minor if e.entry_type.value == "CREDIT" else -e.amount_minor
            return total


class SettlementStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._by_id: dict[str, Settlement] = {}

    def insert(self, settlement: Settlement) -> Settlement:
        with self._lock:
            self._by_id[settlement.settlement_id] = settlement
            return settlement

    def list(self, merchant_id: Optional[str] = None) -> list[Settlement]:
        with self._lock:
            rows = list(self._by_id.values())
        if merchant_id:
            rows = [s for s in rows if s.merchant_id == merchant_id]
        return sorted(rows, key=lambda s: s.created_at)
