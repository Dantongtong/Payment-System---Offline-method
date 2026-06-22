"""
Domain models for the payment system.

Maps directly to the whiteboard design:
  - Payment status state machine:  STARTED -> PROCESSING -> COMPLETED / FAILED
  - Payment Table:  payment_id, merchant_id, client_id, create_at, status, idempotency_key
  - Ledger entries (double-entry) for the Ledger DB
  - Settlement batches for the "Settlement Process"

Money is stored as integer minor units (e.g. cents) to avoid floating point
errors -- a hard requirement for financial correctness.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def now_ms() -> int:
    return int(time.time() * 1000)


# --------------------------------------------------------------------------- #
# Payment status state machine (Started -> Processing -> Completed / Failed)
# --------------------------------------------------------------------------- #
class PaymentStatus(str, Enum):
    STARTED = "STARTED"        # request created, waiting for the client to pay
    PROCESSING = "PROCESSING"  # client initiated, charging the bank provider
    COMPLETED = "COMPLETED"    # bank authorized + completed
    FAILED = "FAILED"          # declined, or retries exhausted (DLQ)


# Only these transitions are legal. Anything else is rejected by the service
# layer -- this is what guarantees "strong consistency and correctness".
ALLOWED_TRANSITIONS: dict[PaymentStatus, set[PaymentStatus]] = {
    PaymentStatus.STARTED: {PaymentStatus.PROCESSING, PaymentStatus.FAILED},
    PaymentStatus.PROCESSING: {PaymentStatus.COMPLETED, PaymentStatus.FAILED},
    PaymentStatus.COMPLETED: set(),  # terminal
    PaymentStatus.FAILED: set(),     # terminal
}


def can_transition(src: PaymentStatus, dst: PaymentStatus) -> bool:
    return dst in ALLOWED_TRANSITIONS[src]


class InvalidTransition(Exception):
    pass


# --------------------------------------------------------------------------- #
# Records (rows) stored in the mock relational DBs
# --------------------------------------------------------------------------- #
@dataclass
class Payment:
    merchant_id: str
    client_id: str
    amount_minor: int
    currency: str
    idempotency_key: str
    payment_id: str = field(default_factory=lambda: _new_id("pay"))
    status: PaymentStatus = PaymentStatus.STARTED
    bank_reference: Optional[str] = None
    failure_reason: Optional[str] = None
    attempts: int = 0
    settlement_id: Optional[str] = None
    created_at: int = field(default_factory=now_ms)
    updated_at: int = field(default_factory=now_ms)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        d["amount_display"] = f"{self.amount_minor / 100:.2f} {self.currency}"
        return d


class LedgerEntryType(str, Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


@dataclass
class LedgerEntry:
    """One leg of a double-entry booking. Every completed payment produces a
    balanced set of entries (sum of debits == sum of credits)."""
    payment_id: str
    account: str               # e.g. "client:c_1", "merchant:m_1", "platform:fees"
    entry_type: LedgerEntryType
    amount_minor: int
    currency: str
    entry_id: str = field(default_factory=lambda: _new_id("led"))
    created_at: int = field(default_factory=now_ms)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["entry_type"] = self.entry_type.value
        return d


@dataclass
class Settlement:
    """A settlement batch: nets a merchant's completed payments and pays out."""
    merchant_id: str
    currency: str
    gross_minor: int
    fee_minor: int
    net_minor: int
    payment_ids: list[str]
    settlement_id: str = field(default_factory=lambda: _new_id("set"))
    created_at: int = field(default_factory=now_ms)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["net_display"] = f"{self.net_minor / 100:.2f} {self.currency}"
        return d
