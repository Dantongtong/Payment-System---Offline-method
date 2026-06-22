"""Fee model + double-entry booking for the Ledger DB."""
from __future__ import annotations

from .models import LedgerEntry, LedgerEntryType, Payment


# Typical card-processing fee: 2.9% + 30 (minor units). Tunable.
FEE_RATE = 0.029
FEE_FIXED_MINOR = 30


def compute_fee(amount_minor: int) -> int:
    return round(amount_minor * FEE_RATE) + FEE_FIXED_MINOR


def book_double_entry(payment: Payment) -> list[LedgerEntry]:
    """Produce a balanced set of ledger legs for a completed payment.

    client pays `amount`; merchant receives `amount - fee`; platform keeps `fee`.
    Sum(debits) == Sum(credits) == amount.
    """
    amount = payment.amount_minor
    fee = compute_fee(amount)
    net = amount - fee
    cur = payment.currency
    return [
        LedgerEntry(payment.payment_id, f"client:{payment.client_id}",
                    LedgerEntryType.DEBIT, amount, cur),
        LedgerEntry(payment.payment_id, f"merchant:{payment.merchant_id}",
                    LedgerEntryType.CREDIT, net, cur),
        LedgerEntry(payment.payment_id, "platform:fees",
                    LedgerEntryType.CREDIT, fee, cur),
    ]
