"""Mock data for the demo: merchants, clients, and sample payment requests.

Client IDs intentionally match the BankProvider SCENARIOS so the demo exercises
every path: success, retry-then-success, timeout, decline, and total outage.
"""
from __future__ import annotations

MERCHANTS = [
    {"merchant_id": "m_coffee", "name": "Blue Bottle Coffee"},
    {"merchant_id": "m_books", "name": "City Lights Books"},
]

CLIENTS = [
    {"client_id": "c_alice", "name": "Alice",  "scenario": "ok"},
    {"client_id": "c_bob",   "name": "Bob",    "scenario": "flaky:2 -> retry then ok"},
    {"client_id": "c_carol", "name": "Carol",  "scenario": "timeout once -> ok"},
    {"client_id": "c_dave",  "name": "Dave",   "scenario": "card declined"},
    {"client_id": "c_erin",  "name": "Erin",   "scenario": "provider outage -> DLQ"},
]

# (merchant_id, client_id, amount_minor, currency, idempotency_key)
SAMPLE_PAYMENTS = [
    ("m_coffee", "c_alice", 1299, "USD", "order_1001"),
    ("m_coffee", "c_bob",   2450, "USD", "order_1002"),
    ("m_books",  "c_carol", 3599, "USD", "order_1003"),
    ("m_books",  "c_dave",  1899, "USD", "order_1004"),
    ("m_coffee", "c_erin",  999,  "USD", "order_1005"),
    ("m_coffee", "c_alice", 4200, "USD", "order_1006"),
]
