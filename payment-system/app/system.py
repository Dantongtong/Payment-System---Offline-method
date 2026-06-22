"""Factory that wires the whole payment system together."""
from __future__ import annotations

from dataclasses import dataclass

from .bank_provider import BankProvider
from .db import LedgerDB, PaymentDB, SettlementStore
from .event_bus import EventBus
from .payment_service import PaymentService
from .retry import RetryPolicy
from .settlement import LedgerConsumer, SettlementProcessor, wire_consumers


@dataclass
class PaymentSystem:
    payment_db: PaymentDB
    ledger_db: LedgerDB
    settlement_store: SettlementStore
    bus: EventBus
    bank: BankProvider
    service: PaymentService
    settlement: SettlementProcessor

    def start(self) -> "PaymentSystem":
        self.bus.start()
        return self

    def stop(self) -> None:
        self.bus.stop()


def build_system(retry_policy: RetryPolicy | None = None,
                 bank_seed: int | None = 7,
                 bank_latency: tuple[float, float] = (0.005, 0.02),
                 start: bool = True) -> PaymentSystem:
    payment_db = PaymentDB()
    ledger_db = LedgerDB()
    settlement_store = SettlementStore()
    bus = EventBus()
    bank = BankProvider(seed=bank_seed,
                        min_latency=bank_latency[0], max_latency=bank_latency[1])
    service = PaymentService(payment_db, bus, bank, retry_policy)

    ledger_consumer = LedgerConsumer(ledger_db, payment_db)
    settlement = SettlementProcessor(payment_db, settlement_store)
    wire_consumers(bus, ledger_consumer, settlement)

    system = PaymentSystem(
        payment_db=payment_db, ledger_db=ledger_db,
        settlement_store=settlement_store, bus=bus, bank=bank,
        service=service, settlement=settlement,
    )
    if start:
        system.start()
    return system
