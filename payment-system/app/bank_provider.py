"""
Mock Bank Provider (the "Bank Provider" box).

Models the design's bank-side lifecycle: Authorized -> Processing -> Completed,
followed by the Settlement Process. It also injects the realistic failure modes
the design worries about ("What should server do when bank provider is
unavailable?", "Bank Provider (SLA)"):

  - transient outages  -> ProviderUnavailable  (RETRYABLE)
  - slow responses     -> ProviderTimeout      (RETRYABLE, SLA latency)
  - hard declines      -> CardDeclined          (NOT retryable)

Behaviour is driven by a small per-card scenario table so the demo is
deterministic, with an optional random mode for load-style testing.
"""
from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass


# --- exceptions -------------------------------------------------------------
class ProviderUnavailable(Exception):
    """Transient: provider down / network error. Retryable."""


class ProviderTimeout(Exception):
    """Transient: exceeded the SLA latency budget. Retryable."""


class CardDeclined(Exception):
    """Permanent business decline. NOT retryable."""


# --- result -----------------------------------------------------------------
@dataclass
class ChargeResult:
    bank_reference: str
    status: str  # "COMPLETED"


# --- scenarios --------------------------------------------------------------
# Keyed by client_id so demos are reproducible.
#   ok              -> succeeds first try
#   flaky:N         -> fails N times (unavailable) then succeeds  (tests retry)
#   timeout_then_ok -> times out once then succeeds
#   declined        -> hard decline, no retry
#   outage          -> always unavailable -> retries exhausted -> DLQ + FAILED
SCENARIOS = {
    "c_alice": "ok",
    "c_bob": "flaky:2",
    "c_carol": "timeout_then_ok",
    "c_dave": "declined",
    "c_erin": "outage",
}


class BankProvider:
    def __init__(self, sla_seconds: float = 1.0, seed: int | None = 7,
                 min_latency: float = 0.005, max_latency: float = 0.02):
        self.sla_seconds = sla_seconds
        self._rng = random.Random(seed)
        self._attempt_counter: dict[str, int] = {}
        self._min_latency = min_latency
        self._max_latency = max_latency

    def charge(self, *, payment_id: str, client_id: str, amount_minor: int,
               currency: str) -> ChargeResult:
        """Authorize -> Processing -> Completed. Raises on failure."""
        scenario = SCENARIOS.get(client_id, "ok")
        n = self._attempt_counter.get(payment_id, 0)
        self._attempt_counter[payment_id] = n + 1

        # simulate network + processing latency
        time.sleep(self._rng.uniform(self._min_latency, self._max_latency))

        if scenario == "ok":
            return self._complete(payment_id)

        if scenario.startswith("flaky:"):
            fail_times = int(scenario.split(":")[1])
            if n < fail_times:
                raise ProviderUnavailable(f"provider 503 (attempt {n + 1})")
            return self._complete(payment_id)

        if scenario == "timeout_then_ok":
            if n == 0:
                raise ProviderTimeout(f"exceeded SLA {self.sla_seconds}s")
            return self._complete(payment_id)

        if scenario == "declined":
            raise CardDeclined("insufficient_funds")

        if scenario == "outage":
            raise ProviderUnavailable("provider hard down")

        return self._complete(payment_id)

    def _complete(self, payment_id: str) -> ChargeResult:
        # Authorized -> Processing -> Completed all collapse into one mock ref.
        return ChargeResult(bank_reference=f"bank_{uuid.uuid4().hex[:12]}",
                            status="COMPLETED")


RETRYABLE_BANK_ERRORS = (ProviderUnavailable, ProviderTimeout)
