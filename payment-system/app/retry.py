"""
Retry with exponential backoff + jitter, a retry limit AND a retry timeout.

Implements the design note:
  "Payment server configured with retry:
   Retry limit + retry timeout + exponential backoff + jitter + DLQ"

Only *retryable* errors are retried (transient: timeout / provider unavailable).
Business declines are NOT retried -- retrying a declined card just wastes time
and risks duplicate side effects.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, Type


@dataclass
class RetryPolicy:
    retry_limit: int = 4          # max attempts beyond the first
    base_delay: float = 0.05      # seconds
    max_delay: float = 1.0        # cap per-attempt backoff
    retry_timeout: float = 3.0    # total wall-clock budget across attempts


class RetriesExhausted(Exception):
    def __init__(self, attempts: int, last_error: Exception):
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"retries exhausted after {attempts} attempts: {last_error}")


def call_with_retry(
    fn: Callable[[int], "T"],
    policy: RetryPolicy,
    retryable: tuple[Type[Exception], ...],
    sleep: Callable[[float], None] = time.sleep,
) -> "T":
    """Call fn(attempt_number). Retry transient failures per the policy."""
    start = time.monotonic()
    attempt = 0
    last_error: Exception | None = None

    while True:
        attempt += 1
        try:
            return fn(attempt)
        except retryable as exc:
            last_error = exc
            elapsed = time.monotonic() - start
            # Stop if we've hit the attempt cap or blown the time budget.
            if attempt > policy.retry_limit or elapsed >= policy.retry_timeout:
                raise RetriesExhausted(attempt, exc) from exc
            # exponential backoff
            backoff = min(policy.max_delay, policy.base_delay * (2 ** (attempt - 1)))
            # full jitter: random between 0 and backoff (AWS-recommended)
            delay = random.uniform(0, backoff)
            # don't sleep past the remaining time budget
            delay = min(delay, max(0.0, policy.retry_timeout - elapsed))
            sleep(delay)
