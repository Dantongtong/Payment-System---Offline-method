"""
Kafka-like in-process event bus (the "Kafka" box in the design).

Why it exists (straight from the design notes):
  - "decouple the payment server call with the payment DB write"
  - "emit every event change, every status change will be logged in the kafka"
  - failed messages land in a DLQ (dead letter queue)

Each subscriber gets its OWN queue + worker thread, mimicking independent Kafka
consumer groups: a slow/broken consumer can't block the others, and its failures
are isolated to the DLQ.

`flush()` blocks until every published event has been fully processed, which
keeps the demo and tests deterministic despite the async workers.
"""
from __future__ import annotations

import threading
import time
import traceback
from dataclasses import dataclass, field
from queue import Queue
from typing import Callable

# Topics
TOPIC_PAYMENT_EVENTS = "payment.events"   # every status change is emitted here
TOPIC_DLQ = "payment.dlq"                 # dead letter queue


@dataclass
class Event:
    topic: str
    type: str                  # e.g. "payment.completed"
    payload: dict
    ts: int = field(default_factory=lambda: int(time.time() * 1000))
    attempts: int = 0


Handler = Callable[[Event], None]


class _Subscription:
    def __init__(self, name: str, handler: Handler):
        self.name = name
        self.handler = handler
        self.queue: "Queue[Event | None]" = Queue()
        self.thread: threading.Thread | None = None


class EventBus:
    def __init__(self, max_handler_retries: int = 2):
        self._subs: dict[str, list[_Subscription]] = {}
        self._max_handler_retries = max_handler_retries
        self._running = False

        # in-flight accounting so flush() can block until fully drained
        self._pending = 0
        self._cond = threading.Condition()

        # observability: keep the tail of the event log + the DLQ
        self.event_log: list[Event] = []
        self.dlq: list[Event] = []
        self._log_lock = threading.Lock()

        # live listeners (e.g. the SSE dashboard). Each gets its own queue.
        self._listeners: list["Queue[Event]"] = []
        self._listeners_lock = threading.Lock()

    # -- live streaming ----------------------------------------------------- #
    def add_listener(self) -> "Queue[Event]":
        q: "Queue[Event]" = Queue(maxsize=1000)
        with self._listeners_lock:
            self._listeners.append(q)
        return q

    def remove_listener(self, q: "Queue[Event]") -> None:
        with self._listeners_lock:
            if q in self._listeners:
                self._listeners.remove(q)

    def _fanout_to_listeners(self, event: Event) -> None:
        with self._listeners_lock:
            listeners = list(self._listeners)
        for q in listeners:
            try:
                q.put_nowait(event)
            except Exception:
                pass  # slow/full listener: drop rather than block producers

    # -- wiring ------------------------------------------------------------- #
    def subscribe(self, topic: str, name: str, handler: Handler) -> None:
        self._subs.setdefault(topic, []).append(_Subscription(name, handler))

    def start(self) -> None:
        self._running = True
        for subs in self._subs.values():
            for sub in subs:
                t = threading.Thread(target=self._run_consumer, args=(sub,),
                                     name=f"consumer:{sub.name}", daemon=True)
                sub.thread = t
                t.start()

    def stop(self) -> None:
        self._running = False
        for subs in self._subs.values():
            for sub in subs:
                sub.queue.put(None)  # poison pill

    # -- producer ----------------------------------------------------------- #
    def publish(self, event: Event) -> None:
        with self._log_lock:
            self.event_log.append(event)
            # Messages produced directly onto the DLQ topic are dead letters too.
            if event.topic == TOPIC_DLQ:
                self.dlq.append(event)
        self._fanout_to_listeners(event)
        targets = self._subs.get(event.topic, [])
        with self._cond:
            self._pending += len(targets)
        for sub in targets:
            sub.queue.put(event)

    def emit(self, type: str, payload: dict, topic: str = TOPIC_PAYMENT_EVENTS) -> None:
        self.publish(Event(topic=topic, type=type, payload=payload))

    # -- consumer loop ------------------------------------------------------ #
    def _run_consumer(self, sub: _Subscription) -> None:
        while self._running:
            event = sub.queue.get()
            if event is None:  # poison pill
                break
            try:
                self._deliver(sub, event)
            finally:
                with self._cond:
                    self._pending -= 1
                    self._cond.notify_all()

    def _deliver(self, sub: _Subscription, event: Event) -> None:
        try:
            sub.handler(event)
        except Exception as exc:  # consumer failed -> route to DLQ
            dead = Event(
                topic=TOPIC_DLQ,
                type=f"dlq:{event.type}",
                payload={
                    "original": event.payload,
                    "consumer": sub.name,
                    "error": f"{type(exc).__name__}: {exc}",
                    "trace": traceback.format_exc(limit=2),
                },
            )
            with self._log_lock:
                self.dlq.append(dead)
                self.event_log.append(dead)

    # -- helpers ------------------------------------------------------------ #
    def flush(self, timeout: float = 5.0) -> None:
        """Block until all in-flight events have been processed."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._pending > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("event bus flush timed out")
                self._cond.wait(timeout=remaining)
