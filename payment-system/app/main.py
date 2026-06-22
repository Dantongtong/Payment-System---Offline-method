"""
FastAPI app -- "API Gateway / LB" + "Payment Server" entry point,
now also serving a real-time ops dashboard at "/".

API endpoints (mapped to functional requirements):
  POST /v1/payments                      FR1 merchant creates a payment request
  POST /v1/payments/{id}/complete        FR2 client completes the payment
  GET  /v1/payments/{id}                 FR4 merchant checks status
  GET  /v1/payments?merchant_id=...          list a merchant's payments
  POST /v1/settlements/run               FR3 run the settlement process
  GET  /v1/settlements?merchant_id=...       list settlement batches
  GET  /v1/ledger?payment_id=...             inspect the ledger
  GET  /v1/events                            tail the Kafka event log + DLQ
  GET  /v1/snapshot                          full current state (dashboard boot)
  GET  /v1/stream                            Server-Sent Events live feed
  POST /v1/reset                             rebuild + reseed mock data
  GET  /                                      the dashboard

All data is in-memory mock data, seeded on startup.
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from .ledger import compute_fee
from .models import InvalidTransition
from .mock_data import CLIENTS, MERCHANTS, SAMPLE_PAYMENTS
from .payment_service import PaymentNotFound
from .retry import RetryPolicy
from .system import build_system

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

STATIC_DIR = Path(__file__).parent / "static"
SYSTEM = None  # set on startup / reset

# Slightly higher bank latency + visible retry backoff so state transitions are
# watchable on the dashboard. Tune freely.
_RETRY_POLICY = RetryPolicy(retry_limit=4, base_delay=0.2, max_delay=1.2,
                            retry_timeout=8.0)
_BANK_LATENCY = (0.25, 0.6)


def _new_system():
    sys = build_system(retry_policy=_RETRY_POLICY, bank_seed=None,
                       bank_latency=_BANK_LATENCY)
    for merchant_id, client_id, amount, currency, idem in SAMPLE_PAYMENTS:
        sys.service.create_payment(
            merchant_id=merchant_id, client_id=client_id,
            amount_minor=amount, currency=currency, idempotency_key=idem)
    return sys


@asynccontextmanager
async def lifespan(app: FastAPI):
    global SYSTEM
    SYSTEM = _new_system()
    yield
    SYSTEM.stop()


app = FastAPI(title="Payment System (mock)", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
class CreatePaymentRequest(BaseModel):
    merchant_id: str
    client_id: str
    amount_minor: int = Field(gt=0, description="amount in minor units (cents)")
    currency: str = "USD"
    idempotency_key: str


# --------------------------------------------------------------------------- #
# Snapshot helper (full current state for dashboard boot / reconnect)
# --------------------------------------------------------------------------- #
def _snapshot() -> dict:
    payments = [p.to_dict() for p in SYSTEM.service.list_payments()]
    ledger = SYSTEM.ledger_db.list()
    debit = sum(e.amount_minor for e in ledger if e.entry_type.value == "DEBIT")
    credit = sum(e.amount_minor for e in ledger if e.entry_type.value == "CREDIT")
    settlements = [s.to_dict() for s in SYSTEM.settlement_store.list()]
    events = [{"type": e.type, "topic": e.topic, "payload": e.payload, "ts": e.ts}
              for e in SYSTEM.bus.event_log[-80:]]
    dlq = [{"type": e.type, "payload": e.payload, "ts": e.ts}
           for e in SYSTEM.bus.dlq]
    return {
        "payments": payments,
        "events": events,
        "dlq": dlq,
        "settlements": settlements,
        "ledger": {"debit_minor": debit, "credit_minor": credit,
                   "balanced": debit == credit,
                   "fee_minor": SYSTEM.ledger_db.balance("platform:fees")},
        "reference": {"merchants": MERCHANTS, "clients": CLIENTS},
    }


# --------------------------------------------------------------------------- #
# Dashboard + streaming
# --------------------------------------------------------------------------- #
@app.get("/")
def dashboard():
    return FileResponse(STATIC_DIR / "dashboard.html")


@app.get("/v1/snapshot")
def snapshot():
    return _snapshot()


@app.get("/v1/stream")
async def stream(request: Request):
    """Server-Sent Events: pushes every bus event to the dashboard live."""
    bus = SYSTEM.bus
    listener = bus.add_listener()

    async def gen():
        # 1) initial snapshot so a fresh/reconnecting client is fully in sync
        yield f"event: snapshot\ndata: {json.dumps(_snapshot())}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    ev = await asyncio.to_thread(listener.get, True, 1.0)
                    data = json.dumps({"type": ev.type, "topic": ev.topic,
                                       "payload": ev.payload, "ts": ev.ts})
                    yield f"event: bus\ndata: {data}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"  # comment line keeps connection warm
        finally:
            bus.remove_listener(listener)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


@app.post("/v1/reset")
def reset():
    global SYSTEM
    old = SYSTEM
    SYSTEM = _new_system()
    old.stop()
    return {"status": "reset"}


# --------------------------------------------------------------------------- #
# Core API
# --------------------------------------------------------------------------- #
@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/v1/reference")
def reference():
    return {"merchants": MERCHANTS, "clients": CLIENTS}


@app.post("/v1/payments", status_code=201)
def create_payment(req: CreatePaymentRequest):
    payment, created = SYSTEM.service.create_payment(
        merchant_id=req.merchant_id, client_id=req.client_id,
        amount_minor=req.amount_minor, currency=req.currency,
        idempotency_key=req.idempotency_key)
    return {"created": created, "deduped": not created, "payment": payment.to_dict()}


@app.post("/v1/payments/{payment_id}/complete")
def complete_payment(payment_id: str):
    try:
        payment = SYSTEM.service.complete_payment(payment_id)
    except PaymentNotFound:
        raise HTTPException(404, "payment not found")
    except InvalidTransition as e:
        raise HTTPException(409, f"invalid transition: {e}")
    return {"payment": payment.to_dict()}


@app.get("/v1/payments/{payment_id}")
def get_payment(payment_id: str):
    try:
        return {"payment": SYSTEM.service.get_payment(payment_id).to_dict()}
    except PaymentNotFound:
        raise HTTPException(404, "payment not found")


@app.get("/v1/payments")
def list_payments(merchant_id: str | None = Query(None)):
    rows = SYSTEM.service.list_payments(merchant_id)
    return {"count": len(rows), "payments": [p.to_dict() for p in rows]}


@app.post("/v1/settlements/run")
def run_settlement(merchant_id: str | None = Query(None)):
    if merchant_id:
        s = SYSTEM.settlement.run(merchant_id)
        batches = [s] if s else []
    else:
        batches = SYSTEM.settlement.run_all()
    return {"count": len(batches), "settlements": [b.to_dict() for b in batches]}


@app.get("/v1/settlements")
def list_settlements(merchant_id: str | None = Query(None)):
    rows = SYSTEM.settlement_store.list(merchant_id)
    return {"count": len(rows), "settlements": [s.to_dict() for s in rows]}


@app.get("/v1/ledger")
def get_ledger(payment_id: str | None = Query(None)):
    rows = SYSTEM.ledger_db.list(payment_id)
    return {"count": len(rows), "entries": [e.to_dict() for e in rows]}


@app.get("/v1/events")
def get_events(limit: int = 50):
    log = SYSTEM.bus.event_log[-limit:]
    return {
        "event_log": [{"type": e.type, "topic": e.topic, "payload": e.payload}
                      for e in log],
        "dlq": [{"type": e.type, "payload": e.payload} for e in SYSTEM.bus.dlq],
    }
