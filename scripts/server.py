#!/usr/bin/env python
"""Tweets REST API server for the companion app, autonomous pipeline, and subscription engine."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import dotenv
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Ensure scripts dir is on sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPTS_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

# Load .env variables
dotenv.load_dotenv(ROOT_DIR / ".env")

import database
import due
import post
import razorpay_gateway
import research
import settings
import subscription

# Initialize database tables and schema
database.init_db()

app = FastAPI(title="Tweets Companion API", version="2.0.0")

# Enable CORS for Capacitor mobile apps and web frontends
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SHOTS_DIR = ROOT_DIR / "shots"
DRAFTS_DIR = ROOT_DIR / "drafts"
SWIPE_DIR = ROOT_DIR / "swipe"
WWW_DIR = ROOT_DIR / "mobile" / "www"

class DraftFillRequest(BaseModel):
    slot: str
    text: str

class PostRequest(BaseModel):
    slot: Optional[str] = None
    text: Optional[str] = None
    kind: str = "value"
    dry_run: bool = False

class ResearchRequest(BaseModel):
    hours: int = 36
    skip_x: bool = True

class TrialStartRequest(BaseModel):
    identifier: Optional[str] = None

class OrderCreateRequest(BaseModel):
    plan_id: str

class PaymentVerifyRequest(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str

def check_active_subscription(handle: str) -> Dict[str, Any]:
    """Verify user has an active 7-day trial or paid subscription."""
    sub = database.get_subscription(handle)
    if not sub.get("is_active"):
        raise HTTPException(
            status_code=403,
            detail="Subscription required: Your access has expired or is inactive. Please start your 7-day free trial or select a subscription plan to continue."
        )
    return sub

@app.get("/api/status")
async def get_status():
    st = post.load_state()
    cfg = settings.load()
    handle = cfg.get("handle") or "ezeslucky"
    sub = database.get_subscription(handle)
    plan_meta = subscription.get_plan(sub.get("plan_id") or "")
    
    return {
        "handle": handle,
        "max_chars": int(cfg.get("max_chars", 280)),
        "ai_update_day": st.get("ai_update_day", 0),
        "next_day": post.next_day(st),
        "total_posts": len(st.get("posts", [])),
        "last_post": st.get("posts", [])[-1] if st.get("posts") else None,
        "slots": cfg.get("slots", []),
        "ai_update_slot": cfg.get("ai_update_slot", "15:00"),
        "timezone": cfg.get("timezone", "local"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        # Subscription status
        "subscription": {
            "status": sub.get("status"),
            "plan_id": sub.get("plan_id"),
            "plan_name": plan_meta["name"] if plan_meta else "None",
            "is_active": sub.get("is_active", False),
            "remaining_trial_days": sub.get("remaining_trial_days", 0),
            "remaining_days": sub.get("remaining_days", 0),
            "is_lifetime": sub.get("is_lifetime", False),
            "trial_end": sub.get("trial_end"),
            "current_period_end": sub.get("current_period_end"),
            "trial_eligible": sub.get("trial_eligible", True),
        }
    }

@app.get("/api/check")
async def check_session():
    try:
        info = await post.session_info()
        return {"ok": True, "info": info}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/plan")
async def get_plan(date: Optional[str] = None):
    plans = due._load_plans()
    if not plans:
        plan = due.make_plan(date)
        return {"date": plan.get("date"), "slots": plan.get("slots", [])}
    
    p_path, plan = plans[-1]
    st = post.load_state()
    done = due._posted_slots(st, plan.get("date", ""))
    for s in plan.get("slots", []):
        if s.get("slot") in done:
            s["posted"] = True
    return plan

@app.post("/api/plan/make")
async def make_new_plan(date: Optional[str] = None):
    cfg = settings.load()
    handle = cfg.get("handle") or "ezeslucky"
    # Verify active subscription
    check_active_subscription(handle)
    
    plan = due.make_plan(date)
    return {"ok": True, "plan": plan}

@app.post("/api/draft/fill")
async def fill_draft(req: DraftFillRequest):
    c = settings.load()
    limit = int(c.get("max_chars") or 280)
    text = req.text.strip()
    
    if len(text) > limit:
        raise HTTPException(status_code=400, detail=f"Text is {len(text)} chars; limit is {limit}")
    
    plans = due._load_plans()
    if not plans:
        due.make_plan()
        plans = due._load_plans()
        
    touched = False
    for p_path, plan in plans:
        for s in plan.get("slots", []):
            if s["slot"] == req.slot:
                s["text"] = text
                p_path.write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
                touched = True
                break
        if touched:
            break
            
    if not touched:
        raise HTTPException(status_code=404, detail=f"Slot {req.slot} not found in plan")
    return {"ok": True, "slot": req.slot, "chars": len(text)}

@app.post("/api/post/trigger")
async def trigger_post(req: PostRequest):
    cfg = settings.load()
    handle = cfg.get("handle") or "ezeslucky"
    # Enforce active subscription authorization
    check_active_subscription(handle)

    text = req.text
    kind = req.kind
    
    if not text and req.slot:
        plans = due._load_plans()
        for _, plan in plans:
            for s in plan.get("slots", []):
                if s["slot"] == req.slot and s.get("text"):
                    text = s["text"]
                    kind = s.get("kind", "value")
                    break
    
    if not text:
        raise HTTPException(status_code=400, detail="No text provided and slot has no draft")
        
    try:
        res = await post.do_post(text, kind=kind, dry_run=req.dry_run)
        if res.get("posted") and req.slot and res.get("tweet_url"):
            for p_path, plan in due._load_plans():
                for s in plan.get("slots", []):
                    if s["slot"] == req.slot:
                        s["posted"] = True
                        s["tweet_url"] = res.get("tweet_url")
                        p_path.write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
                        break
        return {"ok": True, "result": res}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/due")
async def check_due():
    d = due.due()
    return {"idle": d is None, "draft": d}

@app.get("/api/research/latest")
async def get_latest_research():
    SWIPE_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(SWIPE_DIR.glob("*.json"))
    if not files:
        return {"items": [], "total": 0}
    latest = files[-1]
    try:
        data = json.loads(latest.read_text(encoding="utf-8"))
        return {
            "date": latest.stem,
            "counts": data.get("counts", {}),
            "status": data.get("status", {}),
            "items": data.get("items", [])[:30]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/shots/{name}")
async def get_shot(name: str):
    if not name.endswith(".png"):
        name = f"{name}.png"
    p = SHOTS_DIR / name
    if not p.exists():
        raise HTTPException(status_code=404, detail="Screenshot not found")
    return FileResponse(p)

# ==================== RAZORPAY SUBSCRIPTION & PRICING APIS ====================

@app.get("/api/subscription/plans")
async def get_subscription_plans():
    """Retrieve trusted public plan catalog with current exchange rate and INR conversions."""
    catalog = subscription.get_public_plans()
    catalog["key_id"] = razorpay_gateway.get_key_id()
    return catalog

@app.get("/api/subscription/status")
async def get_subscription_status():
    """Retrieve full subscription and trial status for the current user."""
    cfg = settings.load()
    handle = cfg.get("handle") or "ezeslucky"
    sub = database.get_subscription(handle)
    plan_meta = subscription.get_plan(sub.get("plan_id") or "")
    sub["plan_name"] = plan_meta["name"] if plan_meta else "None"
    sub["key_id"] = razorpay_gateway.get_key_id()
    return sub

@app.post("/api/subscription/start-trial")
async def start_free_trial(req: Optional[TrialStartRequest] = None):
    """Start 7-day free trial. Prevents repeat claiming."""
    cfg = settings.load()
    handle = cfg.get("handle") or "ezeslucky"
    identifier = (req.identifier if req and req.identifier else None) or handle
    try:
        sub = database.start_free_trial(handle, identifier=identifier)
        st = post.load_state()
        st["is_pro"] = True
        st["subscription_status"] = "trialing"
        post.save_state(st)
        return {"ok": True, "subscription": sub}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/subscription/create-order")
async def create_subscription_order(req: OrderCreateRequest):
    """Create Razorpay order on server with validated plan pricing and INR conversion."""
    cfg = settings.load()
    handle = cfg.get("handle") or "ezeslucky"
    try:
        order = razorpay_gateway.create_order(handle, req.plan_id)
        database.record_order_created(
            user_id=handle,
            plan_id=req.plan_id,
            order_id=order["order_id"],
            amount_usd=order["amount_usd"],
            amount_inr=order["amount_inr"],
            amount_paise=order["amount"],
            exchange_rate=order["exchange_rate"],
            currency=order["currency"],
        )
        return {"ok": True, "order": order}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Razorpay order: {e}")

@app.post("/api/subscription/verify-payment")
async def verify_subscription_payment(req: PaymentVerifyRequest):
    """Cryptographically verify payment signature and activate subscription."""
    cfg = settings.load()
    handle = cfg.get("handle") or "ezeslucky"

    # 1. Cryptographic HMAC signature check
    is_valid = razorpay_gateway.verify_checkout_signature(
        req.razorpay_order_id,
        req.razorpay_payment_id,
        req.razorpay_signature,
    )
    if not is_valid:
        database.record_payment_failed(req.razorpay_order_id, "HMAC signature verification failed")
        raise HTTPException(status_code=400, detail="Invalid payment signature. Payment could not be verified.")

    # 2. Retrieve trusted order record from database
    conn = database.get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM payments WHERE razorpay_order_id = ?", (req.razorpay_order_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Order record not found in system.")
        p_info = dict(row)
    finally:
        conn.close()

    # 3. Confirm status with Razorpay
    rz_payment = razorpay_gateway.fetch_payment_details(req.razorpay_payment_id)
    if rz_payment.get("status") not in ("captured", "authorized", "paid"):
        database.record_payment_failed(req.razorpay_order_id, f"Payment status: {rz_payment.get('status')}")
        raise HTTPException(status_code=400, detail="Payment is not captured or completed in Razorpay.")

    # 4. Activate plan on server
    sub = database.activate_subscription(
        user_id=handle,
        plan_id=p_info["plan_id"],
        order_id=req.razorpay_order_id,
        payment_id=req.razorpay_payment_id,
        amount_usd=p_info["amount_usd"],
        amount_inr=p_info["amount_inr"],
        amount_paise=p_info["amount_paise"],
        exchange_rate=p_info["exchange_rate"],
        raw_details=rz_payment,
        currency=p_info.get("currency", "INR"),
    )

    # 5. Sync to post state
    st = post.load_state()
    st["is_pro"] = True
    st["plan_id"] = p_info["plan_id"]
    st["subscription_status"] = "active"
    post.save_state(st)

    return {"ok": True, "subscription": sub}

@app.post("/api/subscription/webhook")
async def handle_razorpay_webhook(request: Request):
    """Handle incoming Razorpay webhooks idempotently with raw HMAC signature verification."""
    raw_body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")

    if not razorpay_gateway.verify_webhook_signature(raw_body, signature):
        raise HTTPException(status_code=400, detail="Invalid webhook signature")

    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event_id = payload.get("event_id") or payload.get("id") or f"evt_{hashlib.md5(raw_body).hexdigest()}"
    event_type = payload.get("event", "")

    # Idempotent deduplication
    is_new = database.record_webhook_event(event_id, event_type, payload)
    if not is_new:
        return {"ok": True, "status": "duplicate_event_ignored"}

    # Process captured payment
    if event_type in ("payment.captured", "order.paid"):
        entity = payload.get("payload", {}).get("payment", {}).get("entity", {})
        order_id = entity.get("order_id")
        payment_id = entity.get("id")
        notes = entity.get("notes", {})
        user_id = notes.get("user_id")
        plan_id = notes.get("plan_id")
        if user_id and plan_id and order_id and payment_id:
            plan = subscription.get_plan(plan_id)
            if plan:
                rate = float(notes.get("exchange_rate", subscription.get_exchange_rate()))
                amt_inr, paise = subscription.calculate_inr_amount(plan["price_usd"], rate)
                database.activate_subscription(
                    user_id=user_id,
                    plan_id=plan_id,
                    order_id=order_id,
                    payment_id=payment_id,
                    amount_usd=plan["price_usd"],
                    amount_inr=amt_inr,
                    amount_paise=paise,
                    exchange_rate=rate,
                    raw_details=entity,
                )

    return {"ok": True, "event": event_type}

@app.get("/api/subscription/history")
async def get_subscription_history():
    """Retrieve payment receipt ledger for the current user."""
    cfg = settings.load()
    handle = cfg.get("handle") or "ezeslucky"
    history = database.get_payment_history(handle)
    return {"ok": True, "history": history}

# Serve mobile www if present
if WWW_DIR.exists():
    app.mount("/", StaticFiles(directory=str(WWW_DIR), html=True), name="static")

def run(host: str = "0.0.0.0", port: int = 8000):
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    run()
