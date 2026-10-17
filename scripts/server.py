#!/usr/bin/env python
"""TweetyTweets REST API server for the mobile companion app and dashboard."""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Ensure scripts dir is on sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPTS_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import due
import post
import research
import settings

app = FastAPI(title="TweetyTweets Companion API", version="1.0.0")

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

@app.get("/api/status")
async def get_status():
    st = post.load_state()
    cfg = settings.load()
    return {
        "handle": cfg.get("handle", ""),
        "max_chars": int(cfg.get("max_chars", 280)),
        "ai_update_day": st.get("ai_update_day", 0),
        "next_day": post.next_day(st),
        "total_posts": len(st.get("posts", [])),
        "last_post": st.get("posts", [])[-1] if st.get("posts") else None,
        "slots": cfg.get("slots", []),
        "ai_update_slot": cfg.get("ai_update_slot", "16:00"),
        "timezone": cfg.get("timezone", "local"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
        # Generate plan for today if none exists
        plan = due.make_plan(date)
        return {"date": plan.get("date"), "slots": plan.get("slots", [])}
    
    # Return latest plan
    p_path, plan = plans[-1]
    st = post.load_state()
    done = due._posted_slots(st, plan.get("date", ""))
    for s in plan.get("slots", []):
        if s.get("slot") in done:
            s["posted"] = True
    return plan

@app.post("/api/plan/make")
async def make_new_plan(date: Optional[str] = None):
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
            # Mark slot
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
class PaymentConfigRequest(BaseModel):
    paypal_client_id: Optional[str] = None
    paypal_me_user: Optional[str] = None

class PaymentCompleteRequest(BaseModel):
    order_id: str
    payer_email: Optional[str] = None
    amount: Optional[str] = None
    tier: Optional[str] = "supporter"

@app.get("/api/payment/config")
async def get_payment_config():
    st = post.load_state()
    cfg = settings.load()
    return {
        "paypal_client_id": cfg.get("paypal_client_id", "sb"),
        "paypal_me_user": cfg.get("paypal_me_user", "ezeslucky"),
        "is_pro": st.get("is_pro", False),
        "payments": st.get("payments", [])
    }

@app.post("/api/payment/config")
async def set_payment_config(req: PaymentConfigRequest):
    c_path = ROOT_DIR / "config.json"
    if c_path.exists():
        cfg = json.loads(c_path.read_text(encoding="utf-8"))
        if req.paypal_client_id is not None:
            cfg["paypal_client_id"] = req.paypal_client_id.strip()
        if req.paypal_me_user is not None:
            cfg["paypal_me_user"] = req.paypal_me_user.strip()
        c_path.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"ok": True}

@app.post("/api/payment/complete")
async def record_payment(req: PaymentCompleteRequest):
    st = post.load_state()
    st["is_pro"] = True
    payments = st.get("payments", [])
    payments.append({
        "order_id": req.order_id,
        "payer_email": req.payer_email,
        "amount": req.amount,
        "tier": req.tier,
        "at": datetime.now(timezone.utc).isoformat()
    })
    st["payments"] = payments
    post.save_state(st)
    return {"ok": True, "is_pro": True}

# Serve mobile www if present
if WWW_DIR.exists():
    app.mount("/", StaticFiles(directory=str(WWW_DIR), html=True), name="static")

def run(host: str = "0.0.0.0", port: int = 8000):
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    run()
