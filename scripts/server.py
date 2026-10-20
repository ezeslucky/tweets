#!/usr/bin/env python
"""Tweets REST API server for the companion app, autonomous pipeline, and subscription engine."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import dotenv
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
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
import research_catalog
import settings
import subscription
import x_auth

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
    country: Optional[str] = "IN"

class PaymentVerifyRequest(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str

class CheckUsernameRequest(BaseModel):
    username: str

class UpdateUsernameRequest(BaseModel):
    username: str

class ResearchCollectRequest(BaseModel):
    enabled_sources: Optional[List[str]] = None
    custom_feeds: Optional[List[Dict[str, str]]] = None
    hours: Optional[int] = 36

class ResearchPreferencesRequest(BaseModel):
    selected_sources: List[str]
    custom_feeds: Optional[List[Dict[str, str]]] = None
    topics: Optional[List[str]] = None


def extract_session_token(request: Request) -> Optional[str]:
    """Extract session token from Bearer header, cookie, or query parameters."""
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        tok = auth_header.split(" ", 1)[1].strip()
        if tok:
            return tok

    cookie_tok = request.cookies.get("t_session")
    if cookie_tok:
        return cookie_tok.strip()

    query_tok = request.query_params.get("session_token")
    if query_tok:
        return query_tok.strip()

    return None

def get_current_user_optional(request: Request) -> Optional[Dict[str, Any]]:
    """Retrieve authenticated user if valid session exists."""
    token = extract_session_token(request)
    if not token:
        return None
    return database.get_session_user(token)

def require_auth(request: Request) -> Dict[str, Any]:
    """Enforce authenticated session."""
    user = get_current_user_optional(request)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Authentication required: Please sign in with your X account."
        )
    return user

def require_onboarded_user(request: Request) -> Dict[str, Any]:
    """Enforce authenticated session with completed onboarding."""
    user = require_auth(request)
    if not user.get("onboarding_completed"):
        raise HTTPException(
            status_code=403,
            detail="Onboarding incomplete: Please choose your username to continue."
        )
    return user

def check_active_subscription(handle: str) -> Dict[str, Any]:
    """Verify user has an active 7-day trial or paid subscription."""
    sub = database.get_subscription(handle)
    if not sub.get("is_active"):
        raise HTTPException(
            status_code=403,
            detail="Subscription required: Your access has expired or is inactive. Please start your 7-day free trial or select a subscription plan to continue."
        )
    return sub

# ==================== T AUTH & X (TWITTER) OAUTH 2.0 PKCE ====================

@app.get("/api/auth/x/login")
async def x_login(request: Request, redirect_uri: Optional[str] = None):
    """Initiate official X OAuth 2.0 PKCE flow."""
    if not redirect_uri:
        host = request.headers.get("host") or "localhost:8000"
        scheme = request.url.scheme or "http"
        redirect_uri = f"{scheme}://{host}/api/auth/x/callback"

    auth_url, state, _ = x_auth.create_authorization_request(redirect_uri=redirect_uri)
    return {
        "ok": True,
        "auth_url": auth_url,
        "state": state,
    }

@app.get("/api/auth/x/mock-consent", response_class=HTMLResponse)
async def x_mock_consent(request: Request, state: str = "", redirect_uri: str = "", code_challenge: str = ""):
    """Developer simulation consent screen matching X OAuth 2.0 PKCE prompt."""
    target_callback = redirect_uri or "/api/auth/x/callback"
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Authorize T with X</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body {{ background-color: #000000; color: #f7f9f9; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
  </style>
</head>
<body class="min-h-screen flex items-center justify-center p-4 bg-[#0a0a0a]">
  <div class="max-w-md w-full bg-[#16181c] border border-[#2f3336] rounded-3xl p-7 shadow-2xl">
    <div class="flex items-center justify-between mb-6 pb-5 border-b border-[#2f3336]">
      <!-- T Geometric Logo -->
      <div class="w-12 h-12 rounded-2xl bg-[#1d9bf0]/10 border border-[#1d9bf0]/25 flex items-center justify-center text-[#1d9bf0] shadow-sm">
        <svg class="w-7 h-7" viewBox="0 0 32 32" fill="none">
          <path d="M4 6.5C4 5.67 4.67 5 5.5 5H26.5C27.33 5 28 5.67 28 6.5V11C28 11.83 27.33 12.5 26.5 12.5H19.5V26.5C19.5 27.33 18.83 28 18 28H14C13.17 28 12.5 27.33 12.5 26.5V12.5H5.5C4.67 12.5 4 11.83 4 11V6.5Z" fill="#1d9bf0"/>
          <path d="M5.5 5H26.5C27.33 5 28 5.67 28 6.5V8.5L25 10H7L4 8.5V6.5C4 5.67 4.67 5 5.5 5Z" fill="rgba(255,255,255,0.3)"/>
          <path d="M16 12.5V28" stroke="rgba(255,255,255,0.2)" stroke-width="1.5" stroke-linecap="round"/>
        </svg>
      </div>
      <div class="flex items-center gap-1 text-[#71767b]">
        <span class="w-1.5 h-1.5 rounded-full bg-[#1d9bf0] animate-pulse"></span>
        <span class="text-xs font-mono uppercase tracking-widest text-[#71767b]">PKCE S256</span>
      </div>
      <!-- Official X Logo -->
      <div class="w-12 h-12 rounded-2xl bg-white/5 border border-white/10 flex items-center justify-center text-white">
        <svg class="w-5 h-5 fill-current" viewBox="0 0 24 24"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg>
      </div>
    </div>

    <h2 class="text-xl font-bold tracking-tight mb-2">Authorize T to access your X account?</h2>
    <p class="text-xs text-[#71767b] leading-relaxed mb-6">
      This application will be able to see your profile details, read posts, and verify your account identity.
    </p>

    <form method="GET" action="{target_callback}" class="space-y-4">
      <input type="hidden" name="state" value="{state}" />
      
      <div class="bg-[#202327] rounded-2xl p-3.5 border border-[#2f3336]">
        <label class="block text-[11px] font-bold text-[#71767b] uppercase tracking-wider mb-1.5">Sign in as X User</label>
        <div class="flex items-center gap-2">
          <span class="text-[#1d9bf0] font-bold text-base">@</span>
          <input type="text" id="x_handle_input" value="alex_builds" class="w-full bg-transparent text-white font-semibold text-sm focus:outline-none" placeholder="Enter X username" required />
        </div>
      </div>

      <input type="hidden" name="code" id="mock_code_input" value="" />

      <div class="pt-3 flex flex-col gap-2.5">
        <button type="submit" onclick="var h = document.getElementById('x_handle_input').value.trim().replace(/^@/, '') || 'user'; document.getElementById('mock_code_input').value='mock_' + h + '_' + Math.floor(Math.random()*8999999+1000000)" class="w-full py-3.5 bg-[#1d9bf0] hover:bg-[#1a8cd8] text-white font-bold rounded-full transition shadow-lg text-sm flex items-center justify-center gap-2">
          <span>Authorize app</span>
          <svg class="w-4 h-4 fill-current" viewBox="0 0 24 24"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg>
        </button>
        <a href="{target_callback}?error=access_denied&error_description=User+cancelled+authorization&state={state}" class="w-full py-2.5 text-center text-xs font-semibold text-[#71767b] hover:text-white rounded-full transition">
          Cancel
        </a>
      </div>
    </form>

    <div class="mt-6 pt-4 border-t border-[#2f3336] text-[10px] text-[#71767b] flex items-center justify-between">
      <span>OAuth 2.0 PKCE Grant</span>
      <span>Permissions: tweet.read, users.read</span>
    </div>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html_content)

@app.get("/api/auth/x/callback")
async def x_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
):
    """Handle OAuth 2.0 PKCE callback from X."""
    if error:
        err_msg = error_description or error
        return RedirectResponse(url=f"/?auth_error={urllib.parse.quote(err_msg)}", status_code=303)

    if not code or not state:
        return RedirectResponse(url="/?auth_error=Missing+OAuth+code+or+state+parameter", status_code=303)

    try:
        x_profile = await x_auth.exchange_code_for_user_profile(code, state)
    except Exception as e:
        return RedirectResponse(url=f"/?auth_error={urllib.parse.quote(str(e))}", status_code=303)

    user, is_new = database.create_or_update_x_user(
        x_user_id=x_profile["x_user_id"],
        x_username=x_profile["x_username"],
        display_name=x_profile.get("display_name"),
    )

    session_token = database.create_session(user["id"])
    step = "dashboard" if user.get("onboarding_completed") else "username"
    redirect_url = f"/?auth_token={session_token}&step={step}&username={urllib.parse.quote(user['username'])}"

    resp = RedirectResponse(url=redirect_url, status_code=303)
    resp.set_cookie(
        key="t_session",
        value=session_token,
        max_age=30 * 86400,
        httponly=True,
        samesite="lax",
        secure=False,
        path="/",
    )
    return resp

@app.get("/api/auth/me")
async def get_current_user_profile(request: Request):
    """Return authenticated user profile or unauthenticated state."""
    user = get_current_user_optional(request)
    if not user:
        return {"ok": True, "authenticated": False, "user": None}
    return {
        "ok": True,
        "authenticated": True,
        "user": {
            "id": user["id"],
            "x_user_id": user["x_user_id"],
            "username": user["username"],
            "display_name": user["display_name"],
            "onboarding_completed": bool(user["onboarding_completed"]),
            "created_at": user["created_at"],
        }
    }

@app.post("/api/auth/check-username")
async def check_username_availability(req: CheckUsernameRequest, request: Request):
    """Check format validity and database uniqueness of username."""
    user = get_current_user_optional(request)
    exclude_id = user["id"] if user else None

    clean_name = req.username.strip().lstrip("@")
    valid, err = x_auth.validate_username_format(clean_name)
    if not valid:
        return {"ok": True, "valid": False, "available": False, "error": err}

    available = database.check_username_available(clean_name, exclude_user_id=exclude_id)
    if not available:
        return {
            "ok": True,
            "valid": True,
            "available": False,
            "error": f"Username '@{clean_name}' is already taken. Please choose another."
        }

    return {"ok": True, "valid": True, "available": True, "username": clean_name}

@app.post("/api/auth/username")
async def set_username(req: UpdateUsernameRequest, request: Request):
    """Save user-customized username with uniqueness validation."""
    user = require_auth(request)
    clean_name = req.username.strip().lstrip("@")
    
    valid, err = x_auth.validate_username_format(clean_name)
    if not valid:
        raise HTTPException(status_code=400, detail=err)

    try:
        updated_user = database.update_username(user["id"], clean_name)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    return {
        "ok": True,
        "user": {
            "id": updated_user["id"],
            "username": updated_user["username"],
            "display_name": updated_user["display_name"],
            "onboarding_completed": bool(updated_user["onboarding_completed"]),
        }
    }

@app.post("/api/auth/complete-onboarding")
async def finish_onboarding(request: Request):
    """Mark onboarding completed and transition user to active dashboard."""
    user = require_auth(request)
    updated = database.complete_onboarding(user["id"])
    return {
        "ok": True,
        "user": {
            "id": updated["id"],
            "username": updated["username"],
            "display_name": updated["display_name"],
            "onboarding_completed": True,
        }
    }

@app.post("/api/auth/logout")
async def logout(request: Request, response: Response):
    """Invalidate session and clear session cookie."""
    token = extract_session_token(request)
    if token:
        database.delete_session(token)
    response.delete_cookie(key="t_session", path="/")
    return {"ok": True}

# ==================== MAIN COMPANION STATUS ====================

@app.get("/api/status")
async def get_status(request: Request):
    current_user = get_current_user_optional(request)
    st = post.load_state()
    cfg = settings.load()
    handle = current_user["username"] if current_user else (cfg.get("handle") or "ezeslucky")
    sub = database.get_subscription(handle)
    plan_meta = subscription.get_plan(sub.get("plan_id") or "", country_code=sub.get("billing_country", "IN"))
    
    return {
        "handle": handle,
        "user": current_user,
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

# ==================== RESEARCH & DISCOVERY APIS ====================

@app.get("/api/research/sources")
async def get_research_sources():
    """Return complete catalog of 131+ customizable research sources across 14 categories."""
    sources = research_catalog.get_all_sources()
    is_li_configured = bool(os.getenv("LINKEDIN_ACCESS_TOKEN"))
    for s in sources:
        if s["id"] == "linkedin":
            s["is_configured"] = is_li_configured
            s["status_note"] = "Active" if is_li_configured else "Requires LINKEDIN_ACCESS_TOKEN"
        else:
            s["is_configured"] = True
            s["status_note"] = "Ready"

    return {
        "ok": True,
        "total_sources": len(sources),
        "categories": research_catalog.get_categories(),
        "sources": sources,
        "is_linkedin_configured": is_li_configured
    }

@app.get("/api/research/preferences")
async def get_research_preferences(request: Request):
    """Retrieve customized research sources and custom RSS feeds for current user."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")
    prefs = database.get_user_research_preferences(handle)
    return {"ok": True, "preferences": prefs}

@app.post("/api/research/preferences")
async def save_research_preferences(request: Request, req: ResearchPreferencesRequest):
    """Persist user custom source selections and custom feeds."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")
    saved = database.save_user_research_preferences(
        user_id=handle,
        selected_sources=req.selected_sources,
        custom_feeds=req.custom_feeds,
        topics=req.topics
    )
    return {"ok": True, "preferences": saved}

@app.post("/api/research/preferences/reset")
async def reset_research_preferences(request: Request):
    """Reset source preferences to factory defaults."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")
    res = database.reset_user_research_preferences(handle)
    return {"ok": True, "preferences": res}

@app.post("/api/research/collect")
async def collect_fresh_stories(request: Request, background_tasks: BackgroundTasks, req: Optional[ResearchCollectRequest] = None):
    """Start background research collection across all selected sources, including LinkedIn."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")

    if research.is_collecting():
        return {
            "ok": False,
            "busy": True,
            "job_id": research.get_active_job_id(),
            "message": "A research collection job is currently in progress."
        }

    job_id = f"job_{int(datetime.now().timestamp() * 1000)}"
    prefs = database.get_user_research_preferences(handle)
    enabled_sources = (req.enabled_sources if req and req.enabled_sources else None) or prefs.get("selected_sources") or research_catalog.get_default_source_ids()
    custom_feeds = (req.custom_feeds if req and req.custom_feeds else None) or prefs.get("custom_feeds") or []
    hours = (req.hours if req and req.hours else None) or 36

    database.create_research_job(job_id, handle, enabled_sources)

    # Launch background collector
    background_tasks.add_task(
        research.collect_customized,
        user_id=handle,
        enabled_sources=enabled_sources,
        custom_feeds=custom_feeds,
        hours=hours,
        job_id=job_id,
    )

    return {
        "ok": True,
        "job_id": job_id,
        "status": "running",
        "sources_count": len(enabled_sources) + len(custom_feeds),
        "message": f"Collection started for {len(enabled_sources)} sources"
    }

@app.get("/api/research/jobs/status")
async def get_research_job_status(request: Request):
    """Poll status of latest or active background collection run."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")
    is_busy = research.is_collecting()
    active = research.get_active_job_info()
    latest = database.get_latest_research_job(handle)
    return {
        "ok": True,
        "is_collecting": is_busy,
        "active_job": active,
        "latest_job": latest
    }

@app.get("/api/research/jobs/{job_id}")
async def get_research_job_detail(job_id: str):
    """Retrieve detailed progress and story yields for a specific collection job."""
    job = database.get_research_job(job_id)
    if not job:
        if research.is_collecting() and research.get_active_job_id() == job_id:
            return {"ok": True, "job": {"job_id": job_id, "status": "running"}}
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return {"ok": True, "job": job}

@app.get("/api/research/status")
async def get_research_live_status(request: Request):
    """Microsecond-fast status probe for 1-second client-side auto-refresh polling."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")

    counts = database.get_research_stories_count(user_id=handle)
    latest_job = database.get_latest_research_job(handle)
    is_busy = research.is_collecting()
    active_id = research.get_active_job_id()

    # Determine revision string
    latest_ts = ""
    if latest_job and latest_job.get("completed_at"):
        latest_ts = str(latest_job["completed_at"])
    elif latest_job and latest_job.get("created_at"):
        latest_ts = str(latest_job["created_at"])
    else:
        # Fallback to swipe file mtime
        SWIPE_DIR.mkdir(parents=True, exist_ok=True)
        files = sorted(SWIPE_DIR.glob("*.json"))
        if files:
            latest_ts = str(files[-1].stat().st_mtime)

    return {
        "revision": f"{latest_ts}:{counts.get('total', 0)}:{is_busy}",
        "total_stories": counts.get("total", 0),
        "is_collecting": is_busy,
        "active_job_id": active_id,
        "is_linkedin_configured": bool(os.getenv("LINKEDIN_ACCESS_TOKEN")),
        "counts": counts,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/api/research/latest")
async def get_latest_research(
    source: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    search: Optional[str] = None,
    request: Request = None
):
    """Retrieve research stories from SQLite database with fallback to swipe JSON file."""
    user = get_current_user_optional(request) if request else None
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")
    is_li_configured = bool(os.getenv("LINKEDIN_ACCESS_TOKEN"))

    db_items = database.get_research_stories(
        user_id=handle,
        source=source,
        limit=limit,
        offset=offset,
        search=search
    )
    counts = database.get_research_stories_count(user_id=handle)

    if db_items or counts.get("total", 0) > 0:
        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "counts": counts,
            "status": {"db": "ok"},
            "items": db_items,
            "total": counts.get("total", 0),
            "is_linkedin_configured": is_li_configured,
        }

    # Fallback to swipe/YYYY-MM-DD.json
    SWIPE_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(SWIPE_DIR.glob("*.json"))
    if not files:
        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "counts": {"total": 0, "hn": 0, "linkedin": 0, "reddit": 0, "rss": 0},
            "status": {},
            "items": [],
            "total": 0,
            "is_linkedin_configured": is_li_configured,
        }

    latest = files[-1]
    try:
        data = json.loads(latest.read_text(encoding="utf-8"))
        raw_items = data.get("items", [])

        # Filter if requested
        if source and source.lower() != "all":
            s_low = source.lower()
            if s_low in ("linkedin", "li"):
                raw_items = [i for i in raw_items if (i.get("source") or "").lower() == "linkedin"]
            elif s_low in ("hn", "hackernews", "hacker news"):
                raw_items = [i for i in raw_items if (i.get("source") or "").lower() == "hn" or "hacker" in (i.get("source") or "").lower()]
            elif s_low == "reddit":
                raw_items = [i for i in raw_items if (i.get("source") or "").lower().startswith("reddit")]
            elif s_low == "rss":
                raw_items = [i for i in raw_items if (i.get("source") or "").lower().startswith("rss:")]

        if search:
            q = search.lower().strip()
            raw_items = [i for i in raw_items if q in (i.get("title") or "").lower() or q in (i.get("source") or "").lower()]

        return {
            "date": latest.stem,
            "counts": data.get("counts", {}),
            "status": data.get("status", {}),
            "items": raw_items[offset:offset + limit],
            "total": len(raw_items),
            "is_linkedin_configured": is_li_configured,
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
async def get_subscription_plans(country: Optional[str] = "IN"):
    """Retrieve trusted regional public plan catalog in the native currency."""
    catalog = subscription.get_catalog_for_country(country)
    catalog["key_id"] = razorpay_gateway.get_key_id()
    catalog["supported_currencies"] = razorpay_gateway.get_supported_currencies()
    return catalog

@app.get("/api/subscription/status")
async def get_subscription_status(request: Request):
    """Retrieve full subscription and trial status for the current user."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")
    sub = database.get_subscription(handle)
    country = sub.get("billing_country", "IN")
    plan_meta = subscription.get_plan(sub.get("plan_id") or "", country_code=country)
    sub["plan_name"] = plan_meta["name"] if plan_meta else "None"
    sub["key_id"] = razorpay_gateway.get_key_id()
    sub["supported_currencies"] = razorpay_gateway.get_supported_currencies()
    return sub

@app.post("/api/subscription/start-trial")
async def start_free_trial(request: Request, req: Optional[TrialStartRequest] = None):
    """Start 7-day free trial. Prevents repeat claiming."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")
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
async def create_subscription_order(request: Request, req: OrderCreateRequest):
    """Create Razorpay order on server with validated regional pricing."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")
    country = (req.country or "IN").strip().upper()
    try:
        order = razorpay_gateway.create_order(handle, req.plan_id, country_code=country)
        database.record_order_created(
            user_id=handle,
            plan_id=req.plan_id,
            order_id=order["order_id"],
            amount=order["price"],
            amount_subunits=order["amount_subunits"],
            currency=order["currency"],
            billing_country=country,
        )
        return {"ok": True, "order": order}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Razorpay order: {e}")

@app.post("/api/subscription/verify-payment")
async def verify_subscription_payment(request: Request, req: PaymentVerifyRequest):
    """Cryptographically verify payment signature and activate subscription."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")

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
        amount=p_info.get("amount") or p_info.get("amount_usd") or p_info.get("amount_inr") or 0.0,
        amount_subunits=p_info.get("amount_subunits") or p_info.get("amount_paise") or 0,
        currency=p_info.get("currency", "INR"),
        billing_country=p_info.get("billing_country", "IN"),
        raw_details=rz_payment,
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
        country_code = notes.get("country_code", "IN")
        if user_id and plan_id and order_id and payment_id:
            plan = subscription.get_plan(plan_id, country_code=country_code)
            if plan and plan.get("price"):
                currency = plan["currency"]
                subunits = int(round(plan["price"] * 100))
                database.activate_subscription(
                    user_id=user_id,
                    plan_id=plan_id,
                    order_id=order_id,
                    payment_id=payment_id,
                    amount=plan["price"],
                    amount_subunits=subunits,
                    currency=currency,
                    billing_country=country_code,
                    raw_details=entity,
                )

    return {"ok": True, "event": event_type}

@app.get("/api/subscription/history")
async def get_subscription_history(request: Request):
    """Retrieve payment receipt ledger for the current user."""
    user = get_current_user_optional(request)
    cfg = settings.load()
    handle = user["username"] if user else (cfg.get("handle") or "ezeslucky")
    history = database.get_payment_history(handle)
    return {"ok": True, "history": history}

# Serve mobile www if present
if WWW_DIR.exists():
    app.mount("/", StaticFiles(directory=str(WWW_DIR), html=True), name="static")

def run(host: str = "0.0.0.0", port: int = 8000):
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    run()
