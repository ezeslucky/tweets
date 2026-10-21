"""Database module for Tweets Autonomous Agent.

Provides SQLite persistence for subscriptions, payment records, webhook idempotency,
and trial abuse prevention with full ACID transaction safety.
"""
from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

CATEGORY_SLUG_MAP = {
    "ai": "Artificial Intelligence",
    "tech": "Technology",
    "startups": "Startups and Business",
    "world": "World News",
    "social": "Social Media Trends",
    "science": "Science and Research",
    "developers": "Developer Ecosystem",
    "dev": "Developer Ecosystem",
    "finance": "Finance and Economics",
    "gaming": "Gaming and Entertainment",
    "education": "Education and Careers",
    "health": "Health and Environment",
    "regional": "Regional Trends",
}

REGION_SLUG_MAP = {
    "global": "Global",
    "north_america": "North America",
    "europe": "Europe",
    "asia_pacific": "Asia-Pacific",
    "latin_america": "Latin America",
    "middle_east_africa": "Middle East & Africa",
}
from typing import Any, Dict, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "tweets.db"

_lock = threading.RLock()

def get_db_path() -> Path:
    raw_url = os.getenv("DATABASE_URL", "")
    if raw_url.startswith("sqlite:///"):
        path_str = raw_url.replace("sqlite:///", "")
        p = Path(path_str)
        return p if p.is_absolute() else ROOT_DIR / p
    return DB_PATH

def get_connection() -> sqlite3.Connection:
    path = get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    # Enable Write-Ahead Logging (WAL) for superior concurrency and durability
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_db() -> None:
    """Initialize database tables with constraints and indexes."""
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id TEXT PRIMARY KEY,
                    plan_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    trial_start TEXT,
                    trial_end TEXT,
                    current_period_start TEXT,
                    current_period_end TEXT,
                    razorpay_order_id TEXT,
                    razorpay_payment_id TEXT,
                    billing_country TEXT DEFAULT 'IN',
                    currency TEXT DEFAULT 'INR',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """)
                
                conn.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    plan_id TEXT NOT NULL,
                    razorpay_order_id TEXT NOT NULL,
                    razorpay_payment_id TEXT,
                    billing_country TEXT DEFAULT 'IN',
                    currency TEXT NOT NULL DEFAULT 'INR',
                    amount REAL DEFAULT 0,
                    amount_subunits INTEGER DEFAULT 0,
                    amount_usd REAL DEFAULT 0,
                    amount_inr REAL DEFAULT 0,
                    amount_paise INTEGER DEFAULT 0,
                    exchange_rate REAL DEFAULT 1.0,
                    verification_status TEXT NOT NULL,
                    raw_details TEXT,
                    created_at TEXT NOT NULL
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS webhook_events (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    processed_at TEXT NOT NULL
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS trial_registry (
                    identifier TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    claimed_at TEXT NOT NULL
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    x_user_id TEXT UNIQUE NOT NULL,
                    username TEXT UNIQUE NOT NULL,
                    display_name TEXT,
                    onboarding_completed INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS oauth_states (
                    state TEXT PRIMARY KEY,
                    code_verifier TEXT NOT NULL,
                    redirect_uri TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_token TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS research_stories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_url TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_id TEXT,
                    category TEXT,
                    score INTEGER DEFAULT 0,
                    comments INTEGER DEFAULT 0,
                    created_at TEXT,
                    collected_at TEXT NOT NULL,
                    extra TEXT
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS research_jobs (
                    job_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    sources_requested TEXT,
                    sources_completed TEXT,
                    sources_failed TEXT,
                    stories_found INTEGER DEFAULT 0,
                    new_stories INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    finished_at TEXT
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS user_research_preferences (
                    user_id TEXT PRIMARY KEY,
                    selected_sources TEXT NOT NULL,
                    custom_feeds TEXT,
                    topics TEXT,
                    updated_at TEXT NOT NULL
                );
                """)

                conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(razorpay_order_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON subscriptions(status);")
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_x_user_id ON users(x_user_id);")
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_lower ON users(LOWER(username));")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_research_stories_source ON research_stories(source);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_research_stories_collected ON research_stories(collected_at);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_research_jobs_user ON research_jobs(user_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_research_jobs_status ON research_jobs(status);")

                conn.execute("""
                CREATE TABLE IF NOT EXISTS global_trend_stories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_url TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    category TEXT NOT NULL,
                    region TEXT NOT NULL DEFAULT 'Global',
                    country TEXT DEFAULT '',
                    language TEXT DEFAULT 'en',
                    trend_type TEXT NOT NULL DEFAULT 'Trending',
                    score INTEGER DEFAULT 0,
                    comments INTEGER DEFAULT 0,
                    velocity_score REAL DEFAULT 0.0,
                    cross_source_count INTEGER DEFAULT 1,
                    cluster_id TEXT DEFAULT '',
                    published_at TEXT,
                    collected_at TEXT NOT NULL,
                    extra TEXT
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_intelligence_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_date TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    top_developments_json TEXT NOT NULL,
                    ai_tech_trends_json TEXT NOT NULL,
                    fastest_growing_json TEXT NOT NULL,
                    regional_developments_json TEXT NOT NULL,
                    products_research_json TEXT NOT NULL,
                    delta_changes_json TEXT NOT NULL,
                    summary_markdown TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    total_stories_analyzed INTEGER DEFAULT 0
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS trend_collection_schedules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    schedule_time TEXT NOT NULL DEFAULT '06:00',
                    timezone TEXT NOT NULL DEFAULT 'UTC',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    last_run_at TEXT,
                    next_run_at TEXT,
                    last_status TEXT DEFAULT 'idle',
                    last_error TEXT,
                    last_stories_count INTEGER DEFAULT 0,
                    last_new_count INTEGER DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS user_watchlists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    topic_keyword TEXT NOT NULL,
                    category_filter TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    UNIQUE(user_id, topic_keyword)
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS user_bookmarks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    story_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(user_id, story_id)
                );
                """)

                conn.execute("""
                CREATE TABLE IF NOT EXISTS user_trend_preferences (
                    user_id TEXT PRIMARY KEY,
                    preferred_categories TEXT,
                    preferred_regions TEXT,
                    preferred_sources TEXT,
                    preferred_languages TEXT,
                    updated_at TEXT NOT NULL
                );
                """)

                conn.execute("CREATE INDEX IF NOT EXISTS idx_global_trend_cat ON global_trend_stories(category);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_global_trend_reg ON global_trend_stories(region);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_global_trend_type ON global_trend_stories(trend_type);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_global_trend_vel ON global_trend_stories(velocity_score DESC);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_global_trend_pub ON global_trend_stories(published_at DESC);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_global_trend_coll ON global_trend_stories(collected_at DESC);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_intel_reports_date ON daily_intelligence_reports(report_date DESC);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_watchlists_user ON user_watchlists(user_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_bookmarks_user ON user_bookmarks(user_id);")


                # Automatic schema migrations for multi-currency support
                cur = conn.cursor()
                cur.execute("PRAGMA table_info(payments);")
                payment_cols = {col["name"] for col in cur.fetchall()}
                if "billing_country" not in payment_cols:
                    conn.execute("ALTER TABLE payments ADD COLUMN billing_country TEXT DEFAULT 'IN';")
                if "amount" not in payment_cols:
                    conn.execute("ALTER TABLE payments ADD COLUMN amount REAL DEFAULT 0;")
                if "amount_subunits" not in payment_cols:
                    conn.execute("ALTER TABLE payments ADD COLUMN amount_subunits INTEGER DEFAULT 0;")

                cur.execute("PRAGMA table_info(subscriptions);")
                sub_cols = {col["name"] for col in cur.fetchall()}
                if "billing_country" not in sub_cols:
                    conn.execute("ALTER TABLE subscriptions ADD COLUMN billing_country TEXT DEFAULT 'IN';")
                if "currency" not in sub_cols:
                    conn.execute("ALTER TABLE subscriptions ADD COLUMN currency TEXT DEFAULT 'INR';")
        finally:
            conn.close()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _parse_iso(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str:
        return None
    try:
        # Handle trailing Z or timezone offsets
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def get_subscription(user_id: str) -> Dict[str, Any]:
    """Fetch user subscription, automatically evaluating expiration timestamps."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM subscriptions WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            now = datetime.now(timezone.utc)

            # Check if user has claimed trial in registry
            cursor.execute("SELECT 1 FROM trial_registry WHERE user_id = ?", (user_id,))
            trial_claimed = cursor.fetchone() is not None

            if not row:
                return {
                    "user_id": user_id,
                    "plan_id": None,
                    "status": "expired",
                    "is_active": False,
                    "trial_start": None,
                    "trial_end": None,
                    "current_period_start": None,
                    "current_period_end": None,
                    "remaining_trial_days": 0,
                    "remaining_days": 0,
                    "is_lifetime": False,
                    "trial_eligible": not trial_claimed,
                    "has_claimed_trial": trial_claimed,
                    "billing_country": "IN",
                    "currency": "INR",
                }

            sub = dict(row)
            status = sub["status"]
            trial_end_dt = _parse_iso(sub.get("trial_end"))
            period_end_dt = _parse_iso(sub.get("current_period_end"))
            plan_id = sub.get("plan_id")
            is_lifetime = (plan_id == "lifetime")

            # Check expiration for trial
            if status == "trialing":
                if trial_end_dt and now > trial_end_dt:
                    status = "expired"
                    with conn:
                        conn.execute(
                            "UPDATE subscriptions SET status = 'expired', updated_at = ? WHERE user_id = ?",
                            (_now_iso(), user_id),
                        )
                    sub["status"] = "expired"

            # Check expiration for paid periodic subscription
            elif status == "active" and not is_lifetime:
                if period_end_dt and now > period_end_dt:
                    status = "expired"
                    with conn:
                        conn.execute(
                            "UPDATE subscriptions SET status = 'expired', updated_at = ? WHERE user_id = ?",
                            (_now_iso(), user_id),
                        )
                    sub["status"] = "expired"

            # Calculate remaining days
            remaining_trial_days = 0
            if status == "trialing" and trial_end_dt:
                delta = trial_end_dt - now
                remaining_trial_days = max(0, delta.days + (1 if delta.seconds > 0 else 0))

            remaining_days = 0
            if is_lifetime:
                remaining_days = 99999
            elif status == "active" and period_end_dt:
                delta = period_end_dt - now
                remaining_days = max(0, delta.days + (1 if delta.seconds > 0 else 0))

            is_active = (status in ("trialing", "active"))

            return {
                "user_id": user_id,
                "plan_id": plan_id,
                "status": status,
                "is_active": is_active,
                "trial_start": sub.get("trial_start"),
                "trial_end": sub.get("trial_end"),
                "current_period_start": sub.get("current_period_start"),
                "current_period_end": sub.get("current_period_end"),
                "remaining_trial_days": remaining_trial_days,
                "remaining_days": remaining_days,
                "is_lifetime": is_lifetime,
                "trial_eligible": not trial_claimed,
                "has_claimed_trial": trial_claimed,
                "billing_country": sub.get("billing_country", "IN"),
                "currency": sub.get("currency", "INR"),
                "razorpay_order_id": sub.get("razorpay_order_id"),
                "razorpay_payment_id": sub.get("razorpay_payment_id"),
                "updated_at": sub.get("updated_at"),
            }
        finally:
            conn.close()

def start_free_trial(user_id: str, identifier: Optional[str] = None) -> Dict[str, Any]:
    """Start 7-day free trial, preventing repeat claiming."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            id_key = identifier or user_id

            # Check if trial was ever claimed by user or identifier
            cursor.execute("SELECT 1 FROM trial_registry WHERE identifier = ? OR user_id = ?", (id_key, user_id))
            if cursor.fetchone():
                raise ValueError("7-day free trial has already been claimed for this account or device.")

            now = datetime.now(timezone.utc)
            trial_start = now.isoformat()
            trial_end = (now + timedelta(days=7)).isoformat()
            now_str = _now_iso()

            with conn:
                # Record in trial registry
                conn.execute(
                    "INSERT INTO trial_registry (identifier, user_id, claimed_at) VALUES (?, ?, ?)",
                    (id_key, user_id, now_str),
                )

                # Upsert subscription
                conn.execute("""
                    INSERT INTO subscriptions (
                        user_id, plan_id, status, trial_start, trial_end,
                        current_period_start, current_period_end, razorpay_order_id,
                        razorpay_payment_id, created_at, updated_at
                    ) VALUES (?, 'trial', 'trialing', ?, ?, ?, ?, NULL, NULL, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        plan_id = 'trial',
                        status = 'trialing',
                        trial_start = excluded.trial_start,
                        trial_end = excluded.trial_end,
                        updated_at = excluded.updated_at;
                """, (user_id, trial_start, trial_end, trial_start, trial_end, now_str, now_str))

            return get_subscription(user_id)
        finally:
            conn.close()

def record_order_created(
    user_id: str,
    plan_id: str,
    order_id: str,
    amount: float = 0.0,
    amount_subunits: int = 0,
    currency: str = "INR",
    billing_country: str = "IN",
    amount_usd: Optional[float] = None,
    amount_inr: Optional[float] = None,
    amount_paise: Optional[int] = None,
    exchange_rate: Optional[float] = None,
) -> None:
    """Record pending order initiation in payments table with multi-currency tracking."""
    init_db()
    effective_amount = amount
    effective_subunits = amount_subunits

    if amount_usd is not None and not effective_amount:
        effective_amount = amount_usd
    if amount_inr is not None and not effective_amount:
        effective_amount = amount_inr
    if amount_paise is not None and not effective_subunits:
        effective_subunits = amount_paise

    eff_usd = amount_usd if amount_usd is not None else (effective_amount if currency == "USD" else 0.0)
    eff_inr = amount_inr if amount_inr is not None else (effective_amount if currency == "INR" else 0.0)
    eff_paise = amount_paise if amount_paise is not None else effective_subunits
    eff_rate = exchange_rate if exchange_rate is not None else 1.0

    with _lock:
        conn = get_connection()
        try:
            now_str = _now_iso()
            with conn:
                conn.execute("""
                    INSERT INTO payments (
                        user_id, plan_id, razorpay_order_id, razorpay_payment_id,
                        billing_country, currency, amount, amount_subunits,
                        amount_usd, amount_inr, amount_paise,
                        exchange_rate, verification_status, raw_details, created_at
                    ) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?)
                """, (
                    user_id, plan_id, order_id,
                    billing_country, currency, effective_amount, effective_subunits,
                    eff_usd, eff_inr, eff_paise,
                    eff_rate, now_str
                ))

                # Note: We do NOT downgrade an active/trialing user to payment_pending
                cursor = conn.cursor()
                cursor.execute("SELECT status FROM subscriptions WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                if not row or row["status"] in ("expired", "cancelled", "none"):
                    conn.execute("""
                        INSERT INTO subscriptions (
                            user_id, plan_id, status, trial_start, trial_end,
                            current_period_start, current_period_end, razorpay_order_id,
                            razorpay_payment_id, billing_country, currency, created_at, updated_at
                        ) VALUES (?, ?, 'payment_pending', NULL, NULL, NULL, NULL, ?, NULL, ?, ?, ?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET
                            plan_id = excluded.plan_id,
                            status = 'payment_pending',
                            razorpay_order_id = excluded.razorpay_order_id,
                            billing_country = excluded.billing_country,
                            currency = excluded.currency,
                            updated_at = excluded.updated_at;
                    """, (user_id, plan_id, order_id, billing_country, currency, now_str, now_str))
        finally:
            conn.close()

def activate_subscription(
    user_id: str,
    plan_id: str,
    order_id: str,
    payment_id: str,
    amount: float = 0.0,
    amount_subunits: int = 0,
    currency: str = "INR",
    billing_country: str = "IN",
    raw_details: Optional[Dict[str, Any]] = None,
    amount_usd: Optional[float] = None,
    amount_inr: Optional[float] = None,
    amount_paise: Optional[int] = None,
    exchange_rate: Optional[float] = None,
) -> Dict[str, Any]:
    """Atomically activate subscription after verified payment in native currency."""
    init_db()
    effective_amount = amount
    effective_subunits = amount_subunits

    if amount_usd is not None and not effective_amount:
        effective_amount = amount_usd
    if amount_inr is not None and not effective_amount:
        effective_amount = amount_inr
    if amount_paise is not None and not effective_subunits:
        effective_subunits = amount_paise

    eff_usd = amount_usd if amount_usd is not None else (effective_amount if currency == "USD" else 0.0)
    eff_inr = amount_inr if amount_inr is not None else (effective_amount if currency == "INR" else 0.0)
    eff_paise = amount_paise if amount_paise is not None else effective_subunits
    eff_rate = exchange_rate if exchange_rate is not None else 1.0

    with _lock:
        conn = get_connection()
        try:
            now = datetime.now(timezone.utc)
            now_str = now.isoformat()
            
            # Calculate duration
            if plan_id == "monthly":
                period_end = (now + timedelta(days=30)).isoformat()
            elif plan_id == "semiannual":
                period_end = (now + timedelta(days=180)).isoformat()
            elif plan_id == "annual":
                period_end = (now + timedelta(days=365)).isoformat()
            elif plan_id == "lifetime":
                period_end = None
            else:
                raise ValueError(f"Invalid plan ID for paid activation: {plan_id}")

            raw_json = json.dumps(raw_details or {}, ensure_ascii=False)

            with conn:
                # Update or insert payments table
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM payments WHERE razorpay_order_id = ?", (order_id,))
                p_row = cursor.fetchone()
                if p_row:
                    conn.execute("""
                        UPDATE payments SET
                            razorpay_payment_id = ?,
                            verification_status = 'verified',
                            raw_details = ?,
                            billing_country = COALESCE(billing_country, ?),
                            currency = COALESCE(currency, ?),
                            amount = CASE WHEN amount > 0 THEN amount ELSE ? END,
                            amount_subunits = CASE WHEN amount_subunits > 0 THEN amount_subunits ELSE ? END
                        WHERE razorpay_order_id = ?
                    """, (payment_id, raw_json, billing_country, currency, effective_amount, effective_subunits, order_id))
                else:
                    conn.execute("""
                        INSERT INTO payments (
                            user_id, plan_id, razorpay_order_id, razorpay_payment_id,
                            billing_country, currency, amount, amount_subunits,
                            amount_usd, amount_inr, amount_paise,
                            exchange_rate, verification_status, raw_details, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified', ?, ?)
                    """, (
                        user_id, plan_id, order_id, payment_id,
                        billing_country, currency, effective_amount, effective_subunits,
                        eff_usd, eff_inr, eff_paise,
                        eff_rate, raw_json, now_str
                    ))

                # Upsert subscriptions table
                conn.execute("""
                    INSERT INTO subscriptions (
                        user_id, plan_id, status, trial_start, trial_end,
                        current_period_start, current_period_end, razorpay_order_id,
                        razorpay_payment_id, billing_country, currency, created_at, updated_at
                    ) VALUES (?, ?, 'active', NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        plan_id = excluded.plan_id,
                        status = 'active',
                        current_period_start = excluded.current_period_start,
                        current_period_end = excluded.current_period_end,
                        razorpay_order_id = excluded.razorpay_order_id,
                        razorpay_payment_id = excluded.razorpay_payment_id,
                        billing_country = excluded.billing_country,
                        currency = excluded.currency,
                        updated_at = excluded.updated_at;
                """, (user_id, plan_id, now_str, period_end, order_id, payment_id, billing_country, currency, now_str, now_str))

            return get_subscription(user_id)
        finally:
            conn.close()

def record_payment_failed(order_id: str, reason: str = "") -> None:
    """Record payment failure for an order."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                    UPDATE payments SET
                        verification_status = 'failed',
                        raw_details = json_set(COALESCE(raw_details, '{}'), '$.failure_reason', ?)
                    WHERE razorpay_order_id = ?
                """, (reason, order_id))
        finally:
            conn.close()

def record_webhook_event(event_id: str, event_type: str, payload: Dict[str, Any]) -> bool:
    """Idempotently record webhook event. Returns True if newly inserted, False if duplicate."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM webhook_events WHERE event_id = ?", (event_id,))
            if cursor.fetchone():
                return False  # Already processed

            with conn:
                conn.execute("""
                    INSERT INTO webhook_events (event_id, event_type, payload, processed_at)
                    VALUES (?, ?, ?, ?)
                """, (event_id, event_type, json.dumps(payload, ensure_ascii=False), _now_iso()))
            return True
        finally:
            conn.close()

def get_payment_history(user_id: str) -> List[Dict[str, Any]]:
    """Retrieve full payment history for a user."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, user_id, plan_id, razorpay_order_id, razorpay_payment_id,
                       amount, amount_subunits, currency, billing_country,
                       amount_usd, amount_inr, amount_paise, exchange_rate,
                       verification_status, created_at
                FROM payments
                WHERE user_id = ?
                ORDER BY id DESC
            """, (user_id,))
            return [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()

# =========================================================================
# User Management & X OAuth PKCE Sessions
# =========================================================================

def get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve user record by internal user ID."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, x_user_id, username, display_name, onboarding_completed, created_at, updated_at
                FROM users WHERE id = ?
            """, (user_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

def get_user_by_x_id(x_user_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve user record by X provider user ID."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, x_user_id, username, display_name, onboarding_completed, created_at, updated_at
                FROM users WHERE x_user_id = ?
            """, (str(x_user_id),))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    """Retrieve user record by username (case-insensitive)."""
    init_db()
    clean_name = username.strip().lstrip('@')
    with _lock:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, x_user_id, username, display_name, onboarding_completed, created_at, updated_at
                FROM users WHERE LOWER(username) = LOWER(?)
            """, (clean_name,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

def check_username_available(username: str, exclude_user_id: Optional[str] = None) -> bool:
    """Check if a username is available (case-insensitive)."""
    init_db()
    clean_name = username.strip().lstrip('@')
    if not clean_name:
        return False
    with _lock:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            if exclude_user_id:
                cursor.execute("""
                    SELECT 1 FROM users WHERE LOWER(username) = LOWER(?) AND id != ?
                """, (clean_name, exclude_user_id))
            else:
                cursor.execute("""
                    SELECT 1 FROM users WHERE LOWER(username) = LOWER(?)
                """, (clean_name,))
            return cursor.fetchone() is None
        finally:
            conn.close()

def create_or_update_x_user(
    x_user_id: str,
    x_username: str,
    display_name: Optional[str] = None,
) -> Tuple[Dict[str, Any], bool]:
    """Create or update user from verified X OAuth credentials.
    Returns (user_dict, is_new_user).
    """
    init_db()
    str_x_id = str(x_user_id).strip()
    clean_username = x_username.strip().lstrip('@')
    display_name = display_name.strip() if display_name else clean_username
    now_str = _now_iso()

    with _lock:
        conn = get_connection()
        try:
            with conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM users WHERE x_user_id = ?", (str_x_id,))
                existing = cursor.fetchone()
                if existing:
                    # Existing user: update display name and timestamp
                    conn.execute("""
                        UPDATE users SET
                            display_name = COALESCE(?, display_name),
                            updated_at = ?
                        WHERE x_user_id = ?
                    """, (display_name, now_str, str_x_id))
                    cursor.execute("SELECT * FROM users WHERE x_user_id = ?", (str_x_id,))
                    return dict(cursor.fetchone()), False

                # New user: ensure initial username is unique
                candidate_username = clean_username
                cursor.execute("SELECT 1 FROM users WHERE LOWER(username) = LOWER(?)", (candidate_username,))
                if cursor.fetchone():
                    # Collision with another user's chosen username -> append unique suffix
                    candidate_username = f"{clean_username[:10]}_{secrets.randbelow(1000):03d}"

                user_id = f"usr_{uuid.uuid4().hex[:12]}"
                conn.execute("""
                    INSERT INTO users (
                        id, x_user_id, username, display_name,
                        onboarding_completed, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 0, ?, ?)
                """, (user_id, str_x_id, candidate_username, display_name, now_str, now_str))

                cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
                return dict(cursor.fetchone()), True
        finally:
            conn.close()

def update_username(user_id: str, new_username: str) -> Dict[str, Any]:
    """Update username for authenticated user after uniqueness validation."""
    init_db()
    clean_name = new_username.strip().lstrip('@')
    if not clean_name:
        raise ValueError("Username cannot be empty.")
    
    if not check_username_available(clean_name, exclude_user_id=user_id):
        raise ValueError(f"Username '@{clean_name}' is already taken. Please choose another.")

    with _lock:
        conn = get_connection()
        try:
            now_str = _now_iso()
            with conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE users SET
                        username = ?,
                        updated_at = ?
                    WHERE id = ?
                """, (clean_name, now_str, user_id))
                if cursor.rowcount == 0:
                    raise ValueError(f"User '{user_id}' not found.")
                
                cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
                return dict(cursor.fetchone())
        finally:
            conn.close()

def complete_onboarding(user_id: str) -> Dict[str, Any]:
    """Mark onboarding completed for authenticated user."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            now_str = _now_iso()
            with conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE users SET
                        onboarding_completed = 1,
                        updated_at = ?
                    WHERE id = ?
                """, (now_str, user_id))
                cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
                row = cursor.fetchone()
                if not row:
                    raise ValueError(f"User '{user_id}' not found.")
                return dict(row)
        finally:
            conn.close()

def save_oauth_state(state: str, code_verifier: str, redirect_uri: str) -> None:
    """Store PKCE code_verifier and state with single-use lifecycle."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                    INSERT OR REPLACE INTO oauth_states (state, code_verifier, redirect_uri, created_at)
                    VALUES (?, ?, ?, ?)
                """, (state, code_verifier, redirect_uri, _now_iso()))
        finally:
            conn.close()

def get_and_delete_oauth_state(state: str) -> Optional[Dict[str, str]]:
    """Atomically retrieve and consume OAuth state to prevent replay attacks."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                cursor = conn.cursor()
                cursor.execute("SELECT state, code_verifier, redirect_uri FROM oauth_states WHERE state = ?", (state,))
                row = cursor.fetchone()
                if row:
                    conn.execute("DELETE FROM oauth_states WHERE state = ?", (state,))
                    return dict(row)
                return None
        finally:
            conn.close()

def create_session(user_id: str, duration_days: int = 30) -> str:
    """Create and persist a secure session token."""
    init_db()
    token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    expires_at = (now + timedelta(days=duration_days)).isoformat()
    now_str = now.isoformat()

    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                    INSERT INTO sessions (session_token, user_id, expires_at, created_at)
                    VALUES (?, ?, ?, ?)
                """, (token, user_id, expires_at, now_str))
            return token
        finally:
            conn.close()

def get_session_user(session_token: str) -> Optional[Dict[str, Any]]:
    """Retrieve user record if session token is valid and unexpired."""
    if not session_token:
        return None
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            now_str = _now_iso()
            cursor.execute("""
                SELECT u.id, u.x_user_id, u.username, u.display_name,
                       u.onboarding_completed, u.created_at, u.updated_at
                FROM sessions s
                JOIN users u ON s.user_id = u.id
                WHERE s.session_token = ? AND s.expires_at > ?
            """, (session_token, now_str))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

def delete_session(session_token: str) -> None:
    """Invalidate session on logout."""
    if not session_token:
        return
    init_db()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("DELETE FROM sessions WHERE session_token = ?", (session_token,))
        finally:
            conn.close()

# =========================================================================
# Research Swipe Stories, Background Jobs & Customer Preferences
# =========================================================================

def _canonical_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    clean = raw_url.strip().split("#")[0].split("?")[0].rstrip("/").lower()
    return clean

def save_research_stories(
    stories: Optional[List[Dict[str, Any]]] = None,
    user_id: Optional[str] = None,
    **kwargs
) -> Dict[str, int]:
    """Persist and deduplicate newly discovered research stories into the database.
    
    Returns dictionary with inserted count and total processed.
    """
    if stories is None and "stories" in kwargs:
        stories = kwargs["stories"]
    elif stories is None and isinstance(user_id, list):
        stories = user_id
    stories = stories or []

    if not stories:
        return {"inserted": 0, "total": 0}
    init_db()
    new_count = 0
    now_str = _now_iso()

    with _lock:
        conn = get_connection()
        try:
            with conn:
                for s in stories:
                    raw_url = s.get("url") or ""
                    canon = _canonical_url(raw_url)
                    if not canon:
                        continue

                    title = (s.get("title") or "").strip()
                    source = s.get("source") or "unknown"
                    source_id = s.get("source_id") or source
                    category = s.get("category") or "Technology"
                    score = int(s.get("score") or 0)
                    comments = int(s.get("comments") or 0)
                    created_at = s.get("created") or s.get("created_at") or now_str
                    extra_json = json.dumps(s.get("extra") or {}, ensure_ascii=False)

                    # Check existence
                    cur = conn.cursor()
                    cur.execute("SELECT id FROM research_stories WHERE canonical_url = ?", (canon,))
                    row = cur.fetchone()
                    if row:
                        # Update existing metrics
                        conn.execute("""
                            UPDATE research_stories SET
                                score = MAX(score, ?),
                                comments = MAX(comments, ?),
                                collected_at = ?
                            WHERE canonical_url = ?
                        """, (score, comments, now_str, canon))
                    else:
                        conn.execute("""
                            INSERT INTO research_stories (
                                canonical_url, title, source, source_id, category,
                                score, comments, created_at, collected_at, extra
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            canon, title, source, source_id, category,
                            score, comments, created_at, now_str, extra_json
                        ))
                        new_count += 1
            return {"inserted": new_count, "total": len(stories)}
        finally:
            conn.close()

def get_research_stories(
    source_filter: Optional[str] = None,
    search_query: Optional[str] = None,
    limit: int = 60,
    offset: int = 0,
    user_id: Optional[str] = None,
    source: Optional[str] = None,
    search: Optional[str] = None,
) -> List[Dict[str, Any]]:
    # Alias support
    source_filter = source or source_filter
    search_query = search or search_query
    """Retrieve filtered research stories from database."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            query_parts = ["SELECT * FROM research_stories WHERE 1=1"]
            params: List[Any] = []

            if source_filter and source_filter.lower() != "all":
                sf = source_filter.lower()
                if sf == "hacker news" or sf == "hn":
                    query_parts.append("AND (LOWER(source) = 'hn' OR LOWER(source) LIKE 'hn:%')")
                elif sf == "reddit":
                    query_parts.append("AND LOWER(source) LIKE 'reddit%'")
                elif sf == "rss" or sf == "rss feeds":
                    query_parts.append("AND (LOWER(source) LIKE 'rss%' OR LOWER(source) NOT LIKE 'reddit%' AND LOWER(source) NOT LIKE 'linkedin%' AND LOWER(source) != 'hn')")
                elif sf == "linkedin":
                    query_parts.append("AND LOWER(source) LIKE 'linkedin%'")
                else:
                    query_parts.append("AND (LOWER(source) = ? OR LOWER(source_id) = ?)")
                    params.extend([sf, sf])

            if search_query:
                sq = f"%{search_query.strip().lower()}%"
                query_parts.append("AND (LOWER(title) LIKE ? OR LOWER(source) LIKE ?)")
                params.extend([sq, sq])

            query_parts.append("ORDER BY score DESC, id DESC LIMIT ? OFFSET ?")
            params.extend([limit, offset])

            cursor.execute(" ".join(query_parts), params)
            rows = cursor.fetchall()

            out = []
            for r in rows:
                item = dict(r)
                if item.get("extra"):
                    try:
                        item["extra"] = json.loads(item["extra"])
                    except Exception:
                        item["extra"] = {}
                item["url"] = item.get("canonical_url")
                item["created"] = item.get("created_at")
                out.append(item)
            return out
        finally:
            conn.close()

def get_research_stories_count(user_id: Optional[str] = None) -> Dict[str, int]:
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS total FROM research_stories")
            row = cur.fetchone()
            total = int(row["total"] or 0) if row else 0

            cur.execute("SELECT COUNT(*) AS c FROM research_stories WHERE source = 'hn' OR source_id = 'hn'")
            hn_c = int(cur.fetchone()["c"] or 0)

            cur.execute("SELECT COUNT(*) AS c FROM research_stories WHERE source LIKE 'linkedin%' OR source_id = 'linkedin'")
            li_c = int(cur.fetchone()["c"] or 0)

            cur.execute("SELECT COUNT(*) AS c FROM research_stories WHERE source LIKE 'reddit%' OR source_id LIKE 'reddit%'")
            rd_c = int(cur.fetchone()["c"] or 0)

            cur.execute("SELECT COUNT(*) AS c FROM research_stories WHERE source LIKE 'rss%'")
            rss_c = int(cur.fetchone()["c"] or 0)

            return {
                "total": total,
                "hn": hn_c,
                "linkedin": li_c,
                "reddit": rd_c,
                "rss": rss_c,
            }
        finally:
            conn.close()

def create_research_job(job_id: str, user_id: str, sources_requested: List[str]) -> None:
    init_db()
    with _lock:
        conn = get_connection()
        try:
            now_str = _now_iso()
            with conn:
                conn.execute("""
                    INSERT INTO research_jobs (
                        job_id, user_id, status, sources_requested,
                        sources_completed, sources_failed, stories_found,
                        new_stories, error_message, created_at, finished_at
                    ) VALUES (?, ?, 'running', ?, '[]', '{}', 0, 0, NULL, ?, NULL)
                    ON CONFLICT(job_id) DO UPDATE SET
                        status = 'running',
                        sources_requested = excluded.sources_requested;
                """, (job_id, user_id, json.dumps(sources_requested), now_str))
        finally:
            conn.close()

def update_research_job(
    job_id: str,
    status: str,
    sources_completed: Optional[List[str]] = None,
    sources_failed: Optional[Dict[str, str]] = None,
    stories_found: int = 0,
    new_stories: int = 0,
    error_message: str = "",
) -> None:
    init_db()
    with _lock:
        conn = get_connection()
        try:
            now_str = _now_iso()
            with conn:
                conn.execute("""
                    UPDATE research_jobs SET
                        status = ?,
                        sources_completed = COALESCE(?, sources_completed),
                        sources_failed = COALESCE(?, sources_failed),
                        stories_found = ?,
                        new_stories = ?,
                        error_message = ?,
                        finished_at = CASE WHEN ? IN ('completed', 'failed') THEN ? ELSE finished_at END
                    WHERE job_id = ?
                """, (
                    status,
                    json.dumps(sources_completed) if sources_completed is not None else None,
                    json.dumps(sources_failed) if sources_failed is not None else None,
                    stories_found,
                    new_stories,
                    error_message,
                    status,
                    now_str,
                    job_id,
                ))
        finally:
            conn.close()

def get_research_job(job_id: str) -> Optional[Dict[str, Any]]:
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM research_jobs WHERE job_id = ?", (job_id,))
            row = cur.fetchone()
            if not row:
                return None
            res = dict(row)
            for k in ("sources_requested", "sources_completed", "sources_failed"):
                if res.get(k):
                    try:
                        res[k] = json.loads(res[k])
                    except Exception:
                        pass
            return res
        finally:
            conn.close()

def get_latest_research_job(user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            if user_id:
                cur.execute("SELECT * FROM research_jobs WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,))
            else:
                cur.execute("SELECT * FROM research_jobs ORDER BY created_at DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                return None
            res = dict(row)
            for k in ("sources_requested", "sources_completed", "sources_failed"):
                if res.get(k):
                    try:
                        res[k] = json.loads(res[k])
                    except Exception:
                        pass
            return res
        finally:
            conn.close()

def get_user_research_preferences(user_id: str) -> Dict[str, Any]:
    """Retrieve saved source preferences for a customer, falling back to catalog defaults."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM user_research_preferences WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            if row:
                data = dict(row)
                return {
                    "user_id": user_id,
                    "selected_sources": json.loads(data.get("selected_sources") or "[]"),
                    "custom_feeds": json.loads(data.get("custom_feeds") or "[]"),
                    "topics": json.loads(data.get("topics") or "[]"),
                    "updated_at": data.get("updated_at"),
                }
            
            # Default fallback
            from research_catalog import get_default_source_ids
            return {
                "user_id": user_id,
                "selected_sources": get_default_source_ids(),
                "custom_feeds": [],
                "topics": ["AI", "Developer Tools", "Systems", "Web"],
                "updated_at": _now_iso(),
            }
        finally:
            conn.close()

def save_user_research_preferences(
    user_id: str,
    selected_sources: List[str],
    custom_feeds: Optional[List[Dict[str, str]]] = None,
    topics: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Persist user-specific source selections and custom feeds."""
    init_db()
    now_str = _now_iso()
    feeds_list = custom_feeds or []
    topics_list = topics or []

    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                    INSERT INTO user_research_preferences (
                        user_id, selected_sources, custom_feeds, topics, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        selected_sources = excluded.selected_sources,
                        custom_feeds = excluded.custom_feeds,
                        topics = excluded.topics,
                        updated_at = excluded.updated_at;
                """, (
                    user_id,
                    json.dumps(selected_sources),
                    json.dumps(feeds_list),
                    json.dumps(topics_list),
                    now_str,
                ))
            return get_user_research_preferences(user_id)
        finally:
            conn.close()

def reset_user_research_preferences(user_id: str) -> Dict[str, Any]:
    """Reset user preferences to factory defaults."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("DELETE FROM user_research_preferences WHERE user_id = ?", (user_id,))
            return get_user_research_preferences(user_id)
        finally:
            conn.close()

# =========================================================================
# GLOBAL TREND INTELLIGENCE & DISCOVERY PLATFORM
# =========================================================================

def save_global_trend_stories(stories: List[Dict[str, Any]]) -> Dict[str, int]:
    """Persist and update global trend stories with velocity metrics and deduplication."""
    if not stories:
        return {"inserted": 0, "updated": 0, "total": 0}
    init_db()
    new_count = 0
    updated_count = 0
    now_str = _now_iso()

    with _lock:
        conn = get_connection()
        try:
            with conn:
                for s in stories:
                    raw_url = s.get("url") or s.get("canonical_url") or ""
                    canon = _canonical_url(raw_url)
                    if not canon:
                        continue

                    title = (s.get("title") or "").strip()
                    source = s.get("source") or "unknown"
                    source_id = s.get("source_id") or source
                    category = s.get("category") or "Technology"
                    region = s.get("region") or "Global"
                    country = s.get("country") or ""
                    language = s.get("language") or "en"
                    trend_type = s.get("trend_type") or "Trending"
                    score = int(s.get("score") or 0)
                    comments = int(s.get("comments") or 0)
                    velocity = float(s.get("velocity_score") or s.get("velocity") or 0.0)
                    cross_sources = int(s.get("cross_source_count") or 1)
                    cluster_id = s.get("cluster_id") or ""
                    pub_at = s.get("published_at") or s.get("created") or now_str
                    extra_json = json.dumps(s.get("extra") or {}, ensure_ascii=False)

                    cur = conn.cursor()
                    cur.execute("SELECT id, score, velocity_score, cross_source_count FROM global_trend_stories WHERE canonical_url = ?", (canon,))
                    row = cur.fetchone()

                    if row:
                        old_score = row["score"] or 0
                        old_vel = row["velocity_score"] or 0.0
                        old_cs = row["cross_source_count"] or 1
                        conn.execute("""
                            UPDATE global_trend_stories SET
                                score = MAX(?, score),
                                comments = MAX(?, comments),
                                velocity_score = MAX(?, velocity_score),
                                cross_source_count = MAX(?, cross_source_count),
                                trend_type = CASE WHEN ? = 'Breaking' THEN 'Breaking' ELSE trend_type END,
                                collected_at = ?
                            WHERE canonical_url = ?
                        """, (score, comments, velocity, cross_sources, trend_type, now_str, canon))
                        updated_count += 1
                    else:
                        conn.execute("""
                            INSERT INTO global_trend_stories (
                                canonical_url, title, source, source_id, category, region, country, language,
                                trend_type, score, comments, velocity_score, cross_source_count, cluster_id,
                                published_at, collected_at, extra
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            canon, title, source, source_id, category, region, country, language,
                            trend_type, score, comments, velocity, cross_sources, cluster_id,
                            pub_at, now_str, extra_json
                        ))
                        new_count += 1

            return {"inserted": new_count, "updated": updated_count, "total": len(stories)}
        finally:
            conn.close()

def get_global_trend_stories(
    time_range: str = "today",
    category: Optional[str] = None,
    region: Optional[str] = None,
    trend_type: Optional[str] = None,
    source: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "velocity",
    limit: int = 50,
    offset: int = 0,
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Query trend stories with multi-dimensional filtering, search, and ranking."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            query_parts = ["SELECT * FROM global_trend_stories WHERE 1=1"]
            params: List[Any] = []

            # 1. Time range filter
            now = datetime.now(timezone.utc)
            if time_range == "24h" or time_range == "today":
                cutoff = (now - timedelta(hours=24)).isoformat()
                query_parts.append("AND (published_at >= ? OR collected_at >= ?)")
                params.extend([cutoff, cutoff])
            elif time_range == "7d":
                cutoff = (now - timedelta(days=7)).isoformat()
                query_parts.append("AND (published_at >= ? OR collected_at >= ?)")
                params.extend([cutoff, cutoff])
            elif time_range == "30d":
                cutoff = (now - timedelta(days=30)).isoformat()
                query_parts.append("AND (published_at >= ? OR collected_at >= ?)")
                params.extend([cutoff, cutoff])

            # 2. Category filter
            if category and category.lower() != "all":
                cat_target = CATEGORY_SLUG_MAP.get(category.lower(), category).lower()
                query_parts.append("AND (LOWER(category) = ? OR LOWER(category) LIKE ?)")
                params.extend([cat_target, f"%{cat_target}%"])

            # 3. Region filter
            if region and region.lower() != "all" and region.lower() != "global":
                reg_target = REGION_SLUG_MAP.get(region.lower(), region).lower()
                query_parts.append("AND (LOWER(region) = ? OR LOWER(region) LIKE ?)")
                params.extend([reg_target, f"%{reg_target}%"])

            # 4. Trend type filter (Trending, Breaking, Popular, Emerging)
            if trend_type and trend_type.lower() != "all":
                query_parts.append("AND LOWER(trend_type) = ?")
                params.append(trend_type.lower())

            # 5. Source filter
            if source and source.lower() != "all":
                query_parts.append("AND (LOWER(source) = ? OR LOWER(source_id) = ?)")
                params.extend([source.lower(), source.lower()])

            # 6. Search query
            if search:
                sq = f"%{search.strip().lower()}%"
                query_parts.append("AND (LOWER(title) LIKE ? OR LOWER(source) LIKE ? OR LOWER(category) LIKE ?)")
                params.extend([sq, sq, sq])

            # Total count query
            count_sql = "SELECT COUNT(*) AS total FROM (" + " ".join(query_parts) + ")"
            cur.execute(count_sql, params)
            total_items = int(cur.fetchone()["total"] or 0)

            # Sort order
            if sort_by == "velocity":
                query_parts.append("ORDER BY velocity_score DESC, published_at DESC")
            elif sort_by == "score":
                query_parts.append("ORDER BY score DESC, comments DESC, published_at DESC")
            elif sort_by == "recent":
                query_parts.append("ORDER BY published_at DESC, id DESC")
            elif sort_by == "cross_source":
                query_parts.append("ORDER BY cross_source_count DESC, velocity_score DESC")
            else:
                query_parts.append("ORDER BY velocity_score DESC, id DESC")

            query_parts.append("LIMIT ? OFFSET ?")
            params.extend([limit, offset])

            cur.execute(" ".join(query_parts), params)
            rows = cur.fetchall()

            # Retrieve user bookmarks if user_id is provided
            user_bookmarked_ids = set()
            if user_id:
                cur.execute("SELECT story_id FROM user_bookmarks WHERE user_id = ?", (user_id,))
                user_bookmarked_ids = {r["story_id"] for r in cur.fetchall()}

            out = []
            for r in rows:
                item = dict(r)
                if item.get("extra"):
                    try:
                        item["extra"] = json.loads(item["extra"])
                    except Exception:
                        item["extra"] = {}
                item["is_bookmarked"] = item["id"] in user_bookmarked_ids
                out.append(item)

            return {
                "stories": out,
                "total": total_items,
                "limit": limit,
                "offset": offset,
            }
        finally:
            conn.close()

def get_global_trend_overview() -> Dict[str, Any]:
    """Retrieve high-level trend discovery metrics, category breakdowns, and regional summaries."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            now_iso = _now_iso()

            # Total stories
            cur.execute("SELECT COUNT(*) AS total FROM global_trend_stories")
            total = int(cur.fetchone()["total"] or 0)

            # Breaking count (< 24h)
            cur.execute("SELECT COUNT(*) AS c FROM global_trend_stories WHERE trend_type = 'Breaking'")
            breaking_count = int(cur.fetchone()["c"] or 0)

            # Active sources
            cur.execute("SELECT COUNT(DISTINCT source_id) AS c FROM global_trend_stories")
            active_sources_count = int(cur.fetchone()["c"] or 0)

            # Clustered stories (cross_source_count >= 2)
            cur.execute("SELECT COUNT(*) AS c FROM global_trend_stories WHERE cross_source_count >= 2")
            clustered_count = int(cur.fetchone()["c"] or 0)

            # Top 5 highest velocity stories
            cur.execute("SELECT * FROM global_trend_stories ORDER BY velocity_score DESC LIMIT 5")
            top_stories = []
            for r in cur.fetchall():
                it = dict(r)
                if it.get("extra"):
                    try:
                        it["extra"] = json.loads(it["extra"])
                    except Exception:
                        it["extra"] = {}
                top_stories.append(it)

            # Category distribution
            cur.execute("""
                SELECT category, COUNT(*) AS count, ROUND(AVG(velocity_score), 1) AS avg_vel
                FROM global_trend_stories
                GROUP BY category
                ORDER BY count DESC
            """)
            category_stats = [dict(r) for r in cur.fetchall()]

            # Regional distribution
            cur.execute("""
                SELECT region, COUNT(*) AS count, ROUND(AVG(velocity_score), 1) AS avg_vel
                FROM global_trend_stories
                GROUP BY region
                ORDER BY count DESC
            """)
            regional_stats = [dict(r) for r in cur.fetchall()]

            # Emerging topics (high velocity from niche dev/research sources)
            cur.execute("""
                SELECT * FROM global_trend_stories
                WHERE trend_type = 'Emerging'
                ORDER BY velocity_score DESC LIMIT 6
            """)
            emerging_stories = [dict(r) for r in cur.fetchall()]

            top_vel = top_stories[0]["velocity_score"] if top_stories else 0.0
            return {
                "total_stories": total,
                "total_stories_indexed": total,
                "top_velocity": top_vel,
                "sources_monitored": 198,
                "breaking_count": breaking_count,
                "active_sources_count": active_sources_count,
                "clustered_count": clustered_count,
                "trend_type_counts": {
                    "breaking": breaking_count,
                    "trending": sum(1 for s in top_stories if s.get("trend_type") == "Trending"),
                },
                "top_stories": top_stories,
                "category_stats": category_stats,
                "regional_stats": regional_stats,
                "emerging_stories": emerging_stories,
                "timestamp": now_iso,
            }
        finally:
            conn.close()

# -------------------------------------------------- Daily Intelligence Reports
def save_daily_intelligence_report(report_data: Dict[str, Any]) -> Dict[str, Any]:
    """Store or replace daily executive intelligence brief."""
    init_db()
    r_date = report_data["report_date"]
    now_str = _now_iso()

    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                    INSERT INTO daily_intelligence_reports (
                        report_date, title, top_developments_json, ai_tech_trends_json,
                        fastest_growing_json, regional_developments_json, products_research_json,
                        delta_changes_json, summary_markdown, created_at, total_stories_analyzed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(report_date) DO UPDATE SET
                        title = excluded.title,
                        top_developments_json = excluded.top_developments_json,
                        ai_tech_trends_json = excluded.ai_tech_trends_json,
                        fastest_growing_json = excluded.fastest_growing_json,
                        regional_developments_json = excluded.regional_developments_json,
                        products_research_json = excluded.products_research_json,
                        delta_changes_json = excluded.delta_changes_json,
                        summary_markdown = excluded.summary_markdown,
                        created_at = excluded.created_at,
                        total_stories_analyzed = excluded.total_stories_analyzed;
                """, (
                    r_date,
                    report_data.get("title", f"Daily Intelligence Brief — {r_date}"),
                    json.dumps(report_data.get("top_developments", []), ensure_ascii=False),
                    json.dumps(report_data.get("ai_tech_trends", []), ensure_ascii=False),
                    json.dumps(report_data.get("fastest_growing", []), ensure_ascii=False),
                    json.dumps(report_data.get("regional_developments", []), ensure_ascii=False),
                    json.dumps(report_data.get("products_research", []), ensure_ascii=False),
                    json.dumps(report_data.get("delta_changes", []), ensure_ascii=False),
                    report_data.get("summary_markdown", ""),
                    now_str,
                    int(report_data.get("total_stories_analyzed", 0)),
                ))
            return get_daily_intelligence_report(r_date)
        finally:
            conn.close()

def get_daily_intelligence_report(report_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Retrieve full daily intelligence brief for target date or most recent."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            if report_date:
                cur.execute("SELECT * FROM daily_intelligence_reports WHERE report_date = ?", (report_date,))
            else:
                cur.execute("SELECT * FROM daily_intelligence_reports ORDER BY report_date DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                return None
            res = dict(row)
            for k in ("top_developments_json", "ai_tech_trends_json", "fastest_growing_json",
                      "regional_developments_json", "products_research_json", "delta_changes_json"):
                plain_k = k.replace("_json", "")
                if res.get(k):
                    try:
                        res[plain_k] = json.loads(res[k])
                    except Exception:
                        res[plain_k] = []
                else:
                    res[plain_k] = []
            return res
        finally:
            conn.close()

def list_daily_intelligence_reports(limit: int = 30) -> List[Dict[str, Any]]:
    """Return historical directory of daily reports."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, report_date, title, created_at, total_stories_analyzed
                FROM daily_intelligence_reports
                ORDER BY report_date DESC LIMIT ?
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

# -------------------------------------------------- Schedules & Settings
def get_trend_schedule() -> Dict[str, Any]:
    """Retrieve current background trend collection schedule configuration."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM trend_collection_schedules ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                return dict(row)
            # Default schedule: Daily at 06:00 UTC
            now_str = _now_iso()
            with conn:
                conn.execute("""
                    INSERT INTO trend_collection_schedules (
                        schedule_time, timezone, enabled, last_run_at, next_run_at,
                        last_status, last_stories_count, last_new_count, updated_at
                    ) VALUES ('06:00', 'UTC', 1, NULL, NULL, 'idle', 0, 0, ?)
                """, (now_str,))
            cur.execute("SELECT * FROM trend_collection_schedules ORDER BY id DESC LIMIT 1")
            return dict(cur.fetchone())
        finally:
            conn.close()

def update_trend_schedule(schedule_time: str, timezone_str: str = "UTC", enabled: bool = True) -> Dict[str, Any]:
    """Update automated collection schedule time and timezone."""
    init_db()
    now_str = _now_iso()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                    UPDATE trend_collection_schedules SET
                        schedule_time = ?,
                        timezone = ?,
                        enabled = ?,
                        updated_at = ?
                    WHERE id = (SELECT id FROM trend_collection_schedules ORDER BY id DESC LIMIT 1);
                """, (schedule_time, timezone_str, 1 if enabled else 0, now_str))
            return get_trend_schedule()
        finally:
            conn.close()

def record_trend_schedule_run(
    status: str,
    stories_count: int = 0,
    new_count: int = 0,
    error_message: Optional[str] = None
) -> None:
    init_db()
    now_str = _now_iso()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                    UPDATE trend_collection_schedules SET
                        last_run_at = ?,
                        last_status = ?,
                        last_stories_count = ?,
                        last_new_count = ?,
                        last_error = ?,
                        updated_at = ?
                    WHERE id = (SELECT id FROM trend_collection_schedules ORDER BY id DESC LIMIT 1);
                """, (now_str, status, stories_count, new_count, error_message, now_str))
        finally:
            conn.close()

# -------------------------------------------------- User Watchlists & Bookmarks
def get_user_watchlist(user_id: str) -> List[Dict[str, Any]]:
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM user_watchlists WHERE user_id = ? ORDER BY created_at DESC", (user_id,))
            items = []
            for r in cur.fetchall():
                it = dict(r)
                it["keyword"] = it.get("topic_keyword")
                items.append(it)
            return items
        finally:
            conn.close()

def add_user_watchlist_item(user_id: str, keyword: str, category_filter: str = "") -> Dict[str, Any]:
    init_db()
    clean_kw = keyword.strip()
    now_str = _now_iso()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                    INSERT INTO user_watchlists (user_id, topic_keyword, category_filter, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(user_id, topic_keyword) DO UPDATE SET
                        category_filter = excluded.category_filter;
                """, (user_id, clean_kw, category_filter, now_str))
            return {"user_id": user_id, "topic_keyword": clean_kw, "keyword": clean_kw, "category_filter": category_filter}
        finally:
            conn.close()

def remove_user_watchlist_item(user_id: str, keyword: str) -> bool:
    init_db()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("DELETE FROM user_watchlists WHERE user_id = ? AND topic_keyword = ?", (user_id, keyword.strip()))
            return True
        finally:
            conn.close()

def get_user_bookmarks(user_id: str) -> List[Dict[str, Any]]:
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT s.*, b.created_at AS bookmarked_at
                FROM user_bookmarks b
                JOIN global_trend_stories s ON b.story_id = s.id
                WHERE b.user_id = ?
                ORDER BY b.created_at DESC
            """, (user_id,))
            rows = cur.fetchall()
            out = []
            for r in rows:
                it = dict(r)
                if it.get("extra"):
                    try:
                        it["extra"] = json.loads(it["extra"])
                    except Exception:
                        it["extra"] = {}
                it["is_bookmarked"] = True
                out.append(it)
            return out
        finally:
            conn.close()

def add_user_bookmark(user_id: str, story_id: int) -> bool:
    init_db()
    now_str = _now_iso()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                    INSERT INTO user_bookmarks (user_id, story_id, created_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id, story_id) DO NOTHING;
                """, (user_id, story_id, now_str))
            return True
        finally:
            conn.close()

def remove_user_bookmark(user_id: str, story_id: int) -> bool:
    init_db()
    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("DELETE FROM user_bookmarks WHERE user_id = ? AND story_id = ?", (user_id, story_id))
            return True
        finally:
            conn.close()

def get_user_trend_preferences(user_id: str) -> Dict[str, Any]:
    init_db()
    with _lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM user_trend_preferences WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            if row:
                r = dict(row)
                return {
                    "user_id": user_id,
                    "preferred_categories": json.loads(r.get("preferred_categories") or "[]"),
                    "preferred_regions": json.loads(r.get("preferred_regions") or "[]"),
                    "preferred_sources": json.loads(r.get("preferred_sources") or "[]"),
                    "preferred_languages": json.loads(r.get("preferred_languages") or "[]"),
                    "updated_at": r.get("updated_at"),
                }
            return {
                "user_id": user_id,
                "preferred_categories": ["Artificial Intelligence", "Technology", "Startups and Business", "World News"],
                "preferred_regions": ["Global", "North America", "Europe", "Asia-Pacific"],
                "preferred_sources": [],
                "preferred_languages": ["en"],
                "updated_at": _now_iso(),
            }
        finally:
            conn.close()

def save_user_trend_preferences(user_id: str, prefs: Dict[str, Any]) -> Dict[str, Any]:
    init_db()
    now_str = _now_iso()
    cats = json.dumps(prefs.get("preferred_categories", []))
    regs = json.dumps(prefs.get("preferred_regions", []))
    srcs = json.dumps(prefs.get("preferred_sources", []))
    langs = json.dumps(prefs.get("preferred_languages", ["en"]))

    with _lock:
        conn = get_connection()
        try:
            with conn:
                conn.execute("""
                    INSERT INTO user_trend_preferences (
                        user_id, preferred_categories, preferred_regions,
                        preferred_sources, preferred_languages, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        preferred_categories = excluded.preferred_categories,
                        preferred_regions = excluded.preferred_regions,
                        preferred_sources = excluded.preferred_sources,
                        preferred_languages = excluded.preferred_languages,
                        updated_at = excluded.updated_at;
                """, (user_id, cats, regs, srcs, langs, now_str))
            return get_user_trend_preferences(user_id)
        finally:
            conn.close()
