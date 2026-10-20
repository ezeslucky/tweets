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

                conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(razorpay_order_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON subscriptions(status);")
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_x_user_id ON users(x_user_id);")
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_lower ON users(LOWER(username));")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);")

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
