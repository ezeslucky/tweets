"""Database module for Tweets Autonomous Agent.

Provides SQLite persistence for subscriptions, payment records, webhook idempotency,
and trial abuse prevention with full ACID transaction safety.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
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
                    amount_usd REAL NOT NULL,
                    amount_inr REAL NOT NULL,
                    amount_paise INTEGER NOT NULL,
                    currency TEXT NOT NULL DEFAULT 'INR',
                    exchange_rate REAL NOT NULL,
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

                conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(razorpay_order_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON subscriptions(status);")
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
    amount_usd: float,
    amount_inr: float,
    amount_paise: int,
    exchange_rate: float,
    currency: str = "INR",
) -> None:
    """Record pending order initiation in payments table."""
    init_db()
    with _lock:
        conn = get_connection()
        try:
            now_str = _now_iso()
            with conn:
                conn.execute("""
                    INSERT INTO payments (
                        user_id, plan_id, razorpay_order_id, razorpay_payment_id,
                        amount_usd, amount_inr, amount_paise, currency,
                        exchange_rate, verification_status, raw_details, created_at
                    ) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, 'pending', NULL, ?)
                """, (user_id, plan_id, order_id, amount_usd, amount_inr, amount_paise, currency, exchange_rate, now_str))

                # Note: We do NOT downgrade an active/trialing user to payment_pending
                cursor = conn.cursor()
                cursor.execute("SELECT status FROM subscriptions WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                if not row or row["status"] in ("expired", "cancelled", "none"):
                    conn.execute("""
                        INSERT INTO subscriptions (
                            user_id, plan_id, status, trial_start, trial_end,
                            current_period_start, current_period_end, razorpay_order_id,
                            razorpay_payment_id, created_at, updated_at
                        ) VALUES (?, ?, 'payment_pending', NULL, NULL, NULL, NULL, ?, NULL, ?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET
                            plan_id = excluded.plan_id,
                            status = 'payment_pending',
                            razorpay_order_id = excluded.razorpay_order_id,
                            updated_at = excluded.updated_at;
                    """, (user_id, plan_id, order_id, now_str, now_str))
        finally:
            conn.close()

def activate_subscription(
    user_id: str,
    plan_id: str,
    order_id: str,
    payment_id: str,
    amount_usd: float,
    amount_inr: float,
    amount_paise: int,
    exchange_rate: float,
    raw_details: Optional[Dict[str, Any]] = None,
    currency: str = "INR",
) -> Dict[str, Any]:
    """Atomically activate subscription after verified payment."""
    init_db()
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
                            raw_details = ?
                        WHERE razorpay_order_id = ?
                    """, (payment_id, raw_json, order_id))
                else:
                    conn.execute("""
                        INSERT INTO payments (
                            user_id, plan_id, razorpay_order_id, razorpay_payment_id,
                            amount_usd, amount_inr, amount_paise, currency,
                            exchange_rate, verification_status, raw_details, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified', ?, ?)
                    """, (user_id, plan_id, order_id, payment_id, amount_usd, amount_inr, amount_paise, currency, exchange_rate, raw_json, now_str))

                # Upsert subscriptions table
                conn.execute("""
                    INSERT INTO subscriptions (
                        user_id, plan_id, status, trial_start, trial_end,
                        current_period_start, current_period_end, razorpay_order_id,
                        razorpay_payment_id, created_at, updated_at
                    ) VALUES (?, ?, 'active', NULL, NULL, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        plan_id = excluded.plan_id,
                        status = 'active',
                        current_period_start = excluded.current_period_start,
                        current_period_end = excluded.current_period_end,
                        razorpay_order_id = excluded.razorpay_order_id,
                        razorpay_payment_id = excluded.razorpay_payment_id,
                        updated_at = excluded.updated_at;
                """, (user_id, plan_id, now_str, period_end, order_id, payment_id, now_str, now_str))

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
                       amount_usd, amount_inr, amount_paise, currency, exchange_rate,
                       verification_status, created_at
                FROM payments
                WHERE user_id = ?
                ORDER BY id DESC
            """, (user_id,))
            return [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()
