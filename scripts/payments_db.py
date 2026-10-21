"""Database persistence and admin configuration for flexible multi-provider payment system."""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import database


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_payment_system_tables(conn: sqlite3.Connection) -> None:
    """Create modular payment tables, indexes, and seed default configs if empty."""
    with conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS payment_providers_config (
            provider_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 1,
            mode TEXT NOT NULL DEFAULT 'sandbox',
            supported_currencies TEXT NOT NULL,
            supported_regions TEXT NOT NULL,
            webhook_secret_set INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS payment_methods_config (
            method_id TEXT PRIMARY KEY,
            provider_id TEXT NOT NULL,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            icon TEXT NOT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            is_recurring_supported INTEGER NOT NULL DEFAULT 1,
            supported_currencies TEXT NOT NULL,
            supported_regions TEXT NOT NULL,
            display_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS pricing_plans_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            region TEXT NOT NULL,
            name TEXT NOT NULL,
            price REAL,
            duration_days INTEGER NOT NULL,
            billing_period TEXT NOT NULL,
            description TEXT,
            badge TEXT,
            features_json TEXT NOT NULL DEFAULT '[]',
            is_recurring INTEGER NOT NULL DEFAULT 1,
            is_lifetime INTEGER NOT NULL DEFAULT 0,
            recommended INTEGER NOT NULL DEFAULT 0,
            checkout_enabled INTEGER NOT NULL DEFAULT 1,
            unconfigured_reason TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(plan_id, currency, region)
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS payment_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT UNIQUE NOT NULL,
            idempotency_key TEXT UNIQUE NOT NULL,
            user_id TEXT NOT NULL,
            plan_id TEXT NOT NULL,
            provider_id TEXT NOT NULL,
            payment_method_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            amount REAL NOT NULL,
            amount_subunits INTEGER NOT NULL,
            status TEXT NOT NULL,
            provider_order_id TEXT,
            provider_payment_id TEXT,
            provider_refund_id TEXT,
            billing_country TEXT DEFAULT 'IN',
            error_message TEXT,
            raw_details TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS payment_audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor TEXT NOT NULL,
            action TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            details TEXT,
            created_at TEXT NOT NULL
        );
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_trans_user ON payment_transactions(user_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trans_status ON payment_transactions(status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trans_order ON payment_transactions(provider_order_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trans_created ON payment_transactions(created_at DESC);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_lookup ON pricing_plans_config(plan_id, currency, region);")

        # 1. Seed payment providers if empty
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM payment_providers_config")
        if cur.fetchone()[0] == 0:
            now = _now_iso()
            cur.execute("""
                INSERT INTO payment_providers_config (provider_id, name, is_enabled, priority, mode, supported_currencies, supported_regions, webhook_secret_set, created_at, updated_at)
                VALUES 
                ('razorpay', 'Razorpay', 1, 1, 'sandbox', 'INR,USD,EUR', 'IN,GLOBAL', 1, ?, ?),
                ('stripe', 'Stripe', 1, 2, 'sandbox', 'USD,EUR,INR', 'US,EU,GLOBAL', 1, ?, ?),
                ('paypal', 'PayPal', 1, 3, 'sandbox', 'USD,EUR', 'US,EU,GLOBAL', 1, ?, ?)
            """, (now, now, now, now, now, now))

        # 2. Seed payment methods if empty
        cur.execute("SELECT COUNT(*) FROM payment_methods_config")
        if cur.fetchone()[0] == 0:
            now = _now_iso()
            cur.execute("""
                INSERT INTO payment_methods_config (method_id, provider_id, name, category, icon, is_enabled, is_recurring_supported, supported_currencies, supported_regions, display_order, created_at, updated_at)
                VALUES 
                ('upi', 'razorpay', 'UPI (Google Pay, PhonePe, Paytm, QR)', 'instant', 'smartphone', 1, 1, 'INR', 'IN,india', 1, ?, ?),
                ('card', 'razorpay', 'Credit / Debit Card', 'card', 'credit-card', 1, 1, 'INR,USD,EUR', 'IN,US,EU,GLOBAL', 2, ?, ?),
                ('apple_pay', 'stripe', 'Apple Pay & Google Pay', 'wallet', 'smartphone', 1, 1, 'USD,EUR,INR', 'US,EU,GLOBAL', 3, ?, ?),
                ('netbanking', 'razorpay', 'Net Banking', 'bank', 'building-2', 1, 0, 'INR', 'IN,india', 4, ?, ?),
                ('wallet', 'razorpay', 'Digital Wallets', 'wallet', 'wallet', 1, 0, 'INR', 'IN,india', 5, ?, ?),
                ('paypal', 'paypal', 'PayPal', 'wallet', 'wallet', 1, 1, 'USD,EUR', 'US,EU,GLOBAL', 6, ?, ?),
                ('sepa_debit', 'stripe', 'SEPA Direct Debit', 'bank', 'building-2', 1, 1, 'EUR', 'EU,europe', 7, ?, ?)
            """, (now, now, now, now, now, now, now, now, now, now, now, now, now, now))

        # 3. Seed pricing plans matrix if empty
        cur.execute("SELECT COUNT(*) FROM pricing_plans_config")
        if cur.fetchone()[0] == 0:
            now = _now_iso()
            features_std = json.dumps([
                "Unlimited autonomous posts",
                "204 global news swipe sources",
                "CDP live browser profile verification",
                "Full analytics and sync"
            ])
            features_trial = json.dumps([
                "Autonomous X posting pipeline",
                "Daily news research (HN, Reddit, RSS)",
                "Slot planning with jitter",
                "Browser CDP verification",
                "Full mobile companion dashboard"
            ])
            features_lifetime = json.dumps([
                "Permanent lifetime access",
                "All future AI model upgrades",
                "Priority queue execution",
                "Dedicated API limits",
                "VIP developer support"
            ])

            plans_seed = [
                # India (INR)
                ('trial', 'INR', 'india', '7-Day Free Trial', 0, 7, '7 days', 'Full access for 7 days', 'Free Trial', features_trial, 0, 0, 0, 1, None),
                ('monthly', 'INR', 'india', 'Monthly Plan', 250, 30, '1 month', 'Full autonomous power billed monthly', 'Flexible', features_std, 1, 0, 0, 1, None),
                ('six_month', 'INR', 'india', '6-Month Plan', 500, 180, '6 months', 'Best value for sustained momentum', 'Popular', features_std, 1, 0, 0, 1, None),
                ('annual', 'INR', 'india', 'Annual Plan', 999, 365, '1 year', 'Maximum savings for creators', 'Best Value', features_std, 1, 0, 1, 1, None),
                ('lifetime', 'INR', 'india', 'Lifetime Access', None, 36500, 'One-time', 'Pay once, autonomous forever', 'VIP', features_lifetime, 0, 1, 0, 0, 'Lifetime price for India must be configured separately by admin.'),

                # USA (USD)
                ('trial', 'USD', 'usa', '7-Day Free Trial', 0, 7, '7 days', 'Full access for 7 days', 'Free Trial', features_trial, 0, 0, 0, 1, None),
                ('monthly', 'USD', 'usa', 'Monthly Plan', 225, 30, '1 month', 'Full autonomous agent power billed monthly', 'Flexible', features_std, 1, 0, 0, 1, None),
                ('six_month', 'USD', 'usa', '6-Month Plan', 400, 180, '6 months', 'Best value for sustained growth', 'Popular', features_std, 1, 0, 0, 1, None),
                ('annual', 'USD', 'usa', 'Annual Plan', 500, 365, '1 year', 'Recommended option with full access', 'Recommended', features_std, 1, 0, 1, 1, None),
                ('lifetime', 'USD', 'usa', 'Lifetime Access', 2000, 36500, 'One-time', 'One-time payment for permanent access', 'Lifetime VIP', features_lifetime, 0, 1, 0, 1, None),

                # Europe (EUR)
                ('trial', 'EUR', 'europe', '7-Day Free Trial', 0, 7, '7 days', 'Full access for 7 days', 'Free Trial', features_trial, 0, 0, 0, 1, None),
                ('monthly', 'EUR', 'europe', 'Monthly Plan', 210, 30, '1 month', 'Full autonomous agent power billed monthly', 'Flexible', features_std, 1, 0, 0, 1, None),
                ('six_month', 'EUR', 'europe', '6-Month Plan', 380, 180, '6 months', 'Best value for sustained momentum', 'Popular', features_std, 1, 0, 0, 1, None),
                ('annual', 'EUR', 'europe', 'Annual Plan', 475, 365, '1 year', 'Recommended option for European creators', 'Best Value', features_std, 1, 0, 1, 1, None),
                ('lifetime', 'EUR', 'europe', 'Lifetime Access', 1900, 36500, 'One-time', 'One-time payment for permanent access', 'Lifetime VIP', features_lifetime, 0, 1, 0, 1, None),
            ]

            cur.executemany("""
                INSERT INTO pricing_plans_config (
                    plan_id, currency, region, name, price, duration_days, billing_period,
                    description, badge, features_json, is_recurring, is_lifetime, recommended,
                    checkout_enabled, unconfigured_reason, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [p + (now, now) for p in plans_seed])


# ---------------------------------------------------------------------------
# Admin Providers Config
# ---------------------------------------------------------------------------

def get_payment_providers_config() -> List[Dict[str, Any]]:
    """Retrieve all payment providers with admin settings."""
    database.init_db()
    with database._lock:
        conn = database.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM payment_providers_config ORDER BY priority ASC, provider_id ASC")
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def update_payment_provider_config(
    provider_id: str,
    is_enabled: Optional[bool] = None,
    priority: Optional[int] = None,
    mode: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Update admin settings for a payment provider."""
    database.init_db()
    now = _now_iso()
    with database._lock:
        conn = database.get_connection()
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM payment_providers_config WHERE provider_id = ?", (provider_id,))
            existing = cur.fetchone()
            if not existing:
                return None

            new_enabled = int(is_enabled) if is_enabled is not None else existing["is_enabled"]
            new_priority = priority if priority is not None else existing["priority"]
            new_mode = mode if mode is not None else existing["mode"]

            cur.execute("""
                UPDATE payment_providers_config
                SET is_enabled = ?, priority = ?, mode = ?, updated_at = ?
                WHERE provider_id = ?
            """, (new_enabled, new_priority, new_mode, now, provider_id))

            cur.execute("SELECT * FROM payment_providers_config WHERE provider_id = ?", (provider_id,))
            updated = cur.fetchone()
            record_payment_audit_log(
                actor="admin",
                action="update_provider",
                entity_type="provider",
                entity_id=provider_id,
                details=json.dumps({"is_enabled": new_enabled, "priority": new_priority, "mode": new_mode}),
                conn=conn
            )
            return dict(updated) if updated else None


# ---------------------------------------------------------------------------
# Admin Payment Methods Config
# ---------------------------------------------------------------------------

def get_payment_methods_config(provider_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Retrieve payment methods configuration."""
    database.init_db()
    with database._lock:
        conn = database.get_connection()
        cur = conn.cursor()
        if provider_id:
            cur.execute("SELECT * FROM payment_methods_config WHERE provider_id = ? ORDER BY display_order ASC", (provider_id,))
        else:
            cur.execute("SELECT * FROM payment_methods_config ORDER BY display_order ASC, method_id ASC")
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def update_payment_method_config(
    method_id: str,
    is_enabled: Optional[bool] = None,
    display_order: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """Enable or disable individual payment method."""
    database.init_db()
    now = _now_iso()
    with database._lock:
        conn = database.get_connection()
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM payment_methods_config WHERE method_id = ?", (method_id,))
            existing = cur.fetchone()
            if not existing:
                return None

            new_enabled = int(is_enabled) if is_enabled is not None else existing["is_enabled"]
            new_order = display_order if display_order is not None else existing["display_order"]

            cur.execute("""
                UPDATE payment_methods_config
                SET is_enabled = ?, display_order = ?, updated_at = ?
                WHERE method_id = ?
            """, (new_enabled, new_order, now, method_id))

            cur.execute("SELECT * FROM payment_methods_config WHERE method_id = ?", (method_id,))
            updated = cur.fetchone()
            record_payment_audit_log(
                actor="admin",
                action="update_method",
                entity_type="method",
                entity_id=method_id,
                details=json.dumps({"is_enabled": new_enabled, "display_order": new_order}),
                conn=conn
            )
            return dict(updated) if updated else None


# ---------------------------------------------------------------------------
# Database-Backed Pricing Plans
# ---------------------------------------------------------------------------

def get_db_pricing_plans(currency: Optional[str] = None, region: Optional[str] = None) -> List[Dict[str, Any]]:
    """Retrieve database-backed plans strictly for currency and region."""
    database.init_db()
    with database._lock:
        conn = database.get_connection()
        cur = conn.cursor()
        query = "SELECT * FROM pricing_plans_config"
        params = []
        conds = []
        if currency:
            conds.append("currency = ?")
            params.append(currency.upper())
        if region:
            conds.append("region = ?")
            params.append(region.lower())

        if conds:
            query += " WHERE " + " AND ".join(conds)
        query += " ORDER BY id ASC"

        cur.execute(query, params)
        rows = cur.fetchall()
        res = []
        for r in rows:
            d = dict(r)
            d["features"] = json.loads(d.get("features_json") or "[]")
            d["is_recurring"] = bool(d["is_recurring"])
            d["is_lifetime"] = bool(d["is_lifetime"])
            d["recommended"] = bool(d["recommended"])
            d["checkout_enabled"] = bool(d["checkout_enabled"])
            res.append(d)
        return res


def get_db_pricing_plan(plan_id: str, currency: str, region: str) -> Optional[Dict[str, Any]]:
    """Retrieve a single plan strictly by (plan_id, currency, region)."""
    plans = get_db_pricing_plans(currency=currency, region=region)
    for p in plans:
        if p["plan_id"] == plan_id:
            return p
    return None


def update_db_pricing_plan(
    plan_id: str,
    currency: str,
    region: str,
    price: Optional[float] = None,
    checkout_enabled: Optional[bool] = None,
    recommended: Optional[bool] = None
) -> Optional[Dict[str, Any]]:
    """Update plan price or checkout availability in database without hardcoding."""
    database.init_db()
    now = _now_iso()
    with database._lock:
        conn = database.get_connection()
        with conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM pricing_plans_config
                WHERE plan_id = ? AND currency = ? AND region = ?
            """, (plan_id, currency.upper(), region.lower()))
            existing = cur.fetchone()
            if not existing:
                return None

            new_price = price if price is not None else existing["price"]
            new_checkout = int(checkout_enabled) if checkout_enabled is not None else existing["checkout_enabled"]
            new_rec = int(recommended) if recommended is not None else existing["recommended"]
            unconfigured_reason = None
            if new_price is None or new_price <= 0:
                if plan_id == "lifetime":
                    unconfigured_reason = f"Lifetime price for {region.capitalize()} ({currency.upper()}) must be configured before checkout."
                    new_checkout = 0

            cur.execute("""
                UPDATE pricing_plans_config
                SET price = ?, checkout_enabled = ?, recommended = ?, unconfigured_reason = ?, updated_at = ?
                WHERE plan_id = ? AND currency = ? AND region = ?
            """, (new_price, new_checkout, new_rec, unconfigured_reason, now, plan_id, currency.upper(), region.lower()))

            record_payment_audit_log(
                actor="admin",
                action="update_pricing",
                entity_type="plan",
                entity_id=f"{plan_id}:{currency}:{region}",
                details=json.dumps({"price": new_price, "checkout_enabled": new_checkout, "recommended": new_rec}),
                conn=conn
            )
        return get_db_pricing_plan(plan_id, currency, region)


# ---------------------------------------------------------------------------
# Payment Transactions Ledger (ACID Idempotent)
# ---------------------------------------------------------------------------

def create_payment_transaction(
    transaction_id: str,
    idempotency_key: str,
    user_id: str,
    plan_id: str,
    provider_id: str,
    payment_method_id: str,
    currency: str,
    amount: float,
    amount_subunits: int,
    status: str = "pending",
    provider_order_id: Optional[str] = None,
    billing_country: Optional[str] = "IN",
    raw_details: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Record a new payment transaction with unique idempotency key."""
    database.init_db()
    now = _now_iso()
    details_str = json.dumps(raw_details or {})

    with database._lock:
        conn = database.get_connection()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO payment_transactions (
                        transaction_id, idempotency_key, user_id, plan_id, provider_id,
                        payment_method_id, currency, amount, amount_subunits, status,
                        provider_order_id, billing_country, raw_details, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(idempotency_key) DO UPDATE SET
                        updated_at = excluded.updated_at
                """, (
                    transaction_id, idempotency_key, user_id, plan_id, provider_id,
                    payment_method_id, currency.upper(), amount, amount_subunits, status,
                    provider_order_id, billing_country, details_str, now, now
                ))

                cur.execute("SELECT * FROM payment_transactions WHERE idempotency_key = ?", (idempotency_key,))
                row = cur.fetchone()
                return dict(row) if row else {}
        except Exception as e:
            print(f"[PaymentsDB] create_payment_transaction error: {e}")
            raise


def update_payment_transaction_status(
    transaction_id: str,
    status: str,
    provider_payment_id: Optional[str] = None,
    provider_refund_id: Optional[str] = None,
    error_message: Optional[str] = None,
    raw_details: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """Update status of a transaction (e.g. successful, failed, refunded)."""
    database.init_db()
    now = _now_iso()
    details_str = json.dumps(raw_details or {}) if raw_details else None

    with database._lock:
        conn = database.get_connection()
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM payment_transactions WHERE transaction_id = ? OR provider_order_id = ?", (transaction_id, transaction_id))
            existing = cur.fetchone()
            if not existing:
                return None

            tid = existing["transaction_id"]
            cur.execute("""
                UPDATE payment_transactions
                SET status = ?,
                    provider_payment_id = COALESCE(?, provider_payment_id),
                    provider_refund_id = COALESCE(?, provider_refund_id),
                    error_message = COALESCE(?, error_message),
                    raw_details = COALESCE(?, raw_details),
                    updated_at = ?
                WHERE transaction_id = ?
            """, (status, provider_payment_id, provider_refund_id, error_message, details_str, now, tid))

            cur.execute("SELECT * FROM payment_transactions WHERE transaction_id = ?", (tid,))
            updated = cur.fetchone()
            return dict(updated) if updated else None


def get_payment_transactions(
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = None,
    user_id: Optional[str] = None,
    provider_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Filterable transaction history for user and admin inspection."""
    database.init_db()
    with database._lock:
        conn = database.get_connection()
        cur = conn.cursor()
        query = "SELECT * FROM payment_transactions"
        conds = []
        params = []
        if status:
            conds.append("status = ?")
            params.append(status)
        if user_id:
            conds.append("user_id = ?")
            params.append(user_id)
        if provider_id:
            conds.append("provider_id = ?")
            params.append(provider_id)

        if conds:
            query += " WHERE " + " AND ".join(conds)
        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cur.execute(query, params)
        rows = cur.fetchall()
        out = []
        for r in rows:
            d = dict(r)
            if d.get("raw_details"):
                try:
                    d["raw_details"] = json.loads(d["raw_details"])
                except Exception:
                    pass
            out.append(d)
        return out


def get_payment_transaction_by_id(transaction_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve transaction by ID or order ID."""
    database.init_db()
    with database._lock:
        conn = database.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM payment_transactions WHERE transaction_id = ? OR provider_order_id = ?", (transaction_id, transaction_id))
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("raw_details"):
            try:
                d["raw_details"] = json.loads(d["raw_details"])
            except Exception:
                pass
        return d


# ---------------------------------------------------------------------------
# Audit Logs
# ---------------------------------------------------------------------------

def record_payment_audit_log(
    actor: str,
    action: str,
    entity_type: str,
    entity_id: str,
    details: str,
    conn: Optional[sqlite3.Connection] = None
) -> None:
    """Record an audit log for admin operations, status changes, and webhook alerts."""
    now = _now_iso()
    try:
        if conn is not None:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO payment_audit_logs (actor, action, entity_type, entity_id, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (actor, action, entity_type, entity_id, str(details), now))
        else:
            with database._lock:
                c = database.get_connection()
                try:
                    with c:
                        cur = c.cursor()
                        cur.execute("""
                            INSERT INTO payment_audit_logs (actor, action, entity_type, entity_id, details, created_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (actor, action, entity_type, entity_id, str(details), now))
                finally:
                    c.close()
    except Exception as e:
        print(f"[PaymentsDB] Audit log error: {e}")


def get_payment_audit_logs(limit: int = 50) -> List[Dict[str, Any]]:
    """Retrieve recent payment audit logs."""
    database.init_db()
    with database._lock:
        conn = database.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM payment_audit_logs ORDER BY created_at DESC LIMIT ?", (limit,))
        return [dict(r) for r in cur.fetchall()]
