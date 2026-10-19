"""Subscription plans, pricing rules, and server-side configuration.

All plan prices, durations, and exchange rates are strictly managed here.
Never trust client-supplied prices or durations.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

# Exchange rate: 1 USD to INR (configurable via environment variable)
DEFAULT_USD_TO_INR_RATE = 85.0

def get_exchange_rate() -> float:
    try:
        rate = float(os.getenv("USD_TO_INR_RATE", str(DEFAULT_USD_TO_INR_RATE)))
        return max(1.0, rate)
    except Exception:
        return DEFAULT_USD_TO_INR_RATE

# Trusted Plan Catalog
PLANS: Dict[str, Dict[str, Any]] = {
    "trial": {
        "id": "trial",
        "name": "7-Day Free Trial",
        "price_usd": 0,
        "duration_days": 7,
        "billing_period": "7 days",
        "description": "Full access to autonomous research, scheduling, and verified posting pipeline for 7 days.",
        "badge": "Free Trial",
        "features": [
            "Autonomous X posting pipeline",
            "Daily news research (HN, Reddit, RSS)",
            "Automated slot planning with jitter",
            "Browser CDP profile verification",
            "Full mobile companion dashboard"
        ],
        "is_recurring": False,
        "is_lifetime": False,
        "recommended": False,
    },
    "monthly": {
        "id": "monthly",
        "name": "Monthly Pro",
        "price_usd": 225,
        "duration_days": 30,
        "billing_period": "1 month",
        "description": "Full autonomous agent power billed monthly. Cancel anytime before renewal.",
        "badge": "Flexible",
        "features": [
            "Unlimited autonomous posts",
            "Multi-source news swipe collection",
            "Real browser CDP composition",
            "Live tweet simulator & character ring",
            "Direct priority technical updates"
        ],
        "is_recurring": True,
        "is_lifetime": False,
        "recommended": False,
    },
    "semiannual": {
        "id": "semiannual",
        "name": "6-Month Growth",
        "price_usd": 400,
        "duration_days": 180,
        "billing_period": "6 months",
        "description": "Substantial savings for consistent growth (~$66/month equivalent).",
        "badge": "Save 70%",
        "features": [
            "All Monthly Pro features included",
            "180-day continuous posting gate",
            "Voice profile consistency tuner",
            "Advanced research filtering & sort",
            "Priority companion support"
        ],
        "is_recurring": True,
        "is_lifetime": False,
        "recommended": False,
    },
    "annual": {
        "id": "annual",
        "name": "Annual Scale",
        "price_usd": 500,
        "duration_days": 365,
        "billing_period": "1 year",
        "description": "Best value for serious builders and tech founders (~$41/month equivalent).",
        "badge": "Recommended",
        "features": [
            "All Growth features included",
            "365-day autonomous cron execution",
            "Custom RSS & subreddit hooks",
            "Early access to new agent modes",
            "Direct founder support channel"
        ],
        "is_recurring": True,
        "is_lifetime": False,
        "recommended": True,
    },
    "lifetime": {
        "id": "lifetime",
        "name": "Lifetime Access",
        "price_usd": 2000,
        "duration_days": None,
        "billing_period": "One-time payment",
        "description": "Permanent lifetime access to autonomous posting pipeline. Zero recurring charges forever.",
        "badge": "VIP Lifetime",
        "features": [
            "Lifetime access — never expires",
            "Unlimited posts & updates forever",
            "All future features & upgrades included",
            "VIP Pro badge in ledger & dashboard",
            "Zero recurring subscription fees"
        ],
        "is_recurring": False,
        "is_lifetime": True,
        "recommended": False,
    },
}

def get_plan(plan_id: str) -> Optional[Dict[str, Any]]:
    return PLANS.get(plan_id)

def calculate_inr_amount(usd_price: float, rate: Optional[float] = None) -> Tuple[float, int]:
    """Calculate INR amount and paise (amount * 100) using trusted rate."""
    current_rate = rate or get_exchange_rate()
    amount_inr = round(usd_price * current_rate, 2)
    amount_paise = int(round(amount_inr * 100))
    return amount_inr, amount_paise

def get_public_plans() -> Dict[str, Any]:
    """Return sanitized plan list with current exchange rate and calculated INR pricing."""
    rate = get_exchange_rate()
    catalog = []
    for pid, plan in PLANS.items():
        usd = plan["price_usd"]
        inr_amt, paise = calculate_inr_amount(usd, rate)
        catalog.append({
            "id": plan["id"],
            "name": plan["name"],
            "price_usd": usd,
            "price_inr": inr_amt,
            "amount_paise": paise,
            "duration_days": plan["duration_days"],
            "billing_period": plan["billing_period"],
            "description": plan["description"],
            "badge": plan["badge"],
            "features": plan["features"],
            "is_recurring": plan["is_recurring"],
            "is_lifetime": plan["is_lifetime"],
            "recommended": plan["recommended"],
        })
    return {
        "exchange_rate": rate,
        "currency": "INR",
        "plans": catalog,
    }
