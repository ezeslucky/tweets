"""Regional subscription plans, multi-currency pricing, and authoritative server-side rules.

Supports:
- India: INR (₹) - Monthly ₹250, 6-Month ₹500, Annual ₹999, Lifetime (configured separately)
- USA: USD ($) - Monthly $225, 6-Month $400, Annual $500, Lifetime $2,000
- Europe: EUR (€) - Monthly €225, 6-Month €400, Annual €500, Lifetime €2,000

All prices are strictly defined and validated server-side.
Never trust client-supplied prices, currencies, or durations.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

# Supported regional country mappings
COUNTRY_TO_REGION: Dict[str, str] = {
    "IN": "india",
    "US": "usa",
    "DE": "europe",
    "FR": "europe",
    "IT": "europe",
    "ES": "europe",
    "NL": "europe",
    "IE": "europe",
    "BE": "europe",
    "AT": "europe",
    "PT": "europe",
    "FI": "europe",
    "GR": "europe",
    "EU": "europe",
}

COUNTRIES: List[Dict[str, str]] = [
    {"code": "IN", "name": "India", "region": "india", "currency": "INR", "symbol": "₹", "flag": "🇮🇳"},
    {"code": "US", "name": "United States", "region": "usa", "currency": "USD", "symbol": "$", "flag": "🇺🇸"},
    {"code": "DE", "name": "Germany", "region": "europe", "currency": "EUR", "symbol": "€", "flag": "🇩🇪"},
    {"code": "FR", "name": "France", "region": "europe", "currency": "EUR", "symbol": "€", "flag": "🇫🇷"},
    {"code": "IT", "name": "Italy", "region": "europe", "currency": "EUR", "symbol": "€", "flag": "🇮🇹"},
    {"code": "ES", "name": "Spain", "region": "europe", "currency": "EUR", "symbol": "€", "flag": "🇪🇸"},
    {"code": "NL", "name": "Netherlands", "region": "europe", "currency": "EUR", "symbol": "€", "flag": "🇳🇱"},
    {"code": "IE", "name": "Ireland", "region": "europe", "currency": "EUR", "symbol": "€", "flag": "🇮🇪"},
    {"code": "EU", "name": "Other European Union", "region": "europe", "currency": "EUR", "symbol": "€", "flag": "🇪🇺"},
]

def get_india_lifetime_price() -> Optional[float]:
    """Retrieve separately configured lifetime price for India.
    
    If not specified in environment (INDIA_LIFETIME_INR), returns None.
    Do not invent a price or enable checkout until it is configured.
    """
    raw = os.getenv("INDIA_LIFETIME_INR", "").strip()
    if not raw:
        return None
    try:
        val = float(raw)
        return val if val > 0 else None
    except ValueError:
        return None

# Authoritative Regional Pricing Matrix
BASE_REGIONAL_CONFIG: Dict[str, Dict[str, Any]] = {
    "india": {
        "region_id": "india",
        "name": "India",
        "currency": "INR",
        "currency_symbol": "₹",
        "subunit_multiplier": 100,  # 1 INR = 100 paise
        "plans": {
            "trial": {
                "id": "trial",
                "name": "7-Day Free Trial",
                "price": 0,
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
                "checkout_enabled": True,
            },
            "monthly": {
                "id": "monthly",
                "name": "Monthly Plan",
                "price": 250,  # Exact ₹250
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
                "checkout_enabled": True,
            },
            "semiannual": {
                "id": "semiannual",
                "name": "6-Month Plan",
                "price": 500,  # Exact ₹500
                "duration_days": 180,
                "billing_period": "6 months",
                "description": "Substantial savings for consistent growth (~₹83/month equivalent).",
                "badge": "Save 67%",
                "features": [
                    "All Monthly features included",
                    "180-day continuous posting gate",
                    "Voice profile consistency tuner",
                    "Advanced research filtering & sort",
                    "Priority companion support"
                ],
                "is_recurring": True,
                "is_lifetime": False,
                "recommended": False,
                "checkout_enabled": True,
            },
            "annual": {
                "id": "annual",
                "name": "Annual Plan",
                "price": 999,  # Exact ₹999
                "duration_days": 365,
                "billing_period": "1 year",
                "description": "Best value for serious builders and founders (~₹83/month equivalent).",
                "badge": "Recommended",
                "features": [
                    "All 6-Month features included",
                    "365-day autonomous cron execution",
                    "Custom RSS & subreddit hooks",
                    "Early access to new agent modes",
                    "Direct founder support channel"
                ],
                "is_recurring": True,
                "is_lifetime": False,
                "recommended": True,
                "checkout_enabled": True,
            },
            "lifetime": {
                "id": "lifetime",
                "name": "Lifetime Access",
                "price": None,  # Separate configuration required
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
                "checkout_enabled": False,
                "unconfigured_reason": "Lifetime access price for India has not been configured. Please select Monthly (₹250), 6-Month (₹500), or Annual (₹999) plan."
            },
        }
    },
    "usa": {
        "region_id": "usa",
        "name": "United States",
        "currency": "USD",
        "currency_symbol": "$",
        "subunit_multiplier": 100,  # 1 USD = 100 cents
        "plans": {
            "trial": {
                "id": "trial",
                "name": "7-Day Free Trial",
                "price": 0,
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
                "checkout_enabled": True,
            },
            "monthly": {
                "id": "monthly",
                "name": "Monthly Plan",
                "price": 225,  # $225
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
                "checkout_enabled": True,
            },
            "semiannual": {
                "id": "semiannual",
                "name": "6-Month Plan",
                "price": 400,  # $400
                "duration_days": 180,
                "billing_period": "6 months",
                "description": "Substantial savings for 6 months of autonomous growth (~$66/month equivalent).",
                "badge": "Save 70%",
                "features": [
                    "All Monthly features included",
                    "180-day continuous posting gate",
                    "Voice profile consistency tuner",
                    "Advanced research filtering & sort",
                    "Priority companion support"
                ],
                "is_recurring": True,
                "is_lifetime": False,
                "recommended": False,
                "checkout_enabled": True,
            },
            "annual": {
                "id": "annual",
                "name": "Annual Plan",
                "price": 500,  # $500
                "duration_days": 365,
                "billing_period": "1 year",
                "description": "Best value for serious builders and founders (~$41/month equivalent).",
                "badge": "Recommended",
                "features": [
                    "All 6-Month features included",
                    "365-day autonomous cron execution",
                    "Custom RSS & subreddit hooks",
                    "Early access to new agent modes",
                    "Direct founder support channel"
                ],
                "is_recurring": True,
                "is_lifetime": False,
                "recommended": True,
                "checkout_enabled": True,
            },
            "lifetime": {
                "id": "lifetime",
                "name": "Lifetime Access",
                "price": 2000,  # $2,000
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
                "checkout_enabled": True,
            },
        }
    },
    "europe": {
        "region_id": "europe",
        "name": "Europe",
        "currency": "EUR",
        "currency_symbol": "€",
        "subunit_multiplier": 100,  # 1 EUR = 100 cents
        "plans": {
            "trial": {
                "id": "trial",
                "name": "7-Day Free Trial",
                "price": 0,
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
                "checkout_enabled": True,
            },
            "monthly": {
                "id": "monthly",
                "name": "Monthly Plan",
                "price": 225,  # €225
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
                "checkout_enabled": True,
            },
            "semiannual": {
                "id": "semiannual",
                "name": "6-Month Plan",
                "price": 400,  # €400
                "duration_days": 180,
                "billing_period": "6 months",
                "description": "Substantial savings for 6 months of autonomous growth (~€66/month equivalent).",
                "badge": "Save 70%",
                "features": [
                    "All Monthly features included",
                    "180-day continuous posting gate",
                    "Voice profile consistency tuner",
                    "Advanced research filtering & sort",
                    "Priority companion support"
                ],
                "is_recurring": True,
                "is_lifetime": False,
                "recommended": False,
                "checkout_enabled": True,
            },
            "annual": {
                "id": "annual",
                "name": "Annual Plan",
                "price": 500,  # €500
                "duration_days": 365,
                "billing_period": "1 year",
                "description": "Best value for serious builders and founders (~€41/month equivalent).",
                "badge": "Recommended",
                "features": [
                    "All 6-Month features included",
                    "365-day autonomous cron execution",
                    "Custom RSS & subreddit hooks",
                    "Early access to new agent modes",
                    "Direct founder support channel"
                ],
                "is_recurring": True,
                "is_lifetime": False,
                "recommended": True,
                "checkout_enabled": True,
            },
            "lifetime": {
                "id": "lifetime",
                "name": "Lifetime Access",
                "price": 2000,  # €2,000
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
                "checkout_enabled": True,
            },
        }
    }
}

def resolve_region(country_code: Optional[str] = None) -> str:
    """Resolve regional configuration from country code. Defaults to 'india' (IN)."""
    if not country_code:
        return "india"
    code = country_code.strip().upper()
    return COUNTRY_TO_REGION.get(code, "usa")  # Other countries map to USD

def get_region_config(region_id: str) -> Dict[str, Any]:
    return BASE_REGIONAL_CONFIG.get(region_id, BASE_REGIONAL_CONFIG["india"])

def get_plan(plan_id: str, country_code: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Retrieve trusted plan metadata for a given country."""
    region_id = resolve_region(country_code)
    region = get_region_config(region_id)
    currency = region["currency"]

    # Check database-backed configuration first
    try:
        import database
        db_id = "six_month" if plan_id in ["six_month", "semiannual"] else plan_id
        db_p = database.get_db_pricing_plan(db_id, currency, region_id)
        if db_p:
            plan = dict(db_p)
            plan["id"] = plan_id
            plan["currency"] = currency
            plan["currency_symbol"] = region["currency_symbol"]
            return plan
    except Exception:
        pass

    plan_template = region["plans"].get(plan_id)
    if not plan_template:
        return None

    # Deep copy plan dict to prevent mutating template
    plan = dict(plan_template)
    plan["currency"] = region["currency"]
    plan["currency_symbol"] = region["currency_symbol"]

    # Special handling for India Lifetime price
    if region_id == "india" and plan_id == "lifetime":
        india_lifetime_price = get_india_lifetime_price()
        if india_lifetime_price is not None:
            plan["price"] = india_lifetime_price
            plan["checkout_enabled"] = True
            plan.pop("unconfigured_reason", None)
        else:
            plan["price"] = None
            plan["checkout_enabled"] = False

    return plan

def get_catalog_for_country(country_code: Optional[str] = None) -> Dict[str, Any]:
    """Return sanitized, authoritative plan list and currency settings for a specific country."""
    effective_code = (country_code or "IN").strip().upper()
    region_id = resolve_region(effective_code)
    region = get_region_config(region_id)

    plans_list = []
    for pid in ["trial", "monthly", "semiannual", "annual", "lifetime"]:
        p = get_plan(pid, effective_code)
        if not p:
            continue
        
        # Calculate amount in subunits (paise for INR, cents for USD/EUR)
        if p["price"] is not None and p["price"] > 0:
            amount_subunits = int(round(p["price"] * region["subunit_multiplier"]))
            formatted_price = f"{region['currency_symbol']}{p['price']:,}"
        elif p["price"] == 0:
            amount_subunits = 0
            formatted_price = f"{region['currency_symbol']}0"
        else:
            amount_subunits = 0
            formatted_price = "Configure separately"

        plans_list.append({
            "id": p["id"],
            "name": p["name"],
            "price": p["price"],
            "formatted_price": formatted_price,
            "currency": region["currency"],
            "currency_symbol": region["currency_symbol"],
            "amount_subunits": amount_subunits,
            "duration_days": p["duration_days"],
            "billing_period": p["billing_period"],
            "description": p["description"],
            "badge": p["badge"],
            "features": p["features"],
            "is_recurring": p["is_recurring"],
            "is_lifetime": p["is_lifetime"],
            "recommended": p["recommended"],
            "checkout_enabled": p["checkout_enabled"],
            "unconfigured_reason": p.get("unconfigured_reason"),
        })

    return {
        "country": effective_code,
        "region": region_id,
        "currency": region["currency"],
        "currency_symbol": region["currency_symbol"],
        "countries": COUNTRIES,
        "plans": plans_list,
    }
