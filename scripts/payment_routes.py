"""Payment and billing REST API routes for Tweets.

Provides modular multi-currency checkout, eligible method discovery, cryptographic
verification, provider-agnostic webhooks, and secure admin configuration.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Request, Response
from pydantic import BaseModel

import database
import post
import settings
from payments.base import (
    CheckoutSessionResult,
    PaymentVerificationResult,
    RefundResult,
    WebhookResult,
)
from payments.registry import default_registry

router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------

class CheckoutSessionCreateRequest(BaseModel):
    plan_id: str
    currency: Optional[str] = "INR"
    country: Optional[str] = "IN"
    payment_method_id: Optional[str] = None
    provider_id: Optional[str] = None
    idempotency_key: Optional[str] = None


class UniversalPaymentVerifyRequest(BaseModel):
    provider_id: str
    transaction_id: Optional[str] = None
    order_id: Optional[str] = None
    payment_id: Optional[str] = None
    signature: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    plan_id: Optional[str] = None
    currency: Optional[str] = None
    amount: Optional[float] = None


class AdminProviderUpdateRequest(BaseModel):
    is_enabled: Optional[bool] = None
    priority: Optional[int] = None
    mode: Optional[str] = None


class AdminMethodUpdateRequest(BaseModel):
    is_enabled: Optional[bool] = None
    display_order: Optional[int] = None


class AdminPricingUpdateRequest(BaseModel):
    currency: str
    region: str
    price: Optional[float] = None
    checkout_enabled: Optional[bool] = None
    recommended: Optional[bool] = None


class AdminRefundRequest(BaseModel):
    amount: Optional[float] = None
    reason: Optional[str] = "Admin requested refund"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_current_username(request: Request) -> str:
    """Resolve current user handle from session token or config fallback."""
    token = request.cookies.get("tweets_session") or request.headers.get("X-Session-Token")
    if token:
        user = database.get_user_by_session_token(token)
        if user and user.get("username"):
            return user["username"]
    cfg = settings.load()
    return cfg.get("handle") or "tweets"


def _verify_admin_access(request: Request) -> bool:
    """Verify administrator permissions for configuration endpoints."""
    expected_key = os.getenv("ADMIN_API_KEY", "").strip()
    if not expected_key:
        # In developer mode without explicit ADMIN_API_KEY, allow access
        return True
    provided_key = request.headers.get("X-Admin-Key", "").strip()
    return bool(provided_key and provided_key == expected_key)


# ---------------------------------------------------------------------------
# Public Checkout & Payment Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/payments/methods")
async def get_eligible_payment_methods(
    currency: Optional[str] = "INR",
    country: Optional[str] = "IN",
    plan_id: Optional[str] = None
):
    """Retrieve only the active, eligible payment methods for currency and country."""
    curr = (currency or "INR").strip().upper()
    c_code = (country or "IN").strip().upper()

    # Query admin-enabled providers from DB
    providers_cfg = database.get_payment_providers_config()
    enabled_providers = [p["provider_id"] for p in providers_cfg if p.get("is_enabled", 1)]

    # Query admin-enabled payment methods from DB
    methods_cfg = database.get_payment_methods_config()
    enabled_methods = [m["method_id"] for m in methods_cfg if m.get("is_enabled", 1)]

    methods = default_registry.get_eligible_payment_methods(
        currency=curr,
        country_code=c_code,
        plan_id=plan_id,
        enabled_methods_filter=enabled_methods,
        enabled_providers_filter=enabled_providers,
    )

    providers_summary = [
        {
            "provider_id": p["provider_id"],
            "name": p["name"],
            "is_enabled": bool(p["is_enabled"]),
            "mode": p["mode"],
        }
        for p in providers_cfg
        if p["provider_id"] in enabled_providers
    ]

    return {
        "ok": True,
        "currency": curr,
        "country": c_code,
        "methods": methods,
        "providers": providers_summary,
    }


@router.post("/api/payments/checkout-session")
async def create_checkout_session(request: Request, req: CheckoutSessionCreateRequest):
    """Create a verified payment checkout session with the appropriate provider adapter."""
    username = _get_current_username(request)
    currency = (req.currency or "INR").strip().upper()
    country = (req.country or "IN").strip().upper()

    # Determine authoritative region
    if country == "IN":
        region = "india"
    elif country in ["US", "CA"]:
        region = "usa"
    elif country in ["DE", "FR", "IT", "ES", "NL", "IE", "BE", "AT", "PT", "FI", "GR", "EU"]:
        region = "europe"
    else:
        region = "global"

    # Query plan price strictly from database configuration
    plan_id = "six_month" if req.plan_id in ["six_month", "semiannual"] else req.plan_id
    plan = database.get_db_pricing_plan(plan_id, currency, region)
    if not plan:
        # Fallback to subscription service
        import subscription
        plan = subscription.get_plan(req.plan_id, country_code=country)

    if not plan:
        raise HTTPException(status_code=400, detail=f"Invalid plan ID: {req.plan_id}")

    if not plan.get("checkout_enabled") or plan.get("price") is None:
        reason = plan.get("unconfigured_reason") or f"Checkout is disabled for plan '{plan.get('name')}' in {region} ({currency})."
        raise HTTPException(status_code=400, detail=reason)

    if plan["price"] <= 0:
        raise HTTPException(status_code=400, detail="Cannot initiate checkout for free trial. Use /api/subscription/start-trial.")

    # Determine provider
    provider_id = req.provider_id
    if not provider_id:
        if req.payment_method_id == "paypal":
            provider_id = "paypal"
        elif req.payment_method_id in ["apple_pay", "sepa_debit"]:
            provider_id = "stripe"
        elif currency == "INR" or country == "IN":
            provider_id = "razorpay"
        elif currency in ["USD", "EUR"]:
            provider_id = "stripe"
        else:
            provider_id = "stripe"

    # Check if provider is enabled in database
    providers_cfg = {p["provider_id"]: p for p in database.get_payment_providers_config()}
    prov_entry = providers_cfg.get(provider_id)
    if prov_entry and not prov_entry.get("is_enabled", 1):
        raise HTTPException(status_code=400, detail=f"Payment provider '{provider_id}' is currently disabled by administrator.")

    # Idempotency key
    idempotency_key = req.idempotency_key or hashlib.sha256(
        f"{username}:{plan['plan_id'] if 'plan_id' in plan else plan['id']}:{currency}:{int(time.time() / 120)}".encode()
    ).hexdigest()

    try:
        session_result: CheckoutSessionResult = default_registry.create_checkout_session(
            provider_id=provider_id,
            user_id=username,
            plan=plan,
            currency=currency,
            country_code=country,
            payment_method_id=req.payment_method_id,
            idempotency_key=idempotency_key,
        )

        if not session_result.success:
            raise HTTPException(status_code=400, detail=session_result.error_message or "Failed to initiate checkout")

        # Record transaction in database ledger
        transaction = database.create_payment_transaction(
            transaction_id=session_result.session_id,
            idempotency_key=idempotency_key,
            user_id=username,
            plan_id=plan.get("plan_id") or plan.get("id"),
            provider_id=provider_id,
            payment_method_id=req.payment_method_id or "auto",
            currency=currency,
            amount=session_result.amount,
            amount_subunits=session_result.amount_subunits,
            status="pending",
            provider_order_id=session_result.provider_order_id,
            billing_country=country,
            raw_details=session_result.client_params,
        )

        return {
            "ok": True,
            "session_id": session_result.session_id,
            "provider_id": provider_id,
            "provider_order_id": session_result.provider_order_id,
            "checkout_mode": session_result.checkout_mode,
            "amount": session_result.amount,
            "currency": session_result.currency,
            "client_params": session_result.client_params,
            "redirect_url": session_result.redirect_url,
            "transaction": transaction,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error initiating payment checkout: {e}")


@router.post("/api/payments/verify")
async def verify_universal_payment(request: Request, req: UniversalPaymentVerifyRequest):
    """Cryptographically verify payment outcome on backend and activate subscription idempotently."""
    username = _get_current_username(request)

    payload = dict(req.payload or {})
    payload["order_id"] = req.order_id or payload.get("order_id", "")
    payload["payment_id"] = req.payment_id or payload.get("payment_id", "")
    payload["user_id"] = username
    payload["plan_id"] = req.plan_id or payload.get("plan_id", "")
    payload["currency"] = req.currency or payload.get("currency", "INR")
    if req.amount:
        payload["amount"] = req.amount

    verification: PaymentVerificationResult = default_registry.verify_payment(
        provider_id=req.provider_id,
        payload=payload,
        signature=req.signature,
    )

    if not verification.success or verification.status != "successful":
        # Mark transaction failed in database
        database.update_payment_transaction_status(
            transaction_id=req.transaction_id or req.order_id or "",
            status="failed",
            error_message=verification.error_message or "Payment verification rejected",
            raw_details=verification.raw_details,
        )
        raise HTTPException(
            status_code=400,
            detail=verification.error_message or "Cryptographic payment verification failed."
        )

    # 1. Update transaction status in unified ledger
    tx_id = req.transaction_id or req.order_id or verification.transaction_id
    updated_tx = database.update_payment_transaction_status(
        transaction_id=tx_id,
        status="successful",
        provider_payment_id=verification.provider_payment_id,
        raw_details=verification.raw_details,
    )

    plan_id = req.plan_id or verification.plan_id or "monthly"
    currency = req.currency or verification.currency or "INR"

    # 2. Idempotently activate subscription in database
    sub = database.activate_subscription(
        user_id=username,
        plan_id=plan_id,
        order_id=verification.provider_order_id,
        payment_id=verification.provider_payment_id,
        amount=verification.amount,
        amount_subunits=int(round(verification.amount * 100)),
        currency=currency,
        billing_country="IN" if currency == "INR" else "US",
        raw_details=verification.raw_details,
    )

    # 3. Update post state
    st = post.load_state()
    st["is_pro"] = True
    st["plan_id"] = plan_id
    st["subscription_status"] = "active"
    post.save_state(st)

    # 4. Audit log
    database.record_payment_audit_log(
        actor=username,
        action="payment_verified_and_activated",
        entity_type="transaction",
        entity_id=tx_id,
        details=json.dumps({"provider": req.provider_id, "amount": verification.amount, "currency": currency})
    )

    return {
        "ok": True,
        "status": "successful",
        "provider_id": req.provider_id,
        "transaction_id": tx_id,
        "subscription": sub,
        "message": f"Successfully activated subscription ({plan_id}) via {req.provider_id.capitalize()}."
    }


@router.post("/api/payments/webhook/{provider_id}")
async def handle_provider_webhook(provider_id: str, request: Request):
    """Universal webhook endpoint for asynchronous provider events with signature verification."""
    raw_body = await request.body()
    headers_dict = dict(request.headers)

    result: WebhookResult = default_registry.handle_webhook(
        provider_id=provider_id,
        payload=raw_body,
        headers=headers_dict,
    )

    if not result.handled or result.status == "failed":
        database.record_payment_audit_log(
            actor="webhook",
            action="webhook_verification_failed",
            entity_type="webhook",
            entity_id=provider_id,
            details=result.error_message or "Failed verification",
        )
        raise HTTPException(status_code=400, detail=result.error_message or "Webhook verification failed")

    # If payment was captured successfully, update transaction and subscription
    if result.status == "successful" and (result.order_id or result.payment_id):
        tid = result.order_id or result.payment_id
        database.update_payment_transaction_status(
            transaction_id=tid,
            status="successful",
            provider_payment_id=result.payment_id,
            raw_details=result.raw_event,
        )

        if result.user_id and result.plan_id:
            database.activate_subscription(
                user_id=result.user_id,
                plan_id=result.plan_id,
                order_id=result.order_id or "",
                payment_id=result.payment_id or "",
                amount=result.amount or 0.0,
                amount_subunits=int(round((result.amount or 0.0) * 100)),
                currency=result.currency or "INR",
                raw_details=result.raw_event,
            )

    return {"ok": True, "handled": True, "event_type": result.event_type, "status": result.status}


# ---------------------------------------------------------------------------
# Secure Admin Configuration Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/admin/payments/config")
async def get_admin_payment_config(request: Request):
    """Retrieve full billing configuration for authorized administrators."""
    if not _verify_admin_access(request):
        raise HTTPException(status_code=403, detail="Unauthorized access to billing configuration.")

    providers = database.get_payment_providers_config()
    methods = database.get_payment_methods_config()
    plans = database.get_db_pricing_plans()
    audit_logs = database.get_payment_audit_logs(limit=25)

    # Attach live credentials status without exposing secrets
    for p in providers:
        adapter = default_registry.get_provider(p["provider_id"])
        p["is_configured"] = adapter.is_configured() if adapter else False
        p["supported_currencies"] = [c.strip() for c in p["supported_currencies"].split(",")]
        p["supported_regions"] = [r.strip() for r in p["supported_regions"].split(",")]

    return {
        "ok": True,
        "providers": providers,
        "methods": methods,
        "plans": plans,
        "audit_logs": audit_logs,
    }


@router.post("/api/admin/payments/providers/{provider_id}")
async def update_admin_provider_config(provider_id: str, req: AdminProviderUpdateRequest, request: Request):
    """Enable/disable payment provider or update priority/mode."""
    if not _verify_admin_access(request):
        raise HTTPException(status_code=403, detail="Unauthorized.")

    updated = database.update_payment_provider_config(
        provider_id=provider_id,
        is_enabled=req.is_enabled,
        priority=req.priority,
        mode=req.mode,
    )
    if not updated:
        raise HTTPException(status_code=404, detail=f"Provider '{provider_id}' not found.")

    return {"ok": True, "provider": updated}


@router.post("/api/admin/payments/methods/{method_id}")
async def update_admin_method_config(method_id: str, req: AdminMethodUpdateRequest, request: Request):
    """Enable or disable individual payment method."""
    if not _verify_admin_access(request):
        raise HTTPException(status_code=403, detail="Unauthorized.")

    updated = database.update_payment_method_config(
        method_id=method_id,
        is_enabled=req.is_enabled,
        display_order=req.display_order,
    )
    if not updated:
        raise HTTPException(status_code=404, detail=f"Payment method '{method_id}' not found.")

    return {"ok": True, "method": updated}


@router.post("/api/admin/payments/plans/{plan_id}/pricing")
async def update_admin_pricing_plan(plan_id: str, req: AdminPricingUpdateRequest, request: Request):
    """Update database-backed plan pricing for a given currency and region."""
    if not _verify_admin_access(request):
        raise HTTPException(status_code=403, detail="Unauthorized.")

    updated = database.update_db_pricing_plan(
        plan_id=plan_id,
        currency=req.currency,
        region=req.region,
        price=req.price,
        checkout_enabled=req.checkout_enabled,
        recommended=req.recommended,
    )
    if not updated:
        raise HTTPException(status_code=404, detail=f"Plan '{plan_id}' for {req.region} ({req.currency}) not found.")

    return {"ok": True, "plan": updated}


@router.get("/api/admin/payments/transactions")
async def get_admin_payment_transactions(
    request: Request,
    status: Optional[str] = None,
    user_id: Optional[str] = None,
    provider_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """Review payment status, transaction history, and failed webhook attempts."""
    if not _verify_admin_access(request):
        raise HTTPException(status_code=403, detail="Unauthorized.")

    txs = database.get_payment_transactions(
        limit=limit,
        offset=offset,
        status=status,
        user_id=user_id,
        provider_id=provider_id,
    )
    return {"ok": True, "total": len(txs), "transactions": txs}


@router.post("/api/admin/payments/transactions/{transaction_id}/refund")
async def refund_admin_payment_transaction(transaction_id: str, req: AdminRefundRequest, request: Request):
    """Process a refund via the provider adapter and update transaction ledger."""
    if not _verify_admin_access(request):
        raise HTTPException(status_code=403, detail="Unauthorized.")

    tx = database.get_payment_transaction_by_id(transaction_id)
    if not tx:
        raise HTTPException(status_code=404, detail=f"Transaction '{transaction_id}' not found.")

    if tx.get("status") != "successful":
        raise HTTPException(status_code=400, detail=f"Cannot refund transaction with status '{tx.get('status')}'.")

    provider_id = tx.get("provider_id", "razorpay")
    payment_id = tx.get("provider_payment_id") or tx.get("provider_order_id")

    refund: RefundResult = default_registry.refund_payment(
        provider_id=provider_id,
        payment_id=payment_id,
        amount=req.amount or tx.get("amount"),
        currency=tx.get("currency"),
        reason=req.reason,
    )

    if not refund.success:
        raise HTTPException(status_code=400, detail=refund.error_message or "Refund request rejected by gateway.")

    # Update transaction ledger
    updated_tx = database.update_payment_transaction_status(
        transaction_id=tx["transaction_id"],
        status="refunded",
        provider_refund_id=refund.refund_id,
        raw_details=refund.raw_details,
    )

    database.record_payment_audit_log(
        actor="admin",
        action="payment_refunded",
        entity_type="transaction",
        entity_id=tx["transaction_id"],
        details=json.dumps({"refund_id": refund.refund_id, "amount": refund.amount_refunded, "reason": req.reason})
    )

    return {
        "ok": True,
        "refund_id": refund.refund_id,
        "transaction": updated_tx,
        "message": f"Refund of {tx.get('currency')} {refund.amount_refunded} processed successfully."
    }
