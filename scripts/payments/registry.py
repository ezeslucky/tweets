"""Payment provider registry and dynamic resolution engine.

Coordinates all registered payment provider adapters, queries eligible payment methods
by currency, region, and admin-enabled flags, and routes checkout sessions, payments,
and webhooks to the appropriate provider adapter.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Type

from payments.base import (
    BasePaymentProvider,
    CheckoutSessionResult,
    PaymentMethodInfo,
    PaymentVerificationResult,
    RefundResult,
    WebhookResult,
)
from payments.adapters.razorpay_adapter import RazorpayAdapter
from payments.adapters.stripe_adapter import StripeAdapter
from payments.adapters.paypal_adapter import PayPalAdapter


class PaymentProviderRegistry:
    """Central registry of modular payment providers."""

    def __init__(self):
        self._providers: Dict[str, BasePaymentProvider] = {}
        self._register_default_providers()

    def _register_default_providers(self) -> None:
        """Register the built-in provider adapters."""
        self.register(RazorpayAdapter())
        self.register(StripeAdapter())
        self.register(PayPalAdapter())

    def register(self, provider: BasePaymentProvider) -> None:
        """Register a new or custom payment provider adapter."""
        self._providers[provider.provider_id] = provider

    def unregister(self, provider_id: str) -> Optional[BasePaymentProvider]:
        """Remove a provider from the registry."""
        return self._providers.pop(provider_id, None)

    def get_provider(self, provider_id: str) -> Optional[BasePaymentProvider]:
        """Retrieve a specific provider by its slug ID."""
        return self._providers.get(provider_id)

    def list_providers(self) -> List[Dict[str, Any]]:
        """List all registered providers with their configuration and health status."""
        out = []
        for pid, p in self._providers.items():
            out.append({
                "provider_id": pid,
                "name": p.name,
                "is_configured": p.is_configured(),
                "supported_currencies": p.get_supported_currencies(),
            })
        return out

    def get_eligible_payment_methods(
        self,
        currency: str,
        country_code: str,
        plan_id: Optional[str] = None,
        enabled_methods_filter: Optional[List[str]] = None,
        enabled_providers_filter: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Return all active, eligible payment methods for the given currency and user country.
        
        Only displays methods that are enabled, supported for the user's region and currency,
        and available for the transaction.
        """
        curr = (currency or "INR").strip().upper()
        country = (country_code or "IN").strip().upper()

        # Determine region string
        if country == "IN":
            region = "india"
        elif country in ["US", "CA"]:
            region = "usa"
        elif country in ["DE", "FR", "IT", "ES", "NL", "IE", "BE", "AT", "PT", "FI", "GR", "EU"]:
            region = "europe"
        else:
            region = "global"

        seen_method_ids = set()
        eligible_methods: List[Dict[str, Any]] = []

        # Provider priority order based on currency & region
        # For INR / India: Razorpay -> Stripe
        # For USD / USA: Stripe -> PayPal -> Razorpay
        # For EUR / Europe: Stripe -> PayPal -> Razorpay
        if curr == "INR" or region == "india":
            base_order = ["razorpay", "stripe", "paypal"]
        elif curr == "EUR" or region == "europe":
            base_order = ["stripe", "paypal", "razorpay"]
        else:
            base_order = ["stripe", "paypal", "razorpay"]

        # Include base order then any custom registered providers
        provider_order = [p for p in base_order if p in self._providers]
        for pid in self._providers:
            if pid not in provider_order:
                provider_order.append(pid)

        for pid in provider_order:
            if enabled_providers_filter is not None and pid not in enabled_providers_filter:
                continue

            provider = self._providers.get(pid)
            if not provider:
                continue

            if curr not in provider.get_supported_currencies():
                continue

            methods = provider.get_supported_payment_methods(curr, region)
            for m in methods:
                if enabled_methods_filter is not None and m.id not in enabled_methods_filter:
                    continue

                # Deduplicate if multiple providers offer the same method (e.g. Card via Stripe vs Razorpay)
                # Keep the higher priority provider
                method_key = f"{m.id}_{curr}"
                if method_key in seen_method_ids:
                    continue
                seen_method_ids.add(method_key)

                eligible_methods.append({
                    "id": m.id,
                    "name": m.name,
                    "type": m.type,
                    "icon": m.icon,
                    "provider_id": provider.provider_id,
                    "provider_name": provider.name,
                    "supported_currencies": m.supported_currencies,
                    "supported_regions": m.supported_regions,
                    "is_recurring_supported": m.is_recurring_supported,
                    "description": m.description,
                    "badge": m.badge,
                })

        return eligible_methods

    def create_checkout_session(
        self,
        provider_id: str,
        user_id: str,
        plan: Dict[str, Any],
        currency: str,
        country_code: str,
        payment_method_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        **kwargs
    ) -> CheckoutSessionResult:
        """Route checkout session creation to the designated provider adapter."""
        provider = self.get_provider(provider_id)
        if not provider:
            return CheckoutSessionResult(
                success=False,
                session_id="",
                provider_id=provider_id,
                provider_order_id="",
                amount=0,
                amount_subunits=0,
                currency=currency,
                checkout_mode="modal",
                error_message=f"Payment provider '{provider_id}' is not registered or supported.",
            )

        return provider.create_checkout_session(
            user_id=user_id,
            plan=plan,
            currency=currency,
            country_code=country_code,
            payment_method_id=payment_method_id,
            idempotency_key=idempotency_key,
            **kwargs
        )

    def verify_payment(
        self,
        provider_id: str,
        payload: Dict[str, Any],
        signature: Optional[str] = None
    ) -> PaymentVerificationResult:
        """Route cryptographic payment verification to the appropriate provider adapter."""
        provider = self.get_provider(provider_id)
        if not provider:
            return PaymentVerificationResult(
                success=False,
                status="failed",
                transaction_id="",
                provider_id=provider_id,
                provider_order_id="",
                provider_payment_id="",
                amount=0,
                currency="INR",
                plan_id="",
                user_id="",
                error_message=f"Unknown provider '{provider_id}'",
            )

        return provider.verify_payment(payload=payload, signature=signature)

    def refund_payment(
        self,
        provider_id: str,
        payment_id: str,
        amount: Optional[float] = None,
        currency: Optional[str] = None,
        reason: Optional[str] = None
    ) -> RefundResult:
        """Route refund to the designated provider adapter."""
        provider = self.get_provider(provider_id)
        if not provider:
            return RefundResult(
                success=False,
                status="failed",
                refund_id="",
                transaction_id=payment_id,
                provider_id=provider_id,
                amount_refunded=0.0,
                currency=currency or "USD",
                error_message=f"Unknown provider '{provider_id}'",
            )

        return provider.refund_payment(
            payment_id=payment_id,
            amount=amount,
            currency=currency,
            reason=reason
        )

    def handle_webhook(
        self,
        provider_id: str,
        payload: bytes,
        headers: Dict[str, str]
    ) -> WebhookResult:
        """Route incoming gateway webhook to the designated provider adapter."""
        provider = self.get_provider(provider_id)
        if not provider:
            return WebhookResult(
                handled=False,
                event_type="unknown",
                provider_id=provider_id,
                status="failed",
                error_message=f"Webhook received for unknown provider '{provider_id}'",
            )

        return provider.handle_webhook(payload=payload, headers=headers)


# Global singleton registry instance
default_registry = PaymentProviderRegistry()
