"""Common base interface and data structures for modular payment providers.

Every payment provider (Razorpay, Stripe, PayPal, etc.) implements this interface
as an independent adapter. Provider-specific logic is isolated from subscription
management and frontend controllers.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class PaymentMethodInfo:
    """Metadata describing a supported payment method."""
    id: str                                  # e.g. 'upi', 'card', 'netbanking', 'wallet', 'paypal', 'apple_pay'
    name: str                                # e.g. 'UPI (GPay, PhonePe, Paytm)'
    type: str                                # e.g. 'instant', 'card', 'wallet', 'bank'
    icon: str                                # Lucide icon name or SVG descriptor
    supported_currencies: List[str]          # e.g. ['INR'], ['USD', 'EUR'], etc.
    supported_regions: List[str]             # e.g. ['IN'], ['US', 'EU', 'GLOBAL']
    is_recurring_supported: bool = True     # Whether recurring subscriptions are supported
    description: str = ""
    badge: Optional[str] = None              # e.g. 'Instant', 'Zero Foreign Fees', 'Recommended'


@dataclass
class CheckoutSessionResult:
    """Standardized response when creating a checkout session or order."""
    success: bool
    session_id: str                          # Internal transaction / order reference
    provider_id: str                         # 'razorpay', 'stripe', 'paypal'
    provider_order_id: str                   # Upstream gateway order/session ID
    amount: float                            # Nominal amount (e.g. 250.00)
    amount_subunits: int                     # Subunits (e.g. 25000 paise/cents)
    currency: str                            # 'INR', 'USD', 'EUR'
    checkout_mode: str                       # 'modal', 'redirect', 'sdk'
    client_params: Dict[str, Any] = field(default_factory=dict) # Gateway-specific public keys/options
    redirect_url: Optional[str] = None       # For redirect-based checkouts (Stripe/PayPal)
    error_message: Optional[str] = None


@dataclass
class PaymentVerificationResult:
    """Standardized result of cryptographic payment verification."""
    success: bool
    status: str                              # 'successful', 'failed', 'pending', 'cancelled'
    transaction_id: str
    provider_id: str
    provider_order_id: str
    provider_payment_id: str
    amount: float
    currency: str
    plan_id: str
    user_id: str
    error_message: Optional[str] = None
    raw_details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RefundResult:
    """Standardized result of a payment refund operation."""
    success: bool
    status: str                              # 'refunded', 'pending', 'failed'
    refund_id: str
    transaction_id: str
    provider_id: str
    amount_refunded: float
    currency: str
    error_message: Optional[str] = None
    raw_details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WebhookResult:
    """Standardized result of processing an asynchronous gateway webhook."""
    handled: bool
    event_type: str                          # e.g. 'payment.captured', 'checkout.session.completed'
    provider_id: str
    status: str                              # 'successful', 'failed', 'refunded', 'ignored'
    order_id: Optional[str] = None
    payment_id: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None
    user_id: Optional[str] = None
    plan_id: Optional[str] = None
    error_message: Optional[str] = None
    raw_event: Dict[str, Any] = field(default_factory=dict)


class BasePaymentProvider(ABC):
    """Abstract payment provider adapter."""

    @property
    @abstractmethod
    def provider_id(self) -> str:
        """Unique slug identifier for the provider (e.g. 'razorpay', 'stripe', 'paypal')."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable provider name (e.g. 'Razorpay', 'Stripe', 'PayPal')."""
        pass

    @abstractmethod
    def is_configured(self) -> bool:
        """Whether all required API credentials/keys are configured for this provider."""
        pass

    @abstractmethod
    def get_supported_currencies(self) -> List[str]:
        """List of ISO uppercase currencies supported by this provider."""
        pass

    @abstractmethod
    def get_supported_payment_methods(self, currency: str, region: str) -> List[PaymentMethodInfo]:
        """Return list of payment methods eligible for the given currency and region."""
        pass

    @abstractmethod
    def create_checkout_session(
        self,
        user_id: str,
        plan: Dict[str, Any],
        currency: str,
        country_code: str,
        payment_method_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        **kwargs
    ) -> CheckoutSessionResult:
        """Initiate a checkout session or order with the provider."""
        pass

    @abstractmethod
    def create_payment(
        self,
        user_id: str,
        amount: float,
        currency: str,
        payment_method_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Direct payment creation for supported server-to-server payment flows."""
        pass

    @abstractmethod
    def verify_payment(
        self,
        payload: Dict[str, Any],
        signature: Optional[str] = None
    ) -> PaymentVerificationResult:
        """Cryptographically verify payment outcome on backend. Never trust frontend alone."""
        pass

    @abstractmethod
    def refund_payment(
        self,
        payment_id: str,
        amount: Optional[float] = None,
        currency: Optional[str] = None,
        reason: Optional[str] = None
    ) -> RefundResult:
        """Initiate refund for a captured payment transaction."""
        pass

    @abstractmethod
    def cancel_subscription(self, subscription_id: str) -> bool:
        """Cancel an active recurring subscription with the upstream provider."""
        pass

    @abstractmethod
    def handle_webhook(
        self,
        payload: bytes,
        headers: Dict[str, str]
    ) -> WebhookResult:
        """Verify webhook signature and process asynchronous event idempotently."""
        pass
