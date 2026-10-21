# Flexible, Future-Ready Payment Provider Architecture

This document describes the modular payment architecture for **Tweets**, explains how to add new payment gateways and methods with zero core billing disruption, and details testing in sandbox mode.

---

## 1. Architectural Overview

The payment system follows a **Provider-Adapter & Registry** design pattern. The billing core, checkout modals, and subscription status controllers do not assume any specific payment gateway or single currency.

```
                         +-----------------------------+
                         |   Frontend Checkout Modal   |
                         +--------------+--------------+
                                        |
                 GET /api/payments/methods?currency=INR&country=IN
                                        v
                         +-----------------------------+
                         |   PaymentProviderRegistry   |
                         +--------------+--------------+
                                        |
                 +----------------------+----------------------+
                 |                      |                      |
                 v                      v                      v
        +-----------------+    +-----------------+    +-----------------+
        | RazorpayAdapter |    |  StripeAdapter  |    |  PayPalAdapter  |
        +-----------------+    +-----------------+    +-----------------+
        | UPI, Cards,     |    | Cards, ApplePay |    | PayPal Wallet,  |
        | NetBanking,     |    | GooglePay, SEPA |    | Pay in 4        |
        | Wallets         |    +-----------------+    +-----------------+
        +-----------------+
```

### Core Components
1. **`BasePaymentProvider`** ([`scripts/payments/base.py`](../scripts/payments/base.py)):
   Abstract base class establishing the contract for all gateways:
   - `get_supported_payment_methods(currency, region)`
   - `create_checkout_session(user_id, plan, currency, country_code, **kwargs)`
   - `create_payment(user_id, amount, currency, payment_method_id)`
   - `verify_payment(payload, signature)`
   - `refund_payment(payment_id, amount, currency, reason)`
   - `cancel_subscription(subscription_id)`
   - `handle_webhook(payload, headers)`

2. **`PaymentProviderRegistry`** ([`scripts/payments/registry.py`](../scripts/payments/registry.py)):
   Dynamic registry managing active adapters, prioritizing providers based on region and currency, and filtering methods based on admin configuration.

3. **Database-Backed Pricing Matrix** ([`scripts/payments_db.py`](../scripts/payments_db.py)):
   All plans and prices are stored in the SQLite database table `pricing_plans_config`. No plan prices are hardcoded in the frontend.

4. **Cryptographic Verification & Idempotency**:
   Backend verification is strictly mandatory for all payments. Transactions are recorded in `payment_transactions` with unique idempotency keys to eliminate double-charges.

---

## 2. Step-by-Step: Adding a New Payment Gateway (e.g. Adyen)

Adding a new provider does not require modifying existing subscription logic or frontend templates.

### Step 1: Create the Provider Adapter
Create a new file `scripts/payments/adapters/adyen_adapter.py`:

```python
from __future__ import annotations
import os
from typing import Any, Dict, List, Optional
from payments.base import (
    BasePaymentProvider,
    PaymentMethodInfo,
    CheckoutSessionResult,
    PaymentVerificationResult,
    RefundResult,
    WebhookResult,
)

class AdyenAdapter(BasePaymentProvider):
    def __init__(self, api_key: Optional[str] = None, merchant_account: Optional[str] = None):
        self._api_key = api_key or os.getenv("ADYEN_API_KEY", "").strip()
        self._merchant = merchant_account or os.getenv("ADYEN_MERCHANT_ACCOUNT", "").strip()

    @property
    def provider_id(self) -> str:
        return "adyen"

    @property
    def name(self) -> str:
        return "Adyen"

    def is_configured(self) -> bool:
        return bool(self._api_key and self._merchant)

    def get_supported_currencies(self) -> List[str]:
        return ["EUR", "USD", "GBP"]

    def get_supported_payment_methods(self, currency: str, region: str) -> List[PaymentMethodInfo]:
        return [
            PaymentMethodInfo(
                id="adyen_card",
                name="Credit / Debit Card (Adyen)",
                type="card",
                icon="credit-card",
                supported_currencies=["EUR", "USD", "GBP"],
                supported_regions=["GLOBAL"],
                description="Global card processing with 3D Secure 2.0",
            ),
            PaymentMethodInfo(
                id="ideal",
                name="iDEAL",
                type="bank",
                icon="building-2",
                supported_currencies=["EUR"],
                supported_regions=["NL", "europe"],
                description="Instant online banking in the Netherlands",
                badge="NL Native",
            )
        ]

    def create_checkout_session(self, user_id, plan, currency, country_code, **kwargs):
        # Call Adyen Sessions API (/sessions)
        return CheckoutSessionResult(
            success=True,
            session_id="CS_ADYEN_123",
            provider_id=self.provider_id,
            provider_order_id="CS_ADYEN_123",
            amount=plan["price"],
            amount_subunits=int(plan["price"] * 100),
            currency=currency,
            checkout_mode="sdk",
            client_params={"sessionData": "...", "id": "CS_ADYEN_123"}
        )

    def create_payment(self, user_id, amount, currency, payment_method_id, metadata=None):
        return {"status": "pending"}

    def verify_payment(self, payload, signature=None):
        # Verify result code with Adyen
        result_code = payload.get("resultCode", "Authorised")
        return PaymentVerificationResult(
            success=(result_code == "Authorised"),
            status="successful" if result_code == "Authorised" else "failed",
            transaction_id=payload.get("order_id", ""),
            provider_id=self.provider_id,
            provider_order_id=payload.get("order_id", ""),
            provider_payment_id=payload.get("pspReference", ""),
            amount=float(payload.get("amount", 0)),
            currency=payload.get("currency", "EUR"),
            plan_id=payload.get("plan_id", "monthly"),
            user_id=payload.get("user_id", "")
        )

    def refund_payment(self, payment_id, amount=None, currency=None, reason=None):
        # Call Adyen /refund API
        return RefundResult(
            success=True,
            status="refunded",
            refund_id=f"ref_adyen_{payment_id}",
            transaction_id=payment_id,
            provider_id=self.provider_id,
            amount_refunded=amount or 0.0,
            currency=currency or "EUR"
        )

    def cancel_subscription(self, subscription_id: str) -> bool:
        return True

    def handle_webhook(self, payload: bytes, headers: dict) -> WebhookResult:
        # Validate HMAC signature using Adyen HMAC Key
        return WebhookResult(
            handled=True,
            event_type="AUTHORISATION",
            provider_id=self.provider_id,
            status="successful"
        )
```

### Step 2: Register the Adapter
In [`scripts/payments/registry.py`](../scripts/payments/registry.py):
```python
from payments.adapters.adyen_adapter import AdyenAdapter

# In _register_default_providers:
self.register(AdyenAdapter())
```

### Step 3: Configure Environment Variables
Add your provider credentials to `.env`:
```ini
ADYEN_API_KEY=your_adyen_api_key_here
ADYEN_MERCHANT_ACCOUNT=your_merchant_account
ADYEN_HMAC_KEY=your_webhook_hmac_key
```

### Step 4: Run Automated Tests
Execute the flexible payment test suite:
```bash
python -m unittest tests/test_flexible_payments.py
```

---

## 3. Supported Currencies & Regional Rules

| Country / Region | Default Currency | Native Payment Methods | Primary Gateway |
|---|:---:|---|:---:|
| **India (`IN`)** | `INR` (₹) | UPI, Domestic Cards, Net Banking, Wallets | **Razorpay** |
| **United States (`US`)** | `USD` ($) | Global Cards, Apple Pay, Google Pay, PayPal | **Stripe / PayPal** |
| **Europe (`EU` / `DE` / `FR`)** | `EUR` (€) | Global Cards, Apple Pay, SEPA Direct Debit, PayPal | **Stripe / PayPal** |

---

## 4. Admin Configuration API

Administrators can configure the billing engine dynamically via `/api/admin/payments/*`:

- `GET /api/admin/payments/config`: View all active providers, methods, and database pricing.
- `POST /api/admin/payments/providers/{provider_id}`: Toggle provider active status, priority rank, or switch between `sandbox` and `live` mode.
- `POST /api/admin/payments/methods/{method_id}`: Enable/disable individual payment methods without redeployment.
- `POST /api/admin/payments/plans/{plan_id}/pricing`: Update plan prices in the database.
- `GET /api/admin/payments/transactions`: View real-time transaction ledger.
- `POST /api/admin/payments/transactions/{transaction_id}/refund`: Issue a refund through the provider adapter.
