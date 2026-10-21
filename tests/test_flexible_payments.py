"""Comprehensive test suite for the Flexible, Future-Ready Payment System."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import unittest
from pathlib import Path

# Add scripts directory to path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import database
import payments_db
from payments.base import (
    BasePaymentProvider,
    CheckoutSessionResult,
    PaymentMethodInfo,
    PaymentVerificationResult,
    RefundResult,
    WebhookResult,
)
from payments.registry import PaymentProviderRegistry
from payments.adapters.razorpay_adapter import RazorpayAdapter
from payments.adapters.stripe_adapter import StripeAdapter
from payments.adapters.paypal_adapter import PayPalAdapter


class TestFlexiblePaymentSystem(unittest.TestCase):

    def setUp(self):
        database.init_db()
        self.registry = PaymentProviderRegistry()

    def test_01_provider_interface_compliance(self):
        """Verify that all default adapters implement BasePaymentProvider interface."""
        providers = [RazorpayAdapter(), StripeAdapter(), PayPalAdapter()]
        for p in providers:
            self.assertIsInstance(p, BasePaymentProvider)
            self.assertTrue(len(p.provider_id) > 0)
            self.assertTrue(len(p.name) > 0)
            self.assertIsInstance(p.get_supported_currencies(), list)
            self.assertTrue(len(p.get_supported_currencies()) > 0)

    def test_02_custom_provider_extensibility(self):
        """Verify that a brand-new provider can be registered dynamically without billing core changes."""
        class MockCryptoProvider(BasePaymentProvider):
            @property
            def provider_id(self) -> str:
                return "crypto_usdc"
            @property
            def name(self) -> str:
                return "USDC on Solana"
            def is_configured(self) -> bool:
                return True
            def get_supported_currencies(self) -> list:
                return ["USD"]
            def get_supported_payment_methods(self, currency: str, region: str) -> list:
                return [PaymentMethodInfo(
                    id="crypto_usdc",
                    name="USDC (Solana Pay)",
                    type="instant",
                    icon="coins",
                    supported_currencies=["USD"],
                    supported_regions=["GLOBAL"],
                )]
            def create_checkout_session(self, user_id, plan, currency, country_code, **kwargs):
                return CheckoutSessionResult(
                    success=True, session_id="sol_123", provider_id="crypto_usdc",
                    provider_order_id="sol_ord_123", amount=plan["price"],
                    amount_subunits=int(plan["price"] * 100), currency=currency,
                    checkout_mode="qr"
                )
            def create_payment(self, user_id, amount, currency, payment_method_id, metadata=None):
                return {"status": "created"}
            def verify_payment(self, payload, signature=None):
                return PaymentVerificationResult(
                    success=True, status="successful", transaction_id="sol_123",
                    provider_id="crypto_usdc", provider_order_id="sol_ord_123",
                    provider_payment_id="sol_sig_456", amount=float(payload.get("amount", 0)),
                    currency="USD", plan_id=payload.get("plan_id", "monthly"),
                    user_id=payload.get("user_id", "tester")
                )
            def refund_payment(self, payment_id, amount=None, currency=None, reason=None):
                return RefundResult(success=True, status="refunded", refund_id="sol_ref_1",
                                    transaction_id=payment_id, provider_id="crypto_usdc",
                                    amount_refunded=amount or 0.0, currency="USD")
            def cancel_subscription(self, subscription_id: str) -> bool:
                return True
            def handle_webhook(self, payload: bytes, headers: dict) -> WebhookResult:
                return WebhookResult(handled=True, event_type="sol.confirmed",
                                     provider_id="crypto_usdc", status="successful")

        custom_p = MockCryptoProvider()
        self.registry.register(custom_p)
        self.assertIsNotNone(self.registry.get_provider("crypto_usdc"))

        # Test querying eligible methods including new provider
        methods = self.registry.get_eligible_payment_methods("USD", "US")
        method_ids = [m["id"] for m in methods]
        self.assertIn("crypto_usdc", method_ids)

        # Cleanup
        self.registry.unregister("crypto_usdc")
        self.assertIsNone(self.registry.get_provider("crypto_usdc"))

    def test_03_regional_payment_method_filtering(self):
        """Verify that payment methods strictly respect regional and currency availability."""
        # India (INR): UPI must be present, Apple Pay / SEPA should NOT be active
        in_methods = self.registry.get_eligible_payment_methods(currency="INR", country_code="IN")
        in_ids = [m["id"] for m in in_methods]
        self.assertIn("upi", in_ids)
        self.assertIn("card", in_ids)
        self.assertIn("netbanking", in_ids)
        self.assertNotIn("sepa_debit", in_ids)

        # USA (USD): Card, Apple Pay, PayPal must be present; UPI must NOT be present
        us_methods = self.registry.get_eligible_payment_methods(currency="USD", country_code="US")
        us_ids = [m["id"] for m in us_methods]
        self.assertIn("card", us_ids)
        self.assertIn("apple_pay", us_ids)
        self.assertIn("paypal", us_ids)
        self.assertNotIn("upi", us_ids)
        self.assertNotIn("sepa_debit", us_ids)

        # Europe (EUR): SEPA Direct Debit must be present
        eu_methods = self.registry.get_eligible_payment_methods(currency="EUR", country_code="DE")
        eu_ids = [m["id"] for m in eu_methods]
        self.assertIn("sepa_debit", eu_ids)
        self.assertIn("card", eu_ids)
        self.assertIn("apple_pay", eu_ids)
        self.assertNotIn("upi", eu_ids)

    def test_04_database_backed_pricing_matrix(self):
        """Verify that all plan prices are stored in database and strictly preserve authoritative values."""
        in_plans = {p["plan_id"]: p for p in database.get_db_pricing_plans("INR", "india")}
        self.assertEqual(in_plans["monthly"]["price"], 250.0)
        self.assertEqual(in_plans["six_month"]["price"], 500.0)
        self.assertEqual(in_plans["annual"]["price"], 999.0)
        self.assertIsNone(in_plans["lifetime"]["price"])
        self.assertFalse(in_plans["lifetime"]["checkout_enabled"])

        us_plans = {p["plan_id"]: p for p in database.get_db_pricing_plans("USD", "usa")}
        self.assertEqual(us_plans["monthly"]["price"], 225.0)
        self.assertEqual(us_plans["six_month"]["price"], 400.0)
        self.assertEqual(us_plans["annual"]["price"], 500.0)
        self.assertEqual(us_plans["lifetime"]["price"], 2000.0)
        self.assertTrue(us_plans["lifetime"]["checkout_enabled"])

        eu_plans = {p["plan_id"]: p for p in database.get_db_pricing_plans("EUR", "europe")}
        self.assertEqual(eu_plans["monthly"]["price"], 210.0)
        self.assertEqual(eu_plans["six_month"]["price"], 380.0)
        self.assertEqual(eu_plans["annual"]["price"], 475.0)
        self.assertEqual(eu_plans["lifetime"]["price"], 1900.0)

    def test_05_checkout_session_creation_and_idempotency(self):
        """Verify checkout session creation across providers with transaction idempotency."""
        user_id = "test_user_idempotent"
        plan = database.get_db_pricing_plan("monthly", "INR", "india")
        self.assertIsNotNone(plan)

        session = self.registry.create_checkout_session(
            provider_id="razorpay",
            user_id=user_id,
            plan=plan,
            currency="INR",
            country_code="IN",
            payment_method_id="upi"
        )
        self.assertTrue(session.success)
        self.assertEqual(session.amount, 250.0)
        self.assertEqual(session.currency, "INR")
        self.assertEqual(session.provider_id, "razorpay")

        # Test recording transaction
        tx = database.create_payment_transaction(
            transaction_id=session.session_id,
            idempotency_key=f"idem_{session.session_id}",
            user_id=user_id,
            plan_id="monthly",
            provider_id="razorpay",
            payment_method_id="upi",
            currency="INR",
            amount=session.amount,
            amount_subunits=session.amount_subunits,
            status="pending"
        )
        self.assertEqual(tx["status"], "pending")
        self.assertEqual(tx["amount"], 250.0)

        # Idempotent re-insertion should not duplicate
        tx2 = database.create_payment_transaction(
            transaction_id=session.session_id,
            idempotency_key=f"idem_{session.session_id}",
            user_id=user_id,
            plan_id="monthly",
            provider_id="razorpay",
            payment_method_id="upi",
            currency="INR",
            amount=session.amount,
            amount_subunits=session.amount_subunits,
            status="pending"
        )
        self.assertEqual(tx["id"], tx2["id"])

    def test_06_backend_cryptographic_verification(self):
        """Verify HMAC-SHA256 signature checking and state transitions."""
        rzp = RazorpayAdapter(key_id="rzp_test_testkey", key_secret="secret123456789")

        order_id = "order_abc123"
        payment_id = "pay_xyz789"
        msg = f"{order_id}|{payment_id}".encode("utf-8")
        valid_signature = hmac.new("secret123456789".encode("utf-8"), msg, hashlib.sha256).hexdigest()

        # 1. Invalid signature must fail
        bad_res = rzp.verify_payment(
            payload={"order_id": order_id, "payment_id": payment_id},
            signature="tampered_fake_signature"
        )
        self.assertFalse(bad_res.success)
        self.assertEqual(bad_res.status, "failed")

        # 2. Valid signature must succeed
        good_res = rzp.verify_payment(
            payload={"order_id": order_id, "payment_id": payment_id, "amount": 250.0, "currency": "INR"},
            signature=valid_signature
        )
        self.assertTrue(good_res.success)
        self.assertEqual(good_res.status, "successful")

    def test_07_provider_webhooks_and_refunds(self):
        """Verify webhook processing and refund transaction flows."""
        # 1. Razorpay Webhook
        rzp = RazorpayAdapter(webhook_secret="whsec_test_123")
        webhook_body = json.dumps({
            "event": "payment.captured",
            "payload": {
                "payment": {
                    "entity": {
                        "id": "pay_hook_123",
                        "order_id": "order_hook_456",
                        "amount": 25000,
                        "currency": "INR",
                        "notes": {"user_id": "user_wh", "plan_id": "monthly"}
                    }
                }
            }
        }).encode("utf-8")
        sig = hmac.new("whsec_test_123".encode("utf-8"), webhook_body, hashlib.sha256).hexdigest()

        wh_res = rzp.handle_webhook(webhook_body, {"X-Razorpay-Signature": sig})
        self.assertTrue(wh_res.handled)
        self.assertEqual(wh_res.status, "successful")
        self.assertEqual(wh_res.order_id, "order_hook_456")

        # 2. Refunds
        ref_res = rzp.refund_payment("pay_hook_123", amount=250.0, currency="INR", reason="Test Refund")
        self.assertTrue(ref_res.success)
        self.assertEqual(ref_res.status, "refunded")
        self.assertTrue(ref_res.refund_id.startswith("rfnd_rzp_"))

    def test_08_admin_configuration_controls(self):
        """Verify administrator toggles for providers, methods, and plans."""
        # 1. Update method config
        updated_method = database.update_payment_method_config("wallet", is_enabled=False)
        self.assertIsNotNone(updated_method)
        self.assertEqual(updated_method["is_enabled"], 0)

        # Restore
        restored_method = database.update_payment_method_config("wallet", is_enabled=True)
        self.assertEqual(restored_method["is_enabled"], 1)

        # 2. Update provider config
        updated_provider = database.update_payment_provider_config("paypal", priority=5)
        self.assertIsNotNone(updated_provider)
        self.assertEqual(updated_provider["priority"], 5)

        # Restore priority
        database.update_payment_provider_config("paypal", priority=3)

        # 3. Update pricing plan
        plan_update = database.update_db_pricing_plan("monthly", "USD", "usa", price=230.0)
        self.assertIsNotNone(plan_update)
        self.assertEqual(plan_update["price"], 230.0)

        # Restore price
        database.update_db_pricing_plan("monthly", "USD", "usa", price=225.0)


if __name__ == "__main__":
    unittest.main()
