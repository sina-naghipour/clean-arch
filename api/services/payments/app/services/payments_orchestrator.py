from database import pydantic_models
from database.database_models import PaymentDB, PaymentStatus
from repositories.payments_repository import PaymentRepository
from .stripe_service import StripeService
from opentelemetry import trace
import logging
from .commissions_service import CommissionService
from typing import Optional


class PaymentOrchestrator:
    def __init__(self, payment_repo: PaymentRepository, stripe_service: StripeService, logger: logging.Logger, commission_service: Optional[CommissionService] = None,):
        self.payment_repo = payment_repo
        self.stripe_service = stripe_service
        self.logger = logger
        self.commission_service = commission_service
        
    async def create_and_process_payment(self, payment_data: pydantic_models.PaymentCreate, tracer: trace.Tracer, checkout_mode: bool = True) -> PaymentDB:
        with tracer.start_as_current_span("create_and_process_payment") as span:
            span.set_attributes({
                "order.id": payment_data.order_id,
                "user.id": payment_data.user_id,
                "amount": float(payment_data.amount),
                "currency": payment_data.currency,
                "checkout_mode": checkout_mode
            })
            
            existing_payment = await self.payment_repo.get_payment_by_order_id(payment_data.order_id)
            if existing_payment:
                span.set_attribute("payment.exists", True)
                return existing_payment
            
            referrer_id = payment_data.referrer_id
            
            if referrer_id:
                span.set_attribute("referrer.id", referrer_id)
            
            payment = PaymentDB(
                order_id=payment_data.order_id,
                user_id=payment_data.user_id,
                amount=payment_data.amount,
                payment_method_token=payment_data.payment_method_token,
                currency=payment_data.currency,
                status=PaymentStatus.CREATED,
                referrer_id=referrer_id
            )
            
            created_payment = await self.payment_repo.create_payment(payment)
            span.set_attribute("payment.id", str(created_payment.id))
            
            try:

                metadata = {
                    'order_id': payment_data.order_id,
                    'user_id': payment_data.user_id,
                    'payment_id': str(created_payment.id),
                    'payment_type': 'checkout' if checkout_mode else 'payment_intent',
                }

                if referrer_id:
                    metadata['referrer_id'] = referrer_id
                
                stripe_result = await self.stripe_service.create_payment(
                    amount=payment_data.amount,
                    currency=payment_data.currency,
                    payment_method_token=payment_data.payment_method_token,
                    metadata=metadata,
                    checkout_mode=checkout_mode,
                    success_url=payment_data.success_url,
                    cancel_url=payment_data.cancel_url
                )
                
                if checkout_mode:
                    created_payment.checkout_url = stripe_result.get("url")
                    await self.payment_repo.update_payment_checkout_url(created_payment.id, stripe_result.get("url"))
                    stripe_id = stripe_result.get("payment_intent_id") or stripe_result.get("id")
                else:
                    stripe_id = stripe_result.get("id")
                    if stripe_result.get("client_secret"):
                        await self.payment_repo.update_payment_client_secret(created_payment.id, stripe_result.get("client_secret"))
                        created_payment.client_secret = stripe_result.get("client_secret")
                
                if stripe_id:
                    await self.payment_repo.update_payment_stripe_id(created_payment.id, stripe_id)
                
                if not checkout_mode:
                    payment_status = self.stripe_service.map_stripe_status_to_payment_status(stripe_result["status"])
                    created_payment.stripe_payment_intent_id = stripe_id
                else:
                    if stripe_result["type"] == "checkout":
                        payment_status = PaymentStatus.PROCESSING
                        created_payment.checkout_session_id = stripe_result["id"]
                        created_payment.stripe_payment_intent_id = None

                await self.payment_repo.update_payment_status(created_payment.id, payment_status)
                created_payment.status = payment_status
                
                span.set_attribute("stripe.id", stripe_id or "none")
                span.set_attribute("payment.status", str(payment_status.value))
                
            except Exception as e:
                span.record_exception(e)
                await self.payment_repo.update_payment_status(created_payment.id, PaymentStatus.FAILED)
                created_payment.status = PaymentStatus.FAILED
            
            return created_payment