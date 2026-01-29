from uuid import UUID
import logging
from typing import Optional
from fastapi import Request
import httpx
from opentelemetry import trace
from optl.trace_decorator import trace_service_operation

from database import pydantic_models
from database.database_models import PaymentDB
from repositories.payments_repository import PaymentRepository
from .stripe_service import StripeService
from .notification_service import NotificationService
from .retry_service import RetryService
from .payments_orchestrator import PaymentOrchestrator
from .payments_notification_service import PaymentNotificationService
from .webhook_handler import WebhookHandler
from .refund_processor import RefundProcessor
from cache.redis_cache import cached, invalidate_cache
from database.pydantic_models import PaymentStatus
from .idempotency_service import IdempotencyService
from .commissions_service import CommissionService

class PaymentService:
    def __init__(self, 
                logger: logging.Logger, 
                db_session,
                stripe_service: StripeService,
                commission_service: Optional[CommissionService] = None,
                http_client: Optional[httpx.AsyncClient] = None, redis_cache = None):
        self.logger = logger
        self.tracer = trace.get_tracer(__name__)
        self.idempotency_service = IdempotencyService(redis_cache)
        self.commission_service = commission_service

        self.payment_repo = PaymentRepository(db_session)
        self.stripe_service = stripe_service
        
        notification_service = NotificationService(http_client)
        retry_service = RetryService()

        self.payment_orchestrator = PaymentOrchestrator(
            self.payment_repo, 
            stripe_service, 
            logger
        )
        
        self.webhook_handler = WebhookHandler(
            self.payment_repo, 
            self.tracer, 
            logger,
            commission_service=self.commission_service
        )
        
        self.payment_notification_service = PaymentNotificationService(notification_service, retry_service, logger)
        self.refund_processor = RefundProcessor(self.payment_repo, stripe_service, logger)
    
    def _to_payment_response(self, payment: PaymentDB) -> pydantic_models.PaymentResponse:
        return pydantic_models.PaymentResponse(
            id=str(payment.id),
            order_id=payment.order_id,
            user_id=payment.user_id,
            amount=payment.amount,
            status=payment.status,
            stripe_payment_intent_id=payment.stripe_payment_intent_id,
            payment_method_token=payment.payment_method_token,
            currency=payment.currency,
            created_at=payment.created_at.isoformat(),
            updated_at=payment.updated_at.isoformat(),
            client_secret=payment.client_secret,
            checkout_url=getattr(payment, 'checkout_url', None),
            checkout_session_id=payment.checkout_session_id
        )
    
    @trace_service_operation("create_payment")
    @invalidate_cache(pattern="cache:payment_by_order:*")
    async def create_payment(self, payment_data: pydantic_models.PaymentCreate):
        checkout_mode = getattr(payment_data, 'checkout_mode', True)
        
        payment = await self.payment_orchestrator.create_and_process_payment(
            payment_data, self.tracer, checkout_mode
        )
        return self._to_payment_response(payment)
    
    @trace_service_operation("get_payment")
    @cached(ttl=300, key_prefix="payment_service")
    async def get_payment(self, payment_id: str):
        with self.tracer.start_as_current_span("get_payment") as span:
            span.set_attribute("payment.id", payment_id)
            
            payment_uuid = UUID(payment_id)
            payment = await self.payment_repo.get_payment_by_id(payment_uuid)
            
            if not payment:
                raise Exception(f"Payment not found: {payment_id}")
            
            return self._to_payment_response(payment)

    @trace_service_operation("process_webhook")
    @invalidate_cache(pattern="cache:payment_by_id:*")
    async def process_webhook(self, request: Request, payload: bytes, sig_header: str):
        with self.tracer.start_as_current_span("process_webhook") as span:
            span.set_attributes({
                "http.url": str(request.url),
                "http.method": request.method,
                "client.ip": str(request.client.host) if request.client else "unknown"
            })
            
            event = await self.stripe_service.handle_webhook_event(payload, sig_header)
            event_type = event["type"]
            event_id = event["id"]
            
            async def process_event():
                event_data = event["data"]
                if isinstance(event_data, dict) and "object" in event_data:
                    event_data = event_data["object"]
                
                self.logger.info(f"Processing webhook {event_type} from IP: {request.client.host}")
                span.set_attribute("stripe.event_type", event_type)
                span.set_attribute("stripe.event_id", event_id)
                
                metadata = event_data.get('metadata', {})
                payment_id = metadata.get('payment_id')                
                if not payment_id:
                    self.logger.warning(f"No payment_id in metadata for event: {event_type}")
                    return {"status": "ignored", "reason": "no_payment_id"}
                
                result, receipt_url = await self.webhook_handler.handle_stripe_event(
                    event_type, event_data, UUID(payment_id)
                )
                
                if result and result != "ignored":
                    payment = await self.payment_repo.get_payment_by_id(UUID(payment_id))
                    checkout_url = getattr(payment, 'checkout_url', None)
                
                    if payment:
                        await self.payment_notification_service.notify_orders_service(payment, result, receipt_url, checkout_url)
                
                self.logger.info(f"Processed {event_type} for payment ID: {payment_id}")
                return {"status": "processed", "event": event_type}
        
            return await self.idempotency_service.execute_once(
                key=f"webhook:{event_id}",
                operation=process_event,
                ttl=7*86400
            )
    
    @trace_service_operation("create_refund")
    @invalidate_cache(pattern="cache:payment_by_id:*")
    async def create_refund(self, payment_id: str, refund_data: pydantic_models.RefundRequest):
        with self.tracer.start_as_current_span("create_refund") as span:
            span.set_attributes({
                "payment.id": payment_id,
                "refund.amount": float(refund_data.amount) if refund_data.amount else None,
                "refund.reason": refund_data.reason or "unknown"
            })
            
            refund_result = await self.refund_processor.process_refund(payment_id, refund_data)
            
            payment = await self.payment_repo.get_payment_by_id(UUID(payment_id))
            if payment:
                await self.payment_notification_service.notify_orders_service(payment, "refunded")
            
            span.set_attribute("refund.id", refund_result["id"])
            span.set_attribute("refund.status", refund_result["status"])
            
            return refund_result
        
    @trace_service_operation("handle_checkout_success")
    async def handle_checkout_success(self, request):
        return {"status": "success", "message": "Checkout success handled"}

    @trace_service_operation("handle_checkout_cancel")
    async def handle_checkout_cancel(self, request):
        return {"status": "cancelled", "message": "Checkout cancel handled"}