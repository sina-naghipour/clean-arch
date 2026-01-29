import time
import logging
from uuid import UUID
from database.database_models import PaymentDB
from .notification_service import NotificationService
from .retry_service import RetryService


class PaymentNotificationService:
    def __init__(self, notification_service: NotificationService, retry_service: RetryService, logger: logging.Logger):
        self.notification_service = notification_service
        self.retry_service = retry_service
        self.logger = logger
    
    async def notify_orders_service(self, payment: PaymentDB, status: str, receipt_url: str = None, checkout_url: str = None) -> bool:
        self.logger.info(f"SENDING TO ORDERS SERVICE: payment {payment.id}, status: {status}, receipt_url: {receipt_url}")
        if not payment or not payment.order_id:
            return False
        
        idempotency_key = f"payment_{payment.id}_{status}_{int(time.time())}"
        notification_data = {
            "order_id": payment.order_id,
            "payment_id": str(payment.id),
            "status": status,
            "stripe_payment_intent_id": payment.stripe_payment_intent_id,
            "receipt_url": receipt_url,
            "checkout_url": checkout_url
        }
        
        try:
            await self.retry_service.execute_with_retry(
                lambda: self.notification_service.send_notification(notification_data, idempotency_key),
                self.logger
            )
            return True
        except Exception as e:
            self.logger.warning(f"Could not notify Orders service: {e}")
            return False