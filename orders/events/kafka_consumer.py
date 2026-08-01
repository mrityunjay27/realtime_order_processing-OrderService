import logging

from confluent_kafka import Consumer, KafkaError
from django.conf import settings
from django.db import transaction, IntegrityError

from core.logging import context as logging_context

logger = logging.getLogger(__name__)

from orders.events.event_envelope import EventEnvelope
from orders.events.idempotency import IdempotencyService
from orders.events.order_events import (
    INVENTORY_RESERVED,
    INVENTORY_RESERVED_RETRY,
    INVENTORY_FAILED,
    INVENTORY_FAILED_RETRY,
    PAYMENT_SUCCEEDED,
    PAYMENT_SUCCEEDED_RETRY,
    PAYMENT_FAILED,
    PAYMENT_FAILED_RETRY,
)
from orders.events.failure_handler import FailureHandler
from orders.events.audit.services import EventHistoryService
from orders.events.audit.constants import AGGREGATE_ORDER, format_aggregate_id
from orders.services.order_service import OrderService


class KafkaEventConsumer:

    def __init__(self):
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": "order-service-group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.failure_handlers = {
            INVENTORY_RESERVED: FailureHandler(
                retry_topic="inventory.reserved.retry",
                dlq_topic="inventory.reserved.dlq",
            ),
            INVENTORY_FAILED: FailureHandler(
                retry_topic="inventory.failed.retry",
                dlq_topic="inventory.failed.dlq",
            ),
            PAYMENT_SUCCEEDED: FailureHandler(
                retry_topic="payments.succeeded.retry",
                dlq_topic="payments.succeeded.dlq",
            ),
            PAYMENT_FAILED: FailureHandler(
                retry_topic="payments.failed.retry",
                dlq_topic="payments.failed.dlq",
            ),
        }

    
    def handle_inventory_reserved(self, envelope: EventEnvelope):
        logger.info("Inventory reserved for order %s [%s]", envelope.payload["order_id"], envelope.correlation_id)
        OrderService.set_inventory_reserved(
            order_id=envelope.payload["order_id"],
            correlation_id=envelope.correlation_id,
        )

    def handle_inventory_failed(self, envelope: EventEnvelope):
        logger.warning("Inventory reservation failed for order %s [%s]", envelope.payload["order_id"], envelope.correlation_id)
        OrderService.fail_order(
                order_id=envelope.payload["order_id"],
            )        

    def handle_payment_succeeded(self, envelope: EventEnvelope):
        logger.info("Payment succeeded for order %s [%s]", envelope.payload["order_id"], envelope.correlation_id)
        OrderService.handle_payment_succeeded(
            order_id=envelope.payload["order_id"],
        )

    def handle_payment_failed(self, envelope: EventEnvelope):
        logger.warning("Payment failed for order %s [%s]", envelope.payload["order_id"], envelope.correlation_id)
        OrderService.handle_payment_failed(
            order_id=envelope.payload["order_id"],
            correlation_id=envelope.correlation_id,
        )

    def _extract_aggregate_id(self, envelope: EventEnvelope) -> str:
        return envelope.payload.get("order_id", "unknown")

    def start(self):
        self.consumer.subscribe([
            INVENTORY_RESERVED,
            INVENTORY_RESERVED_RETRY,
            INVENTORY_FAILED,
            INVENTORY_FAILED_RETRY,
            PAYMENT_SUCCEEDED,
            PAYMENT_SUCCEEDED_RETRY,
            PAYMENT_FAILED,
            PAYMENT_FAILED_RETRY,
        ])
        EVENT_HANDLERS = {
            INVENTORY_RESERVED: self.handle_inventory_reserved,
            INVENTORY_RESERVED_RETRY: self.handle_inventory_reserved,
            INVENTORY_FAILED: self.handle_inventory_failed,
            INVENTORY_FAILED_RETRY: self.handle_inventory_failed,
            PAYMENT_SUCCEEDED: self.handle_payment_succeeded,
            PAYMENT_SUCCEEDED_RETRY: self.handle_payment_succeeded,
            PAYMENT_FAILED: self.handle_payment_failed,
            PAYMENT_FAILED_RETRY: self.handle_payment_failed,
        }

        logger.info("Order Consumer Started...")

        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    continue

                envelope = EventEnvelope.from_json(msg.value().decode("utf-8"))
                envelope_dict = envelope.to_dict()

                logging_context.set_context(
                    correlation_id=envelope.correlation_id,
                    event_id=envelope.event_id,
                    event_type=envelope.event_type,
                    kafka={
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    },
                )

                logger.info(
                    "Kafka event received",
                    extra={
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    },
                )

                try:
                    with transaction.atomic():
                        if IdempotencyService.already_processed(envelope.event_id):
                            logger.info("Event %s already processed", envelope.event_id)

                        elif envelope.event_type in EVENT_HANDLERS:
                            EVENT_HANDLERS[envelope.event_type](envelope)
                            IdempotencyService.mark_processed(envelope.event_id, envelope.event_type)

                            aggregate_id = self._extract_aggregate_id(envelope)
                            EventHistoryService.record_consumed(
                                event_id=envelope.event_id,
                                event_type=envelope.event_type,
                                correlation_id=envelope.correlation_id,
                                aggregate_type=AGGREGATE_ORDER,
                                aggregate_id=format_aggregate_id(AGGREGATE_ORDER, aggregate_id),
                                payload=envelope.to_dict(),
                            )

                        else:
                            logger.warning("No handler for event_type %s", envelope.event_type)

                    # DB transaction succeeded — safe to commit Kafka offset
                    self.consumer.commit(msg)

                except IntegrityError:
                    # Database says duplicate (race condition) — safe to commit
                    logger.info("Duplicate event %s, committing offset", envelope.event_id)
                    self.consumer.commit(msg)

                except Exception as exc:
                    self.consumer.commit(msg)

                    aggregate_id = envelope.payload.get("order_id", "unknown")
                    EventHistoryService.record_consumed_failed(
                        event_id=envelope.event_id,
                        event_type=envelope.event_type,
                        correlation_id=envelope.correlation_id,
                        aggregate_type=AGGREGATE_ORDER,
                        aggregate_id=format_aggregate_id(AGGREGATE_ORDER, aggregate_id),
                        payload=envelope.to_dict(),
                    )

                    base_type = envelope.event_type.replace(".retry", "")
                    handler = self.failure_handlers.get(base_type)
                    if handler:
                        handler.handle(envelope_dict, exc)
                    else:
                        logger.exception("No failure handler for event_type %s", envelope.event_type)

                finally:
                    logging_context.clear_context()

        finally:
            self.consumer.close()
