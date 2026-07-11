# Order Kafka Consumer: Processes inventory-reserved and inventory-failed
# events from Kafka, updating order status accordingly.
#
# Idempotency guarantee:
#   Kafka delivers messages at-least-once. This consumer ensures each
#   event is processed exactly once by wrapping the business logic and
#   the ProcessedEvent insert in a single database transaction.
#
# Failure scenarios handled:
#   1. Duplicate delivery → already_processed() check skips it
#   2. Crash after processing but before mark_processed → Kafka
#      redelivers; already_processed() catches it on retry
#   3. Crash after mark_processed but before commit → transaction
#      rolls back; Kafka redelivers; clean retry
#   4. Two consumers process same event concurrently → one succeeds,
#      the other gets IntegrityError from unique constraint → caught
#      and logged, no data corruption

import logging

from confluent_kafka import Consumer
from django.conf import settings
from django.db import transaction, IntegrityError

logger = logging.getLogger(__name__)

from orders.events.event_envelope import EventEnvelope
from orders.events.idempotency import IdempotencyService
from orders.events.order_events import (
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
)
from orders.services.order_service import OrderService


class KafkaEventConsumer:

    def __init__(self):
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": "order-service-group",
                "auto.offset.reset": "earliest",
            }
        )

    
    def handle_inventory_reserved(self, envelope: EventEnvelope):
        logger.info("Inventory reserved for order %s [%s]", envelope.payload["order_id"], envelope.correlation_id)
        OrderService.confirm_order(
            order_id=envelope.payload["order_id"]
        )

    def handle_inventory_failed(self, envelope: EventEnvelope):
        logger.warning("Inventory reservation failed for order %s [%s]", envelope.payload["order_id"], envelope.correlation_id)
        OrderService.fail_order(
                order_id=envelope.payload["order_id"],
            )        

    def start(self):
        self.consumer.subscribe([
            INVENTORY_RESERVED,
            INVENTORY_FAILED,
        ])
        EVENT_HANDLERS = {
            INVENTORY_RESERVED: self.handle_inventory_reserved,
            INVENTORY_FAILED: self.handle_inventory_failed,
        }

        logger.info("Order Consumer Started...")

        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    logger.error("Consumer error: %s", msg.error())
                    continue

                envelope = EventEnvelope.from_json(msg.value().decode("utf-8"))

                logger.info("Received event [%s] from %s: %s", envelope.event_id, envelope.event_type, envelope.payload)

                try:
                    with transaction.atomic():
                        if IdempotencyService.already_processed(envelope.event_id):
                            logger.info("Event %s already processed, skipping", envelope.event_id)
                            continue

                        handler = EVENT_HANDLERS.get(envelope.event_type)
                        if handler:
                            handler(envelope)
                            IdempotencyService.mark_processed(envelope.event_id, envelope.event_type)
                        else:
                            logger.warning("No handler for event_type %s", envelope.event_type)
                except IntegrityError:
                    logger.info("Event %s already processed by another consumer", envelope.event_id)

        finally:
            self.consumer.close()