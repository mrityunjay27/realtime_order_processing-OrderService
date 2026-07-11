# Order Kafka Consumer: Processes inventory-reserved and inventory-failed
# events from Kafka, updating order status accordingly.
#
# Offset commit strategy:
#   Kafka auto-commit is DISABLED. The offset is committed manually only
#   AFTER the database transaction succeeds. This guarantees:
#
#   - DB commit + Kafka commit are both done → event fully processed
#   - DB fails → no Kafka commit → Kafka redelivers on restart
#   - Crash between DB commit and Kafka commit → Kafka redelivers,
#     but ProcessedEvent's unique constraint prevents re-processing
#
# This is the foundation for retry and DLQ: if processing fails,
# the offset is NOT committed, so Kafka will redeliver the message
# on consumer restart. A future retry limit can then route to DLQ.

import logging

from confluent_kafka import Consumer, KafkaError
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
                "enable.auto.commit": False,
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
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    continue

                envelope = EventEnvelope.from_json(msg.value().decode("utf-8"))

                logger.info("Received event [%s] from %s: %s", envelope.event_id, envelope.event_type, envelope.payload)

                try:
                    with transaction.atomic():
                        if IdempotencyService.already_processed(envelope.event_id):
                            logger.info("Event %s already processed", envelope.event_id)

                        elif envelope.event_type in EVENT_HANDLERS:
                            EVENT_HANDLERS[envelope.event_type](envelope)
                            IdempotencyService.mark_processed(envelope.event_id, envelope.event_type)

                        else:
                            logger.warning("No handler for event_type %s", envelope.event_type)

                    # DB transaction succeeded — safe to commit Kafka offset
                    self.consumer.commit(msg)

                except IntegrityError:
                    # Database says duplicate (race condition) — safe to commit
                    logger.info("Duplicate event %s, committing offset", envelope.event_id)
                    self.consumer.commit(msg)

                except Exception:
                    # Processing failed — do NOT commit.
                    # Kafka will redeliver on consumer restart.
                    logger.exception("Failed processing event %s, will retry", envelope.event_id)

        finally:
            self.consumer.close()