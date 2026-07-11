import logging

from confluent_kafka import Consumer
from django.conf import settings

logger = logging.getLogger(__name__)

from orders.events.event_envelope import EventEnvelope
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

                handler = EVENT_HANDLERS.get(envelope.event_type)
                if handler:
                    handler(envelope)
                else:
                    logger.warning("No handler for event_type %s", envelope.event_type)

        finally:
            self.consumer.close()