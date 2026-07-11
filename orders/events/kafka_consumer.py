import json
import logging

from confluent_kafka import Consumer
from django.conf import settings

logger = logging.getLogger(__name__)

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

    
    def handle_inventory_reserved(self, event):
        cid = event.get("correlation_id", "unknown")
        logger.info("Inventory reserved for order %s [%s]", event["order_id"], cid)
        OrderService.confirm_order(
            order_id=event["order_id"]
        )

    def handle_inventory_failed(self, event):
        cid = event.get("correlation_id", "unknown")
        logger.warning("Inventory reservation failed for order %s [%s]", event["order_id"], cid)
        OrderService.fail_order(
                order_id=event["order_id"],
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

                topic = msg.topic()
                event = json.loads(msg.value().decode("utf-8"))

                logger.info("Received event from %s: %s", topic, event)

                if topic in EVENT_HANDLERS:
                    EVENT_HANDLERS[topic](event)
                else:
                    logger.warning("No handler for topic %s", topic)

        finally:
            self.consumer.close()