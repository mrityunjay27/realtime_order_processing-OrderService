import json

from confluent_kafka import Consumer
from django.conf import settings

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
        print(f"✅ Inventory reserved for order {event['order_id']}")
        OrderService.confirm_order(
            order_id=event["order_id"]
        )

    def handle_inventory_failed(self, event):
        
        print(f"❌ Inventory reservation failed for order {event['order_id']}")
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

        print("🚀 Order Consumer Started...")

        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(msg.error())
                    continue

                topic = msg.topic()
                event = json.loads(msg.value().decode("utf-8"))

                print(f"📩 Received event from {topic}: {event}")

                if topic in EVENT_HANDLERS:
                    EVENT_HANDLERS[topic](event)
                else:
                    print(f"⚠️ No handler for topic {topic}")

        finally:
            self.consumer.close()