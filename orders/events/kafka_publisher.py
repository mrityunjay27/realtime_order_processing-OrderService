import json
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class KafkaEventPublisher:
    def __init__(self):
        config = {
            "bootstrap.servers": "localhost:9092",
        }
        self.producer = Producer(config)

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Delivery failed for {msg.topic()}: {err}")
        else:
            logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    def publish(self, topic: str, event: dict):
        payload = json.dumps(event).encode("utf-8")
        self.producer.produce(topic, value=payload, callback=self.delivery_report)
        self.producer.flush()