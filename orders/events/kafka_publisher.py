import json
import logging
from uuid import uuid4
from confluent_kafka import Producer
from orders.events.event_envelope import EventEnvelope

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
        envelope = EventEnvelope(
            event_type=topic,
            correlation_id=event.get("correlation_id", str(uuid4())),
            payload=event,
        )
        payload = json.dumps(envelope.to_dict()).encode("utf-8")
        self.producer.produce(topic, value=payload, callback=self.delivery_report)
        self.producer.flush()