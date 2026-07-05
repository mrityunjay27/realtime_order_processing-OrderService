import json
from kafka import KafkaProducer


class KafkaEventPublisher:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",  # Kafka is available at this location. Via port fowarding from container to host machine.
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def publish(self, topic: str, event: dict):
        self.producer.send(topic, value=event)
        self.producer.flush()
        print(f"🚀 Event sent to Kafka topic: {topic}")