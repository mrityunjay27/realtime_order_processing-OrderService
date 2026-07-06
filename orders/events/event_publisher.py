import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ConsoleEventPublisher:
    """
    Temporary publisher (NO Kafka yet).
    Just logs events.
    """

    def publish(self, topic: str, event: dict):
        payload = {
            "topic": topic,
            "timestamp": datetime.utcnow().isoformat(),
            "event": event
        }

        logger.info("EVENT PUBLISHED:\n%s", json.dumps(payload, indent=2))

class EventPublisher:
    """
    Abstract event publisher.
    Later we can plug Kafka, RabbitMQ, etc.
    """

    def publish(self, topic: str, event: dict):
        raise NotImplementedError("Publisher not implemented")