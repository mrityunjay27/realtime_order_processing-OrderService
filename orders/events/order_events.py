from dataclasses import dataclass, field
from typing import List
from uuid import uuid4

ORDER_CREATED = "order.created"
INVENTORY_RESERVED = "inventory.reserved"
INVENTORY_RESERVED_RETRY = "inventory.reserved.retry"
INVENTORY_RESERVED_DLQ = "inventory.reserved.dlq"
INVENTORY_FAILED = "inventory.failed"
INVENTORY_FAILED_RETRY = "inventory.failed.retry"
INVENTORY_FAILED_DLQ = "inventory.failed.dlq"

PAYMENT_SUCCEEDED = "payments.succeeded"
PAYMENT_SUCCEEDED_RETRY = "payments.succeeded.retry"
PAYMENT_SUCCEEDED_DLQ = "payments.succeeded.dlq"
PAYMENT_FAILED = "payments.failed"
PAYMENT_FAILED_RETRY = "payments.failed.retry"
PAYMENT_FAILED_DLQ = "payments.failed.dlq"

RELEASE_INVENTORY = "inventory.release"
RELEASE_INVENTORY_RETRY = "inventory.release.retry"
RELEASE_INVENTORY_DLQ = "inventory.release.dlq"


@dataclass
class OrderItemEvent:
    product_id: str
    quantity: int
    price: float


@dataclass
class OrderCreatedEvent:
    correlation_id: str = field(default_factory=lambda: str(uuid4()))
    order_id: str = ""
    customer_id: str = ""
    total_amount: float = 0.0
    items: List[OrderItemEvent] = field(default_factory=list)