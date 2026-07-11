from dataclasses import dataclass, field
from typing import List
from uuid import uuid4

ORDER_CREATED = "order.created"
INVENTORY_RESERVED = "inventory.reserved"
INVENTORY_FAILED = "inventory.failed"


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