from dataclasses import dataclass
from typing import List

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
    order_id: str
    customer_id: str
    total_amount: float
    items: List[OrderItemEvent]