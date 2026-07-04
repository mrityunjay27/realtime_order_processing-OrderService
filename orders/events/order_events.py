from dataclasses import dataclass
from typing import List


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