import logging

from django.db import transaction, DatabaseError
from orders.models import Order, OrderItem
from django.core.exceptions import ValidationError
from orders.events.order_events import OrderCreatedEvent, OrderItemEvent
from orders.events.event_envelope import EventEnvelope
from orders.events.outbox_service import OutboxService
from orders.events.exceptions import (
    RetryableEventException,
    NonRetryableEventException,
)
from dataclasses import asdict

logger = logging.getLogger(__name__)

class OrderService:

    @staticmethod
    def build_order_created_event(order):
        items = []

        for item in order.items.all():
            items.append(
                OrderItemEvent(
                    product_id=str(item.product_id),
                    quantity=item.quantity,
                    price=float(item.price)
                )
            )

        return OrderCreatedEvent(
            order_id=str(order.id),
            customer_id=str(order.customer.id),
            total_amount=float(order.total_amount),
            items=items
        )


    @staticmethod
    @transaction.atomic
    def create_order(customer, items_data):
        """
        Creates order + items + calculates total + saves to outbox.

        The Kafka publish happens asynchronously via the outbox publisher.
        This guarantees that the order and the event are either both saved
        or neither is — no orphaned events or lost orders.
        """

        order = Order.objects.create(
            customer=customer,
            status="PENDING",
            total_amount=0
        )

        total = 0

        for item in items_data:
            product_id = item["product_id"]
            quantity = item["quantity"]
            price = item["price"]

            if quantity <= 0:
                raise ValidationError("Quantity must be greater than 0")

            if price <= 0:
                raise ValidationError("Price must be greater than 0")

            OrderItem.objects.create(
                order=order,
                product_id=product_id,
                quantity=quantity,
                price=price
            )

            total += price * quantity

        order.total_amount = total
        order.save()

        event = OrderService.build_order_created_event(order)

        envelope = EventEnvelope(
            event_type="orders.created",
            correlation_id=event.correlation_id,
            payload=asdict(event),
        )

        OutboxService.create_event(
            event_id=envelope.event_id,
            event_type=envelope.event_type,
            payload=envelope.to_dict(),
        )

        return order
    
    @staticmethod
    @transaction.atomic
    def confirm_order(order_id):
        try:
            order = Order.objects.get(id=order_id)
            order.status = "CONFIRMED"
            order.save()
            logger.info("Order %s confirmed.", order_id)
        except Order.DoesNotExist:
            raise NonRetryableEventException(
                f"Order {order_id} not found"
            )
        except DatabaseError as exc:
            raise RetryableEventException(
                f"Database error while confirming order: {exc}"
            ) from exc

    @staticmethod
    @transaction.atomic
    def fail_order(order_id):
        try:
            order = Order.objects.get(id=order_id)
            order.status = "FAILED"
            order.save()
            logger.info("Order %s failed.", order_id)
        except Order.DoesNotExist:
            raise NonRetryableEventException(
                f"Order {order_id} not found"
            )
        except DatabaseError as exc:
            raise RetryableEventException(
                f"Database error while failing order: {exc}"
            ) from exc