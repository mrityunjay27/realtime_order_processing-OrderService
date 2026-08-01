import logging

from django.db import transaction, DatabaseError
from orders.models import Order, OrderItem
from django.core.exceptions import ValidationError
from orders.events.order_events import OrderCreatedEvent, OrderItemEvent
from orders.events.event_envelope import EventEnvelope
from orders.events.outbox_service import OutboxService
from orders.events.audit.services import EventHistoryService
from orders.events.audit.constants import AGGREGATE_ORDER, format_aggregate_id
from orders.events.exceptions import (
    RetryableEventException,
    NonRetryableEventException,
)
from dataclasses import asdict
from uuid import uuid4

from core.logging import context as logging_context

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
            correlation_id=logging_context.get_correlation_id() or str(uuid4()),
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

        logger.info("Order creation started")

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

        EventHistoryService.record_published(
            event_id=envelope.event_id,
            event_type=envelope.event_type,
            correlation_id=event.correlation_id,
            aggregate_type=AGGREGATE_ORDER,
            aggregate_id=format_aggregate_id(AGGREGATE_ORDER, order.id),
            payload=envelope.to_dict(),
        )

        logger.info("Order created with id %s", order.id)

        return order
    
    @staticmethod
    @transaction.atomic
    def set_inventory_reserved(order_id, correlation_id):
        try:
            order = Order.objects.get(id=order_id)
            order.status = "INVENTORY_RESERVED"
            order.save()

            _publish_payment_requested(correlation_id, order_id, float(order.total_amount))

            logger.info("Order %s — inventory reserved, payment requested.", order_id)
        except Order.DoesNotExist:
            raise NonRetryableEventException(
                f"Order {order_id} not found"
            )
        except DatabaseError as exc:
            raise RetryableEventException(
                f"Database error while updating order to INVENTORY_RESERVED: {exc}"
            ) from exc

    @staticmethod
    @transaction.atomic
    def handle_payment_succeeded(order_id):
        try:
            order = Order.objects.get(id=order_id)
            order.status = "COMPLETED"
            order.save()
            logger.info("Order %s completed — payment succeeded.", order_id)
        except Order.DoesNotExist:
            raise NonRetryableEventException(
                f"Order {order_id} not found"
            )
        except DatabaseError as exc:
            raise RetryableEventException(
                f"Database error while completing order: {exc}"
            ) from exc

    @staticmethod
    @transaction.atomic
    def handle_payment_failed(order_id, correlation_id):
        try:
            order = Order.objects.get(id=order_id)
            order.status = "FAILED"
            order.save()

            _publish_release_inventory(correlation_id, order_id, order)

            logger.info("Order %s failed — payment failed, releasing inventory.", order_id)
        except Order.DoesNotExist:
            raise NonRetryableEventException(
                f"Order {order_id} not found"
            )
        except DatabaseError as exc:
            raise RetryableEventException(
                f"Database error while failing order: {exc}"
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


def _publish_payment_requested(correlation_id, order_id, amount):
    envelope = EventEnvelope(
        event_type="payments.requested",
        correlation_id=correlation_id,
        payload={
            "correlation_id": correlation_id,
            "order_id": order_id,
            "amount": amount,
        },
    )
    OutboxService.create_event(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        payload=envelope.to_dict(),
    )
    EventHistoryService.record_published(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        correlation_id=correlation_id,
        aggregate_type=AGGREGATE_ORDER,
        aggregate_id=format_aggregate_id(AGGREGATE_ORDER, order_id),
        payload=envelope.to_dict(),
    )


def _publish_release_inventory(correlation_id, order_id, order):
    items = []
    for item in order.items.all():
        items.append({
            "product_id": str(item.product_id),
            "quantity": item.quantity,
        })

    envelope = EventEnvelope(
        event_type="inventory.release",
        correlation_id=correlation_id,
        payload={
            "correlation_id": correlation_id,
            "order_id": order_id,
            "items": items,
        },
    )
    OutboxService.create_event(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        payload=envelope.to_dict(),
    )
    EventHistoryService.record_published(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        correlation_id=correlation_id,
        aggregate_type=AGGREGATE_ORDER,
        aggregate_id=format_aggregate_id(AGGREGATE_ORDER, order_id),
        payload=envelope.to_dict(),
    )