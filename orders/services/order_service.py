from django.db import transaction
from orders.models import Order, OrderItem, Product
from django.core.exceptions import ValidationError
from orders.events.order_events import OrderCreatedEvent, OrderItemEvent
from orders.events.event_publisher import ConsoleEventPublisher
from orders.events.kafka_publisher import KafkaEventPublisher
from dataclasses import asdict

class OrderService:

    @staticmethod
    def build_order_created_event(order):
        items = []

        for item in order.items.all():
            items.append(
                OrderItemEvent(
                    product_id=str(item.product.id),
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
        Creates order + items + calculates total
        Step 1: Create order with status PENDING and total_amount 0
        Step 2: Create order items and calculate total_amount
        Step 3: Update order with total_amount
        Step 4: Publish order created event to Kafka
        """

        order = Order.objects.create(
            customer=customer,
            status="PENDING",
            total_amount=0
        )

        total = 0

        for item in items_data:
            product = item["product"]
            quantity = item["quantity"]
            price = item["price"]

            if quantity <= 0:
                raise ValidationError("Quantity must be greater than 0")

            if price <= 0:
                raise ValidationError("Price must be greater than 0")

            if not Product.objects.filter(id=product.id).exists():
                raise ValidationError(f"Product {product.id} does not exist")

            OrderItem.objects.create(
                order=order,
                product=product,
                quantity=quantity,
                price=price
            )

            total += price * quantity

        order.total_amount = total
        order.save()

        # Publish to console log
        # publisher = ConsoleEventPublisher()
        # event = OrderService.build_order_created_event(order)
        # publisher.publish("order_created", asdict(event))

        # Publish to Kafka
        publisher = KafkaEventPublisher()
        event = OrderService.build_order_created_event(order)
        publisher.publish("orders.created", asdict(event))  # We have to create this topic manually.

        return order