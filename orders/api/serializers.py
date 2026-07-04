from rest_framework import serializers
from orders.models import Order, OrderItem, Customer, Product

class OrderItemSerializer(serializers.ModelSerializer):
    class Meta:
        model = OrderItem
        fields = ["product", "quantity", "price"]


class OrderSerializer(serializers.ModelSerializer):
    items = OrderItemSerializer(many=True)

    class Meta:
        model = Order
        fields = ["id", "customer", "status", "total_amount", "items"]
        read_only_fields = ["id", "status", "total_amount"]

    def create(self, validated_data):
        items_data = validated_data.pop("items")

        order = Order.objects.create(**validated_data)

        total = 0

        for item in items_data:
            product = item["product"]
            quantity = item["quantity"]
            price = item["price"]

            OrderItem.objects.create(
                order=order,
                product=product,
                quantity=quantity,
                price=price
            )

            total += price * quantity

        order.total_amount = total
        order.save()

        return order