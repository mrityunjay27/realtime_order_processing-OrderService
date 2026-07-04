from rest_framework import serializers
from orders.models import Order, OrderItem
from orders.services.order_service import OrderService


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
        customer = validated_data["customer"]

        return OrderService.create_order(customer, items_data)