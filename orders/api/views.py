from rest_framework import generics
from orders.models import Order
from .serializers import OrderSerializer

class OrderCreateView(generics.CreateAPIView):
    queryset = Order.objects.all()  # For /list.
    serializer_class = OrderSerializer

