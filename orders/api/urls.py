from django.urls import path
from .views import OrderCreateView

urlpatterns = [
    path("orders/", OrderCreateView.as_view(), name="create-order"),
]