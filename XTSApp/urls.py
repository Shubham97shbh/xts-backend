from django.urls import path, include
from .views import StartProcessView, StopProcessView, LogsView

urlpatterns = [
    path('api/xts/log-view', LogsView.as_view(), name='log-view'),
    path('api/xts/start_app', StartProcessView.as_view(), name='start_app'),
    path('api/xts/stop_app', StopProcessView.as_view(), name='stop_app'),
]
