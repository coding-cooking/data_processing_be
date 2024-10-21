from django.urls import path
from . import views
from .views import ProcessDataView

urlpatterns = [
    path('', ProcessDataView.as_view(), name='process_data'),
]
