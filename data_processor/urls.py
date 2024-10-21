from django.urls import path
from . import views
from .views import ProcessDataView


urlpatterns = [
    # path('', views.upload_file, name='upload_file'),  
    path('', ProcessDataView.as_view(), name='process_data'),
]
