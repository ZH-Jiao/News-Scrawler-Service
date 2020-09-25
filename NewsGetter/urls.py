from django.urls import path
from . import views

urlpatterns = [
    path('get-nyt', views.get_nyt),
    path('get-cnn', views.get_cnn)
]
