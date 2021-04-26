"""edge_node URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from edge_node.apps.views import get_cpu_state
from edge_node.apps.views import get_road_info
from edge_node.apps.views import road_info_switch, get_road_info_state
from edge_node.apps.views import event_ner, getYingjiju, getBus, getBendibao, getJiaoguanju

urlpatterns = [
    path('getCpuState', get_cpu_state),
    path('getRoadInfo', get_road_info),
    path('switchRoadInfo', road_info_switch),
    path('roadinfoState', get_road_info_state),
    path('eventNer', event_ner),
    path('getYingjiju', getYingjiju),
    path('getBendibao', getBendibao),
    path('getJiaoguanju', getJiaoguanju),
    path('getBus', getBus),
]
