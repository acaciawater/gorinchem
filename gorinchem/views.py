'''
Created on Sep 1, 2016

@author: theo
'''
from acacia.meetnet.views import NetworkView
from acacia.meetnet.models import Network, Screen, LoggerPos
import logging
from django.shortcuts import get_object_or_404
from django.views.generic.list import ListView

logger = logging.getLogger(__name__)

class HomeView(NetworkView):

    def get_context_data(self, **kwargs):
        context = NetworkView.get_context_data(self, **kwargs)
        context['maptype'] = 'SATELLITE'
        return context
    
    def get_object(self):
        return Network.objects.get(name = 'Gorinchem')
    
class LoggerPosListView(ListView):
    model = LoggerPos

    def get_context_data(self, **kwargs):
        context =  ListView.get_context_data(self, **kwargs)
        pk = int(self.kwargs.get('pk'))
        screen = get_object_or_404(Screen, pk=pk)
        context['screen'] = screen
        network = screen.well.network
        context['network'] = network
        return context
    
    def get_queryset(self):
        pk = int(self.kwargs.get('pk'))
        screen = get_object_or_404(Screen, pk=pk)
        return screen.loggerpos_set.all()
