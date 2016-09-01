'''
Created on Sep 1, 2016

@author: theo
'''
from acacia.meetnet.views import NetworkView
from acacia.meetnet.models import Network
 
class HomeView(NetworkView):
    def get_object(self):
        return Network.objects.get(name = 'Gorinchem')
    