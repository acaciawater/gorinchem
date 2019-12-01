'''
Created on Sep 1, 2019

@author: theo
'''
import json

from django.conf import settings
from django.http.response import JsonResponse, HttpResponseServerError
from django.views.generic.detail import DetailView

from acacia.meetnet.models import Network, Well
from acacia.meetnet.views import NetworkView
from django.utils import timezone

def statuscolor(last):
    """ returns color for bullets on home page.
    Green is less than 1 day old, yellow is 1 - 2 days, red is 3 - 7 days and gray is more than one week old 
     """
    if last:
        now = timezone.now()
        age = now - last.date
        if age.days < 1:
            return 'green' 
        elif age.days < 2:
            return 'yellow'
        elif age.days < 7:
            return 'red' 
    return 'grey'

class HomeView(NetworkView):
    template_name = 'gorinchem/home.html'

    def get_context_data(self, **kwargs):
        context = NetworkView.get_context_data(self, **kwargs)
        options = {
            'center': [52.15478, 4.48565],
            'zoom': 12 }
        context['api_key'] = settings.GOOGLE_MAPS_API_KEY
        context['options'] = json.dumps(options)

        welldata = []
        for w in Well.objects.order_by('name'):
            last = w.last_measurement()
            welldata.append((w,last,statuscolor(last)))
        context['wells'] = welldata
        return context

    def get_object(self):
        return Network.objects.first()

class PopupView(DetailView):
    """ returns html response for leaflet popup """
    model = Well
    template_name = 'meetnet/well_info.html'
    
def well_locations(request):
    """ return json response with well locations
    """
    result = []
    for p in Well.objects.all():
        try:
            pnt = p.location
            result.append({'id': p.id, 'name': p.name, 'nitg': p.nitg, 'description': p.description, 'lon': pnt.x, 'lat': pnt.y})
        except Exception as e:
            return HttpResponseServerError(unicode(e))
    return JsonResponse(result,safe=False)
