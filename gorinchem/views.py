'''
Created on Sep 1, 2016

@author: theo
'''
from acacia.meetnet.views import NetworkView
from acacia.meetnet.models import Network
from django.views.generic import FormView, TemplateView
from forms import UploadFileForm
import os,logging
from util import handle_uploaded_files
from django.core.files.storage import default_storage
from django.shortcuts import get_object_or_404
from django.core.urlresolvers import reverse

logger = logging.getLogger(__name__)

class HomeView(NetworkView):
    
    def get_context_data(self, **kwargs):
        context = NetworkView.get_context_data(self, **kwargs)
        context['maptype'] = 'SATELLITE'
        return context
    
    def get_object(self):
        return Network.objects.get(name = 'Gorinchem')
    
class UploadDoneView(TemplateView):
    template_name = 'upload_done.html'

    def get_context_data(self, **kwargs):
        context = super(UploadDoneView, self).get_context_data(**kwargs)
        context['user'] = self.request.user
        context['network'] = get_object_or_404(Network,pk=int(kwargs.get('id',1)))
        return context

def save_file(file_obj,folder):
    path = default_storage.path(os.path.join(folder,file_obj.name))
    with open(path, 'wb') as destination:
        for chunk in file_obj.chunks():
            destination.write(chunk)
    return path

class UploadFileView(FormView):

    template_name = 'upload.html'
    form_class = UploadFileForm
    success_url = '/done/1'
    
    def get_success_url(self):
        return reverse('upload_done',kwargs=self.kwargs)

    def get_context_data(self, **kwargs):
        context = super(UploadFileView, self).get_context_data(**kwargs)
        context['network'] = get_object_or_404(Network,pk=int(self.kwargs.get('id')))
        return context

    def form_valid(self, form):

        # download files to upload folder
        local_files = []
        for f in form.files.getlist('filename'):
            path = save_file(f,'upload')
            local_files.append(path)
            
        network = get_object_or_404(Network,pk=int(self.kwargs.get('id')))

        # start background process that handles uploaded files
        from threading import Thread
        t = Thread(target=handle_uploaded_files, args=(self.request, network, local_files))
        t.start()
        
        return super(UploadFileView,self).form_valid(form)
