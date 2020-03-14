"""gorinchem URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""

from django.conf.urls import url, include
from django.contrib import admin
from django.conf.urls.static import static
from django.conf import settings
from views import well_locations, PopupView, HomeView

urlpatterns = [
    url(r'^$', HomeView.as_view(), name='home'),
    url(r'^grappelli/', include('grappelli.urls')),
    url(r'^data/', include('acacia.data.urls',namespace='acacia')),
    url(r'^net/', include('acacia.meetnet.urls',namespace='meetnet')),
    url(r'^bro/',include('acacia.meetnet.bro.urls',namespace='bro')),
    url(r'^locs/',well_locations,name='locs'),
    url(r'^pop/(?P<pk>\d+)', PopupView.as_view()),
    url(r'^api/v1/', include('acacia.meetnet.api.urls')),
    url(r'^admin/', include(admin.site.urls)),
    url(r'^accounts/', include('registration.backends.hmac.urls')),
]

urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
urlpatterns += static(settings.EXPORT_URL, document_root=settings.EXPORT_ROOT)
urlpatterns += static(settings.LOGGING_URL, document_root=settings.LOGGING_ROOT)

if settings.DEBUG:
    import debug_toolbar
    urlpatterns = [
        url(r'^__debug__/', include(debug_toolbar.urls)),
    ] + urlpatterns
