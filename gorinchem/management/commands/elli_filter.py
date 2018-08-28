'''
Created on Aug 28, 2018

@author: theo
'''
import os
from django.core.management.base import BaseCommand, CommandError
from acacia.data.models import Project, ProjectLocatie, MeetLocatie, Datasource, Parameter, Series,\
    Generator
from django.conf import settings
from acacia.meetnet.models import LoggerDatasource

class Command(BaseCommand):
    args = ''
    help = 'Filters ellitrack sourcefiles'

    def handle(self, *args, **options):
        elli = Generator.objects.get(name='Ellitrack')
        count = 0
        for ds in elli.datasource_set.all():
            serial = ds.loggerdatasource.logger.serial
            print ('Datalogger '+serial)
            for sf in ds.sourcefiles.all():
                if not serial in sf.name:
                    sf.delete()
                    print('deleted '+sf.name)
                    count += 1
        self.stdout.write('%d files deleted\n' % count)
