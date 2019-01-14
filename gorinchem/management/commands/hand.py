'''
Created on Dec 6, 2014

@author: theo
'''
import csv, datetime
from django.core.management.base import BaseCommand
from acacia.data.models import ManualSeries
from acacia.meetnet.models import Well,Screen
from django.contrib.auth.models import User
import pytz

class Command(BaseCommand):
    args = ''
    help = 'Importeer handpeilingen'
    def add_arguments(self, parser):
        parser.add_argument('-f','--file',
                action='store',
                dest = 'fname',
                default = None,
                help='CSV file met handpeilingen'
        )
        
    def handle(self, *args, **options):
        fname = options.get('fname')
        CET=pytz.timezone('Etc/GMT-1')
        user=User.objects.get(username='theo')
        if fname:
            with open(fname,'r') as f:
                reader = csv.DictReader(f, delimiter=',')
                for row in reader:
                    name = row['Naam']
                    try:
                        well = Well.objects.get(name=name)
                        filt = int(row['Filter'])
                        screen = well.screen_set.get(nr=filt)
                        datumtijd = '%s %s' % (row['Datum'], row['WinterTijd'])
                        depth = row['Meting']
                        if depth:
                            depth = float(depth)
                        else:
                            continue
                        if not screen.refpnt:
                            print 'Reference point for screen %s not available' % screen
                            continue
                        nap = screen.refpnt - depth
                        date = datetime.datetime.strptime(datumtijd,'%Y-%m-%d %H:%M:%S')
                        date = CET.localize(date)
                        mloc = screen.mloc
                        series_name = '%s HAND' % mloc.name
                        series,created = ManualSeries.objects.get_or_create(name=series_name,mlocatie=mloc,defaults={'description':'Handpeiling', 'unit':'m NAP', 'type':'scatter', 'user':user})
                        if row.get('Verwijderen','nee') == 'ja':
                            deleted = series.datapoints.filter(date=date).delete()
                            print screen, date, ('deleted' if deleted else 'NOT deleted')
                        else:
                            pt, created = series.datapoints.update_or_create(date=date,defaults={'value': nap})
                            print screen, pt.date, pt.value
                    except Well.DoesNotExist:
                        print 'Well %s not found' % name
                    except Screen.DoesNotExist:
                        print 'Screen %s/%03d not found' % (name, filt)
                    except Exception as e:
                        print e, name
                        