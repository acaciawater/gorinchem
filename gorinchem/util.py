from django.template.loader import render_to_string
from acacia.data.models import Generator, MeetLocatie, SourceFile, Chart, Series
from acacia.meetnet.models import Well, Screen, Datalogger, MonFile, Channel, LoggerDatasource

import os,re
from acacia.data.generators import sws

import datetime, pytz
import binascii
from django.core.files.base import ContentFile

import logging
logger = logging.getLogger('upload')

# l=logging.getLogger('acacia.data').addHandler(h)

def createmonfile(source, generator=sws.Diver()):
    ''' parse .mon file and create MonFile instance '''
    
    # default timeone for MON files = CET
    CET = pytz.timezone('CET')
    
    headerdict = generator.get_header(source)
    mon = MonFile()
    header = headerdict['HEADER']
    mon.company = header.get('COMPANY',None)
    mon.compstat = header.get('COMP.STATUS',None)
    if 'DATE' in header and 'TIME' in header:
        dt = header.get('DATE') + ' ' + header.get('TIME')
        mon.date = datetime.datetime.strptime(dt,'%d/%m/%Y %H:%M:%S').replace(tzinfo=CET)
    else:
        mon.date = datetime.datetime.now(CET)
    mon.monfilename = header.get('FILENAME',None)
    mon.createdby = header.get('CREATED BY',None)
    mon.num_points = int(header.get('Number of points','0'))
    
    s = headerdict['Logger settings']
    instype = s.get('Instrument type',None)
    parts = instype.split('=')
    mon.instrument_type = parts[-1] 
    mon.status = s.get('Status',None)
    serial = s.get('Serial number',None)
    if serial is not None:
        serial = re.split(r'[-\s+]',serial)[1]
    mon.serial_number = serial
    mon.instrument_number = s.get('Instrument number',None)
    mon.location = s.get('Location',None)
    mon.sample_period = s.get('Sample period',None)
    mon.sample_method = s.get('Sample method','T')
    mon.num_channels = int(s.get('Number of channels','1'))

    s = headerdict['Series settings']
    mon.start_date = datetime.datetime.strptime(s['Start date / time'],'%S:%M:%H %d/%m/%y').replace(tzinfo=CET)    
    mon.end_date = datetime.datetime.strptime(s['End date / time'], '%S:%M:%H %d/%m/%y').replace(tzinfo=CET)    

    channels = []
    for i in range(mon.num_channels):
        channel = Channel(number = i+1)
        name = 'Channel %d' % (i+1)
        s = headerdict[name]
        channel.identification = s.get('Identification',name)
        t = s.get('Reference level','0 -')
        channel.reference_level, channel.reference_unit = re.split(r'\s+',t)
        channel.range, channel.range_unit = re.split(r'\s+', s.get('Range','0 -'))
        channel.range_unit = repr(channel.range_unit)
        channel.reference_unit = repr(channel.reference_unit)
        channels.append(channel)
    return (mon, channels)

def addmonfile(request,network,f):
    ''' add monfile to database and create related tables '''
    filename = f.name    
    basename = os.path.basename(filename)
    logger.info('Verwerken van bestand ' + basename)
    
    user = request.user
    generator = Generator.objects.get(name='Schlumberger')
    if not filename.endswith('.MON'):
        logger.warning('Bestand {name} wordt overgeslagen: bestandsnaam eindigt niet op .MON'.format(name=basename))
        return (None,None)
    try:
        mon, channels = createmonfile(f)
        serial = mon.serial_number
        put = mon.location
        logger.info('Informatie uit MON file: Put={put}, diver={ser}'.format(put=put,ser=serial))
        well = network.well_set.get(name=put)
        # TODO: find out screen number
        filter = 1
        screen = well.screen_set.get(nr=filter)
        try:
            loc = MeetLocatie.objects.get(name=unicode(screen))
        except:
            loc = MeetLocatie.objects.get(name='%s/%s' % (put,filter))
            
        datalogger, created = Datalogger.objects.get_or_create(serial=serial,defaults={'model': mon.instrument_type})
        if created:
            logger.info('Nieuwe datalogger toegevoegd met serienummer {ser}'.format(ser=serial))
        
        # get installation depth from last existing logger
        existing_loggers = screen.loggerpos_set.all().order_by('start_date')
        last = existing_loggers.last()
        depth = last.depth if last else None
        
        pos, created = datalogger.loggerpos_set.get_or_create(screen=screen,refpnt=screen.refpnt,defaults={'baro': well.baro, 'depth': depth, 'start_date': mon.start_date, 'end_date': mon.end_date})
        if created:
            logger.info('Datalogger {log} gekoppeld aan filter {loc}'.format(log=serial,loc=unicode(screen)))
            if depth is None:
                logger.warning('Geen kabellengte beschikbaar voor deze logger')
        else:
            logger.info('Geinstalleerde logger {log} gevonden in filter {loc}'.format(log=serial,loc=unicode(screen)))

            # update dates of loggerpos
            shouldsave = False
            if not pos.end_date or pos.end_date < mon.end_date:
                pos.end_date = mon.end_date
                shouldsave = True
            if not pos.start_date or pos.start_date > mon.start_date:
                pos.start_date = mon.start_date
                shouldsave = True
            if shouldsave:
                pos.save()

        # get/create datasource for logger
        ds, created = LoggerDatasource.objects.get_or_create(name=datalogger.serial,meetlocatie=loc,
                                                             defaults = {'logger': datalogger, 'generator': generator, 'user': user, 'timezone': 'CET'})
        f.seek(0)
        contents = f.read()
        mon.crc = abs(binascii.crc32(contents))
        try:
            sf = ds.sourcefiles.get(crc=mon.crc)
            logger.warning('Identiek bestand bestaat al in gegevensbron {ds}'.format(ds=unicode(ds)))
        except SourceFile.DoesNotExist:
            # add source file
            mon.name = mon.filename = basename
            mon.datasource = ds
            mon.user = ds.user
            contentfile = ContentFile(contents)
            mon.file.save(name=filename, content=contentfile)
            mon.get_dimensions()
            mon.save()
            mon.channel_set.add(*channels)
            pos.monfile_set.add(mon)

            logger.info('Bestand {filename} toegevoegd aan gegevensbron {ds} voor logger {log}'.format(filename=basename, ds=unicode(ds), log=unicode(pos)))
            return (mon,screen)
    except Well.DoesNotExist:
        logger.error('Put {put} niet gevonden in meetnet {net}'.format(put=put,net=network))
    except Screen.DoesNotExist:
        logger.error('Filter {filt} niet gevonden voor put {put}'.format(put=put, filt=filter))
    return (None,None)

from acacia.meetnet.util import recomp, make_chart

def update_series(request,screen):
    user=request.user
    name = '%s COMP' % screen
    series, created = Series.objects.get_or_create(name=name,defaults={'user':user})
    try:
        meetlocatie = MeetLocatie.objects.get(name=unicode(screen))
        series.mlocatie = meetlocatie
        series.save()
    except:
        logger.exception('Meetlocatie niet gevonden voor filter {screen}'.format(screen=unicode(screen)))
        return

    recomp(screen, series)
                 
    #maak/update grafiek
    chart, created = Chart.objects.get_or_create(name=unicode(screen), defaults={
                'title': unicode(screen),
                'user': user, 
                'percount': 0, 
                })
    chart.series.get_or_create(series=series, defaults={'label' : 'm tov NAP'})

    # handpeilingen toevoegen (als beschikbaar)
    if hasattr(meetlocatie, 'manualseries_set'):
        name = '%s HAND' % screen
        for hand in meetlocatie.manualseries_set.filter(name=name):
            chart.series.get_or_create(series=hand,defaults={'type':'scatter', 'order': 2})
    
    make_chart(screen)

def handle_uploaded_files(request, network, localfiles):
    num = len(localfiles)
    if num == 0:
        return
    #incstall handler that buffers logrecords to be sent by email 
    buffer=logging.handlers.BufferingHandler(20000)
    logger.addHandler(buffer)
    try:
        logger.info('Verwerking van %d bestand(en)' % num)
        screens = set()
        wells = set()
        result = {}
        for pathname in localfiles:
            msg = []
            try:
                filename = os.path.basename(pathname)
                with open(pathname) as f:
                    mon,screen = addmonfile(request,network, f)
                if not mon:
                    msg.append('Niet gebruikt')
                    logger.warning('Bestand {name} overgeslagen'.format(name=filename))
                else:
                    msg.append('Succes')
                    screens.add(screen)
                    wells.add(screen.well)
            except Exception as e:
                logger.exception('Probleem met bestand {name}: {error}'.format(name=filename,error=e))
                msg.append('Fout: '+unicode(e))
                continue
            result[filename] = ', '.join(msg)
    
        logger.info('Bijwerken van tijdreeksen')
        num = 0
        for s in screens:
            try:
                logger.info('Tijdreeks {}'.format(unicode(s)))
                update_series(request, s)
                num += 1
            except Exception as e:
                logger.exception('Bijwerken tijdreeksen voor filter {screen} mislukt: {error}'.format(screen=unicode(s), error=e))
        logger.info('{} tjdreeksen bijgewerkt'.format(num))

        logger.info('Bijwerken van grafieken voor putten')
        num=0
        for w in wells:
            try:
                logger.info('Put {}'.format(unicode(w)))
                make_chart(w)
                num += 1
            except Exception as e:
                logger.exception('Bijwerken van grafieken voor put {well} mislukt: {error}'.format(well=unicode(w), error=e))
        logger.info('{} grafieken bijgewerkt'.format(num))

        if request.user.email:
            
            logbuffer = buffer.buffer
            buffer.flush()

            logger.debug('Sending email to %s (%s)' % (request.user.get_full_name() or request.user.username, request.user.email))
            
            name=request.user.first_name or request.user.username
            html_message = render_to_string('notify_email_nl.html', {'name': name, 'network': network, 'result': result, 'logrecords': logbuffer})
            message = render_to_string('notify_email_nl.txt', {'name': name, 'network': network, 'result': result, 'logrecords': logbuffer})
            request.user.email_user(subject='Meetnet {net}: bestanden verwerkt'.format(net=network), message=message, html_message = html_message)
    finally:
        logger.removeHandler(buffer)