import ast
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from models import NewSubs, Trajet, Station
from datetime import datetime, timedelta
import config


def setup_db():
    connection.setup([config.cassandra_url], config.cassandra_keyspace, protocol_version=3)
    sync_table(NewSubs)
    sync_table(Trajet)
    sync_table(Station)

def save_to_db(table, data):
    
    if table == 'new_subs':
        day=datetime.strptime(data['day'] , "%Y-%m-%d")
        NewSubs.create(
            date=datetime.strptime(day.strftime("%m/%d/%Y"), "%m/%d/%Y"),
            number=int(data['new_subscribers'][0]['number']),
            monthly=int(data['new_subscribers'][0]['monthly']),
            year=int(data['new_subscribers'][0]['year'])
        ).save()
    elif table == 'frequented_tram':
        date=datetime.strptime(data['day'] , "%Y-%m-%d")
        shortDate = datetime.strptime(date.strftime("%m/%d/%Y"), "%m/%d/%Y")

        dataTramA = ast.literal_eval(data['tram_A'][0])
        dataTramB = ast.literal_eval(data['tram_B'][0])
        dataTramC = ast.literal_eval(data['tram_C'][0])

        #extracting and storing the trajectory

        Trajet.create(
            day=shortDate,
            intervalStart=int(data['interval_start']),
            intervalStop=int(data['interval_stop']),
            tram='Tram A',
            users=int(dataTramA['users'])
        ).save()

        Trajet.create(
            day=shortDate,
            intervalStart=int(data['interval_start']),
            intervalStop=int(data['interval_stop']),
            tram='Tram B',
            users=int(dataTramB['users'])
        ).save()

        Trajet.create(
            day=shortDate,
            intervalStart=int(data['interval_start']),
            intervalStop=int(data['interval_stop']),
            tram='Tram C',
            users=int(dataTramC['users'])
        ).save()

        #extracting and storing the stations

        for prop, value in dataTramA.items():
            if prop != 'users':
                Station.create(
                    day=shortDate,
                    intervalStart=int(data['interval_start']),
                    intervalStop=int(data['interval_stop']),
                    name=prop,
                    users=int(value)
                ).save()

        for prop, value in dataTramB.items():
            if prop != 'users':
                Station.create(
                    day=shortDate,
                    intervalStart=int(data['interval_start']),
                    intervalStop=int(data['interval_stop']),
                    name=prop,
                    users=int(value)
                ).save()

        for prop, value in dataTramC.items():
            if prop != 'users':
                Station.create(
                    day=shortDate,
                    intervalStart=int(data['interval_start']),
                    intervalStop=int(data['interval_stop']),
                    name=prop,
                    users=int(value)
                ).save()
            


def load_new_subs():
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    startDate = today - timedelta(days=10)
    days = []
    monthlyUsers = []
    yearlyUsers = []

    for i in range(0, 10):
        day = startDate + timedelta(days=i)
        days.append(day.strftime("%m/%d/%Y"))
        result = NewSubs.objects.allow_filtering().filter(
            date=datetime.strptime(day.strftime("%m/%d/%Y %I:%M %p"), "%m/%d/%Y %I:%M %p"))
        if result:
            #get the max and update all the records to read
            max = 0
            maxRecordMonthly = 0
            maxRecordYearly = 0
            for record in result:
                if record.number > max:
                    max = record.number
                    maxRecordMonthly = record.monthly
                    maxRecordYearly = record.year
                record.read = True
                record.save()

            monthlyUsers.append(maxRecordMonthly)
            yearlyUsers.append(maxRecordYearly)
        else:
            monthlyUsers.append(0)
            yearlyUsers.append(0)

    return { 'axis': days, 'monthlyUsers': monthlyUsers, 'yearlyUsers': yearlyUsers }

def load_new_subs_now() :
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    hour = datetime.now().strftime("%H:%M:%S")
    monthlyUsers = 0
    yearlyUsers = 0

    result = NewSubs.objects.allow_filtering().filter(
        date=datetime.strptime(today.strftime("%m/%d/%Y %I:%M %p"), "%m/%d/%Y %I:%M %p"),
        read=False)
    
    max = 0
    maxRecordMonthly = 0
    maxRecordYearly = 0
    for record in result:
        if record.number > max:
            max = record.number
            maxRecordMonthly = record.monthly
            maxRecordYearly = record.year
        record.read = True
        record.save()
    
    monthlyUsers = maxRecordMonthly
    yearlyUsers = maxRecordYearly
        
    return { 'axis': hour, 'monthlyUsers': monthlyUsers, 'yearlyUsers': yearlyUsers }


def load_trajets_usage():
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    startDate = today - timedelta(days=10)
    days = []
    tramA = []
    tramB = []
    tramC = []

    for i in range(0, 10):
        day = startDate + timedelta(days=i)
        days.append(day.strftime("%m/%d/%Y"))
        result = Trajet.objects.allow_filtering().filter(
            day=datetime.strptime(day.strftime("%m/%d/%Y %I:%M %p"), "%m/%d/%Y %I:%M %p"),
            tram='Tram A')
        if result:
            tramA.append(max([x.users for x in result]))
        else:
            tramA.append(0)
    
    for i in range(0, 10):
        day = startDate + timedelta(days=i)
        result = Trajet.objects.allow_filtering().filter(
            day=datetime.strptime(day.strftime("%m/%d/%Y %I:%M %p"), "%m/%d/%Y %I:%M %p"),
            tram='Tram B')
        if result:
            tramB.append(max([x.users for x in result]))
        else:
            tramB.append(0)
    
    for i in range(0, 10):
        day = startDate + timedelta(days=i)
        result = Trajet.objects.allow_filtering().filter(
            day=datetime.strptime(day.strftime("%m/%d/%Y %I:%M %p"), "%m/%d/%Y %I:%M %p"),
            tram='Tram C')
        if result:
            tramC.append(max([x.users for x in result]))
        else:
            tramC.append(0)

    return { 'axis': days, 'tramA': tramA, 'tramB': tramB, 'tramC': tramC }


def load_trajets_usage_now():
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    hour = datetime.now().strftime("%H:%M:%S")
    tramA = 0
    tramB = 0
    tramC = 0

    result = Trajet.objects.allow_filtering().filter(
        day=datetime.strptime(today.strftime("%m/%d/%Y %I:%M %p"), "%m/%d/%Y %I:%M %p"),
        read=False)

    for record in result:
        if record.tram == 'Tram A' and record.users > tramA:
            tramA = record.users
        elif record.tram == 'Tram B' and record.users > tramB:
            tramB = record.users
        elif record.tram == 'Tram C' and record.users > tramC:
            tramC = record.users
        record.read = True
        record.save()
    
    return { 'axis': hour, 'tramA': tramA, 'tramB': tramB, 'tramC': tramC }


def load_stations():
    stations = Station.objects.all()
    stationNames = []
    for station in stations:
        if station.name not in stationNames:
            stationNames.append(station.name)
    return stationNames

def load_station_usage(stationName):
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    startDate = today - timedelta(days=10)
    days = []
    users = []

    for i in range(0, 10):
        day = startDate + timedelta(days=i)
        days.append(day.strftime("%m/%d/%Y"))
        result = Station.objects.allow_filtering().filter(
            day=datetime.strptime(day.strftime("%m/%d/%Y %I:%M %p"), "%m/%d/%Y %I:%M %p"),
            name = stationName
            )
        if result:
            users.append(max([x.users for x in result]))
        else:
            users.append(0)

    return { 'axis': days, 'users': users }

def load_station_usage_now(station):
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    hour = datetime.now().strftime("%H:%M:%S")
    users = 0
    
    result = Station.objects.allow_filtering().filter(
        day=datetime.strptime(today.strftime("%m/%d/%Y %I:%M %p"), "%m/%d/%Y %I:%M %p"),
        read=False,
        name=station
        )
    if result:
        for record in result:
            if record.users > users:
                users = record.users
            record.read = True
            record.save()
    else:
        users= 0
    
    return { 'axis': hour, 'users': users }