from fastapi import APIRouter
from fastapi.requests import Request
import config
from confluent_kafka import Consumer
from persistence import save_to_db, load_new_subs, load_new_subs_now, load_trajets_usage, load_station_usage, load_stations, load_trajets_usage_now, load_station_usage_now

import time
import ast

route = APIRouter()

#consommer les messages

def consume_subs():

    #les nouveaux adhérents
    c=Consumer({'bootstrap.servers':config.kafka_url,'group.id':'python-consumer','auto.offset.reset':'earliest'})
    print('Les nouveaux adhérents--------------')
    #c.subscribe(['new_subs'])
    running = True
    try:
        while running:
            c.subscribe([config.kafka_subs_topic])
            msg=c.poll(1.0) #timeout
            if msg is None:
                print('No message received by consumer')
                running = True
                continue
            if msg.error():
                print('Error: {}'.format(msg.error()))
                continue
            else:
                data=msg.value().decode('utf-8')
                mydata = ast.literal_eval(data) #transformer en dictionnaire
                save_to_db('new_subs', mydata)
                print(mydata)
                print("--------------------------------------------------------------------------------------------------------------")
                #print(f"L'abonnement {abonnement} a eu le grand nombre d'adhérant avec un nombre de {largest_num}.")
                #print("--------------------------------------------------------------------------------------------------------------")
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

def consume_trams():

    #les nouveaux adhérents
    c=Consumer({'bootstrap.servers':config.kafka_url,'group.id':'python-consumer','auto.offset.reset':'earliest'})
    try:
        print('Les lignes les plus fréquentées--------------')
        running = True
        while running:
            time.sleep(5)
            c.subscribe([config.kafka_usage_topic])
            msg=c.poll(1.0) #timeout
            if msg is None:
                print("No message received by consumer")
                running = True
                continue
            if msg.error():
                print('Error: {}'.format(msg.error()))
                continue
            else:
                data=msg.value().decode('utf-8')
                mydata = ast.literal_eval(data)
                save_to_db('frequented_tram', mydata)
                print(mydata)
                print("--------------------------------------------------------------------------------------------------------------")
                #print(f"La ligne la plus fréquentée est {ligne} avec un nombre d'utilisateurs de {largest_num}.",date_time)
                #print("--------------------------------------------------------------------------------------------------------------")
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

@route.get('/new-subs')
async def get_new_subs(request: Request):
    result = []
    if request.query_params['type'] == "past days":
        result = load_new_subs()
    else:
        result = load_new_subs_now()
    return result

@route.get('/trajets')
async def get_frequented_tram(request: Request):
    result = []
    if request.query_params['type'] == "past days":
        result = load_trajets_usage()
    else:
        result = load_trajets_usage_now()
    return result

@route.get('/stations')
async def get_frequented_station(request: Request):
    result = []
    station = request.query_params['station']
    if request.query_params['type'] == "past days":
        result = load_station_usage(station)
    else:
        result = load_station_usage_now(station)
    return result



async def get_stations():
    result = load_stations()
    return result
