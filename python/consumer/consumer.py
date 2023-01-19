import json
from time import sleep
import time

from kafka import KafkaConsumer

from model import Validation

from cassandra.cluster import Cluster
import pandas as pd
import numpy as np

from consumer_config import CASSANDRA_SERVICE_NAME, CASSANDRA_PORT, CASSANDRA_KEYSPACE, KAFKA_URL


# function which checks whether it's a new client or not
def is_new_client(session, id_client):
    # check if the received id exists in clients table
    user = None
    while True:
        try:
            user = pd.DataFrame(list(session.execute(f"SELECT id_client FROM client where \
            id_client = '{id_client}' allow filtering;")))
            break
        except:
            print("Connection to the cluster failed, retrying in 5 seconds")
            time.sleep(5)

    if (user.shape[0] == 0):
        return True
    else:
        return False


# save the new record
def save_new_record(session, record):
    new_id = record["client"]["id"]

    # if it's a new client
    if (is_new_client(session, new_id) == True):
        # save the client
        client_id = record["client"]["id"]
        first_name = record["client"]["firstName"]
        last_name = record["client"]["lastName"]
        # last_index='null'
        # top_line='null'
        session.execute(f"INSERT INTO client(id_client, first_name , last_index , last_name ,top_line )\
            VALUES ('{client_id}', '{first_name}',null,'{last_name}',null);")
        # save the card
        id_card = record["card"]["id"]
        end_of_validity = record["card"]["endOfValidityTimeStamp"]
        type = record["card"]["type"]
        session.execute(f"INSERT INTO card(id_card ,end_of_validity,id_client,type)\
            VALUES ('{id_card}', '{end_of_validity}','{client_id}','{type}');")
        # save the event

        # get the last event_index
        last_index = pd.DataFrame(list(session.execute(f"SELECT max(id_event) as max FROM Event;")))['max']
        index = 0
        if (last_index[0] != None):
            index = int(last_index[0]) + 1

        arrival_station = str(record["destinationStationId"]).replace("'", "")
        id_card = record["card"]["id"]
        id_user = record["client"]["id"]
        line = record["line"]
        start_station = str(record["stationId"]).replace("'", "")
        timestamp = record["requestTimeStamp"]

        session.execute(f"INSERT INTO event(id_event,end_station,id_card,id_user,line,start_station,timestamp)\
            VALUES ({index}, '{arrival_station}','{id_card}','{id_user}','{line}','{start_station}','{timestamp}');")

    else:  # if it's an old client
        # save the event
        last_index = pd.DataFrame(list(session.execute(f"SELECT max(id_event) as max FROM Event;")))['max']
        index = 0
        if (last_index[0] != None):
            index = int(last_index[0]) + 1

        arrival_station = str(record["destinationStationId"]).replace("'", "")
        id_card = record["card"]["id"]
        id_user = record["client"]["id"]
        line = record["line"]
        start_station = str(record["stationId"]).replace("'", "")
        timestamp = record["requestTimeStamp"]

        session.execute(f"INSERT INTO event(id_event,end_station,id_card,id_user,line,start_station,timestamp)\
            VALUES ({index}, '{arrival_station}','{id_card}','{id_user}','{line}','{start_station}','{timestamp}');")


if __name__ == "__main__":

    cluster = None
    session = None
    while True:
        try:
            print("Connecting to the Cluster..........")
            cluster = Cluster([CASSANDRA_SERVICE_NAME], port=CASSANDRA_PORT)
            session = cluster.connect(CASSANDRA_KEYSPACE)
            break
        except:
            print("Connection to the cluster failed, retrying in 5 seconds")
            time.sleep(5)

    print("connecting to kafka cluster.........")
    consumer = None
    while True:
        try:
            consumer = KafkaConsumer(bootstrap_servers=KAFKA_URL)
            break
        except:
            print("Connection to the kafka cluster failed, retrying in 5 seconds")
            time.sleep(5)

    consumer.subscribe(topics=["temp-topic"])

    print("receiving data............")
    i = 1
    for msg in consumer:
        record: Validation = json.loads(msg.value)
        print(f"record {i}\n")
        save_new_record(session, record)
        i += 1
        # id_event = v[""]
        # arrival_station = v["destinationStationId"]
        # id_card = v["card"]["id"]
        # id_user = v["client"]["id"]
        # line = v["line"]
        # start_station = v["stationId"]
        # timestamp = v["requestTimeStamp"]
    # print(f"{arrival_station}, {id_card}, {id_user},{line},{start_station},{timestamp}")

    # {'card': {'endOfValidityTimeStamp': '2023-01-19T15:13:12', 'id': 'fe170a92d4ac40d0bd24b2582e4d8368', 'type': 'Month'},
    # 'client': {'firstName': 'Estella', 'id': '52cfa110c9624bf3be37613a5eb5b87f', 'lastName': 'Daniel'},
    # 'destinationStationId': 'Albert 1er de Belgique', 'line': 'A', 'requestTimeStamp': '2022-12-25T11:48:26.645064',
    #  'stationId': 'Auguste Delaune'}
