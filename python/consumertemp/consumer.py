import json
from time import sleep

from kafka import KafkaConsumer

from consumer_config import BROKER_URL
from model import Validation

while True:
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=BROKER_URL, auto_offset_reset='earliest')
    except:
        print("waiting for 5s")
        sleep(5)
consumer.subscribe(topics=[KAFKA_VALIDATION_TOPIC])
counter = 0
for msg in consumer:
    counter += 1
    if counter == 8:
        break
    v: Validation = json.loads(msg.value)
    print(v["client"])
    print(v)
