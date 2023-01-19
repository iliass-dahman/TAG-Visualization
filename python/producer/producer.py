from datetime import datetime
from time import sleep

from kafka import KafkaProducer

from model import Validation, Client, Card, Lines
from producer_config import BROKER_URL, KAFKA_VALIDATION_TOPIC

producer = None
while True:
    try:
        print("Connecting to the Kafka..........")
        producer = KafkaProducer(bootstrap_servers=BROKER_URL)
        break
    except:
        sleep(1)
        print("Connection to the Kafka failed, retrying in 5 seconds")

print("Connected to the Kafka" + str(producer))
s = "hello"
while True:
    print("Sending message to the Kafka..........")
    line = Lines.random_line()
    print(line)
    validation = Validation(Client.random(), Card.random(), line.random_station(), line.id, line.random_station(),
                            datetime.now().isoformat())
    print(validation)
    producer.send(KAFKA_VALIDATION_TOPIC, bytes(validation.to_json(), encoding='utf-8'))
    print("Message sent to the Kafka..........")
    sleep(5)
