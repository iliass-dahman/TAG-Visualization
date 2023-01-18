from datetime import datetime
from time import sleep

from kafka import KafkaProducer

from model import Validation, Client, Card, Lines

producer = None
while True:
    try:
        print("Connecting to the Kafka..........")
        producer = KafkaProducer(bootstrap_servers='broker:9092')
        break
    except:
        print("Connection to the Kafka failed, retrying in 5 seconds")

print("Connected to the Kafka"+str(producer))
s = "hello"
while True:
    print("Sending message to the Kafka..........")
    line = Lines.random_line()
    print(line)
    validation = Validation(Client.random(), Card.random(), line.random_station(), line.id, line.random_station(),
                            datetime.now().isoformat())
    print(validation)             
    producer.send('temp-topic', bytes(validation.to_json(), encoding='utf-8'))
    print("Message sent to the Kafka..........")
    sleep(5)
