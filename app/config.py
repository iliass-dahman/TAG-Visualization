import os

kafka_url = os.getenv('BROKER_NAME') + ":" + os.getenv('BROKER_PORT')
kafka_subs_topic = os.getenv('BROKER_SUBSCRIPTION_TOPIC')
kafka_usage_topic = os.getenv('BROKER_USAGE_TOPIC')
CASSANDRA_PORT = os.getenv('CASSANDRA_PORT')
CASSANDRA_SERVICE_NAME = os.getenv('CASSANDRA_URL')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE')
