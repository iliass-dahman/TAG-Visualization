import os


kafka_url = os.getenv('BROKER_NAME') + ":" + os.getenv('BROKER_PORT')
kafka_subs_topic = os.getenv('BROKER_SUBSCRIPTION_TOPIC')
kafka_usage_topic = os.getenv('BROKER_USAGE_TOPIC')
cassandra_url = os.getenv('CASSANDRA_URL')
cassandra_keyspace = os.getenv('CASSANDRA_KEYSPACE')