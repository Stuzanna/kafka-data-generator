import json
import logging
import os
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from faker import Faker

from adminTools import callback, create_topic, topic_exists
from producer_tools import load_schema, create_serializer, produce_record
from customerProducer import produceCustomer
from orderProducer import producePizzaOrder
from pizzaProducer import PizzaProvider
from productProducer import produceProduct

# --- Define Inputs ---
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
print(f'KAFKA_BOOTSTRAP_SERVERS: {bootstrap_servers}')
schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8084')
print(f'SCHEMA_REGISTRY_URL: {schema_registry_url}')
serialization = os.getenv('SERIALIZATION', None)
print(f'SERIALIZATION: {serialization}')
schema_loc = os.getenv('SCHEMA_LOC', None)
print(f'SCHEMA_LOC: {schema_loc}')
schema_id = int(os.getenv('SCHEMA_ID')) if os.getenv('SCHEMA_ID') else None
print(f'SCHEMA_ID: {schema_id}')
subject = os.getenv('SUBJECT', None)
print(f'SUBJECT: {subject}')
schema_file_path = os.getenv('SCHEMA_FILE_PATH', None)
print(f'SCHEMA_FILE_PATH: {schema_file_path}')
topics = os.getenv('TOPICS', "customers,pizza-orders,products").split(',')
topics = [topic.strip() for topic in topics] #Strip any extra whitespace
print(f'TOPICS: {topics}')
max_batches = int(os.getenv('MAX_BATCHES', 500))
print(f'MAX_BATCHES: {max_batches}')
messageDelaySeconds = float(os.getenv('MESSAGE_DELAY_SECONDS', 2))
print(f'MESSAGE_DELAY_SECONDS: {messageDelaySeconds}')
new_topic_replication_factor = int(os.getenv('NEW_TOPIC_REPLICATION_FACTOR', 3))
print(f'NEW_TOPIC_REPLICATION_FACTOR: {new_topic_replication_factor}')
new_topic_partitions = int(os.getenv('NEW_TOPIC_PARTITIONS', 3))
print(f'NEW_TOPIC_PARTITIONS: {new_topic_partitions}')
cluster_sizing = os.getenv('CLUSTER_SIZING', None) # 'small' for demos with 1 broker
if cluster_sizing == 'small':
    new_topic_replication_factor = 1
    print(f'''CLUSTER_SIZING: {cluster_sizing}.
          Set NEW_TOPIC_REPLICATION_FACTOR: {new_topic_replication_factor}.'''
         )

config = {
    'bootstrap.servers': bootstrap_servers,
}

schema_registry_conf = {'url': schema_registry_url, 'basic.auth.user.info': '<SR_UserName:SR_Password>'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# load schema, from remote schema registry or local file
if serialization in ['json', 'avro']:
    if schema_loc == 'local':
        schema_str = load_schema(schema_loc, schema_file_path)
    elif schema_loc == 'remote':
        schema_str = load_schema(schema_loc, None, schema_id, subject, schema_registry_url)
    else:
        raise ValueError(f"Invalid schema location: {schema_loc}. Expected 'local' or 'remote'.")
    serializer = create_serializer(serialization, schema_str=schema_str, schema_registry_client=schema_registry_client)
    print(f"Success: Created {serialization.upper()} serializer.")
else:
    serializer = create_serializer(serialization, None, None)

# Instantiate Kafka producer and admin client
producer = Producer(config)

admin_client = AdminClient(config)

# Check topics exist, or create them
try:
    for topic in topics:
        print(f"Checking if topic '{topic}' exists.")
        if not topic_exists(admin_client, topic):
            create_topic(admin_client, topic, new_topic_partitions, new_topic_replication_factor)
        else:
            print(f"Topic '{topic}' already exists.")
except Exception as e:
        print(f"Failed to create topic {topic}: {e}. Exiting...")


# --- Enrich Faker with custom provider ---
fake = Faker()
provider = PizzaProvider # from the imported module
fake.add_provider(provider) 
# fake has been enriched with PizzaProvider and can now be referenced

# Produce messages
counter = 0

try:
    print("Start producing messages.")
    while True:
        while counter < max_batches:
            for topic in topics:
                if topic == 'customers':
                    payload = produceCustomer()
                elif topic == 'pizza-orders':
                    payload = producePizzaOrder(counter, fake)
                elif topic == 'products':
                    payload = produceProduct()
                else:
                    exit()

                key = next(iter(payload))
                message = payload[key]

                payload = {
                "key": key,
                "message": message
                }
                produce_record(producer, payload['message'], payload['key'], topic, serialization, serializer)
                producer.flush()

            counter += 1  
            time.sleep(messageDelaySeconds)
            if counter >= max_batches:
                producer.flush()
                print(f"Max batches ({max_batches}) reached, stopping producer.")
                break
        producer.flush()
except KafkaException as e:
    logging.error(f"Produce message error: {e}")

except KeyboardInterrupt:
    print("KeyboardInterrupt. Stopping the producer...")

finally:
    print("Attempting to flush the producer...")
    producer.flush()
    print("Producer has been flushed.")
