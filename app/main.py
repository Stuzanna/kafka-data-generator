import json
import random
import time

from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic


from faker import Faker

from pizzaProducer import PizzaProvider
from orderProducer import producePizzaOrder


# --- Define Inputs ---
bootstrap_servers = "redpanda-0:9092"
topic_name = "orders"
num_messages = 100
messageDelaySeconds = 2


# --- Define producer ---
# certsFolder = "/path/to/certs/"
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    # no security in this example, add if needed or consider for input config
    value_serializer=lambda v: json.dumps(v).encode('ascii'),
    key_serializer=lambda v: json.dumps(v).encode('ascii')

)
print("Producer defined.")


# --- Check topic created ---
print(f"Checking if topic '{topic_name}' exists.")
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

topic_exists = topic_name in admin_client.list_topics()

if not topic_exists:
    new_topic = NewTopic(name=topic_name, num_partitions=8, replication_factor=1)
    admin_client.create_topics(new_topics=[new_topic], validate_only=False)

    print(f"Topic '{topic_name}' created successfully.")
else:
    print(f"Topic '{topic_name}' exists. No need to create it.")


# --- Bring in Faker for data generation(want to do in Conda, doing in pip as know works) ---

fake = Faker()
provider=PizzaProvider # from the imported module
fake.add_provider(provider) 
# fake has been enriched with PizzaProvider and can now be referenced

print(f"Faker enriched with provider, '{provider}'.")


# --- Create the stream ---
i = 0

while i < num_messages:
    message, key = producePizzaOrder(i, fake)

    print("Sending: {}".format(message))

    producer.send(
        topic_name,
        key = key,
        value = message
                  )
    
    time.sleep(messageDelaySeconds)

    if (i % num_messages) == 0:
        producer.flush()
    i = i + 1

producer.flush()


