
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from faker import Faker

from pizzaProducer import PizzaProvider
from orderProducer import producePizzaOrder
from customerProducer import produceCustomer
from productProducer import produceProduct

import json
import time

# --- Define Inputs ---
bootstrap_servers = "redpanda-0:9092"
topic_names = ["customers", "pizza-orders", "products"]
num_messages = 1000
messageDelaySeconds = 2


# --- Define producer ---
# certsFolder = "/path/to/certs/"
producer = KafkaProducer(
    bootstrap_servers = bootstrap_servers,
    # no security in this example, add if needed or consider for input config
    value_serializer = lambda v: json.dumps(v).encode('ascii'),
    key_serializer = lambda v: json.dumps(v).encode('ascii')

)
print("Producer defined.")


# --- Check topics created ---
for topic in topic_names:
    print(f"Checking if topic '{topic}' exists.")
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    topic_exists = topic in admin_client.list_topics()

    if not topic_exists:
        new_topic = NewTopic(name=topic, num_partitions=8, replication_factor=1)
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)

        print(f"Topic '{topic}' created successfully.")
    else:
        print(f"Topic '{topic}' exists. No need to create it.")


# --- Bring in Faker for data generation ---

fake = Faker()
provider = PizzaProvider # from the imported module
fake.add_provider(provider) 
# fake has been enriched with PizzaProvider and can now be referenced

# print(f"Faker enriched with provider, '{provider}'.")


# --- Create the stream ---
counter = 0

while counter < num_messages:
    
    key0, message0 = produceCustomer(counter, fake)
    key1, message1 = producePizzaOrder(counter, fake)
    key2, message2 = produceProduct(counter, fake)

    print(f"Sending to topic, '{topic_names[0]}', message:" + " {}".format(message0))
    print(f"Sending to topic, '{topic_names[1]}', message:" + " {}".format(message1))
    print(f"Sending to topic, '{topic_names[2]}', message:" + " {}".format(message2))

    producer.send(
        topic_names[0],
        key = key0,
        value = message0
                  )
    producer.send(
        topic_names[1],
        key = key1,
        value = message1
                  )
    producer.send(
        topic_names[2],
        key = key2,
        value = message2
                  )
    
    time.sleep(messageDelaySeconds)

    if (counter % num_messages) == 0:
        producer.flush()
    counter = counter + 1

producer.flush()
