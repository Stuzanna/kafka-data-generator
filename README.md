# kafka-data-generator
A data generator into Kafka.

A simple app for producing sample data into Kafka on three topics, `customers`, `products` and `pizza-orders`. A supporting docker-compose in this repo, includes a local Kafka and UI to see into Kafka.

# Get Started

Run `docker compose up -d` and navigate to [localhost:8080](http://localhost:8080) to see the Kafka topic with pizza order data using the Conduktor UI.

![viewMessage](/images/view-message.png)
*Messages produced, seen in the Conduktor UI*

![consumer-page](/images/consumer-page.png)
*An example message, seen in the Conduktor UI*

## Docker compose architecture
![simple-architecture](/images/kafka-data-generator-architecture.png)

# Changing configuration of the app

The configuration can be changed by setting environment variables.

Configuration options range from changing the bootstrap server and schema registry location, to serializing topics using Avro and local development schema files.

| Name                           | Default                           | Description                                                                                                                               |
| ------------------------------ | --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`      | `localhost:19092`                 | The bootstrap server(s) for your Kafka cluster.                                                                                           |
| `SCHEMA_REGISTRY_URL`          | `http://localhost:8084`           | URL for the Schema Registry service.                                                                                                      |
| `SERIALIZATION`                | `None`                            | Defines the serialization method for messages. You will need to add a schema to the schema registry for this. `avro` or `json` supported. |
| `SCHEMA_LOC`                   | `None`                            | Location of the schema definition. `remote` or `local` if using a local schema.                                                           |
| `SCHEMA_ID`                    | `None`                            | Numeric identifier for the schema. Must be an integer.                                                                                    |
| `SUBJECT`                      | `None`                            | Subject name for schema registry.                                                                                                         |
| `SCHEMA_FILE_PATH`             | `None`                            | File path to the schema definition file if local.                                                                                         |
| `TOPICS`                       | `customers,pizza-orders,products` | Comma-separated list of topics to use. Whitespace is automatically trimmed. Default topics with fake data provided.                       |
| `MAX_BATCHES`                  | `500`                             | Maximum number of messages batches to produce. Batch size being 1.                                                                        |
| `MESSAGE_DELAY_SECONDS`        | `2`                               | Delay between message productions in seconds (accepts decimal values).                                                                    |
| `NEW_TOPIC_REPLICATION_FACTOR` | `3`                               | Replication factor for new topics. Automatically set to 1 if `CLUSTER_SIZING` is 'small'.                                                 |
| `NEW_TOPIC_PARTITIONS`         | `3`                               | Number of partitions for new topics.                                                                                                      |
| `CLUSTER_SIZING`               | `None`                            | Cluster size configuration. Set to 'small' for demo environments with single broker.                                                      |

The app is published on [Dockerhub](https://hub.docker.com/r/stuzanne/kafka-data-generator).
