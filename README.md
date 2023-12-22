# kafka-data-generator
A data generator into Kafka.

A simple app for producing pizza orders into a Kafka topic. It is optimised for the docker-compose from this repo, including a local Kafka and a UI to see into Kafka.

# Get Started

Run `docker compose up -d` and navigate to [localhost:8080](http://localhost:8080) to see the Kafka topic with pizza order data using the Conduktor UI.

![viewMessage](/images/view-message.png)
*Messages produced, seen in the Conduktor UI*

![consumer-page](/images/consumer-page.png)
*An example message, seen in the Conduktor UI*

## Docker compose architecture
![simple-architecture](/images/simple-architecture.png)

# Changing configuration of the app

The configuration can be changed by modifying and rebuidling the app.

The following properties are set and can be modified by changing the initial section where inputs are defined in `main.py`.

```
bootstrap_servers = "redpanda-0:9092"
topic_name = "orders"
num_messages = 100
messageDelaySeconds = 2
```

If you wish to modify other properties for this producer, such as security, additional sections need to be added to the relevant section in `main.py`. This PoC is currently limited to no security, you can get this working by simply running `docker compose up -d` in the same folder as the compose to get a working Kafka cluster with the pizza orders produced and a UI running on `localhost:8080`.

## Rebuild the app

After saving the new config e.g. if a different bootstrap server, rebuild following the instructions in the README within `/app`.

The app is published on [Dockerhub](https://hub.docker.com/r/stuzanne/kafka-data-generator).


