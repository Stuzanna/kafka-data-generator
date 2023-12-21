Credit to [Aiven tutorial](https://aiven.io/developer/teach-yourself-apache-kafka-and-python-with-a-jupyter-notebook) for the guide on their pizza generator that inspired this one.

# Docker Build Help
Commands used when building.

docker build -t kafka-data-generator:0.x -f ./Dockerfile ./

docker build -t stuzanne/kafka-data-generator:0.x -f ./Dockerfile ./
docker build -t stuzanne/kafka-data-generator:latest -f ./Dockerfile ./

docker tag stuzanne/kafka-data-generator:0.1 stuzanne/kafka-data-generator:latest

docker push stuzanne/kafka-data-generator:tagname
