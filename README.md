# AIC 2016 - Stream Processing

## tDatasource
For the submodule tDatasource you need a running python 3 installation or a virtual environment. It is dependant on the module
```confluent-kafka``` which is dependant on a C library called ```librdkafka``` .

usage: ap.py [MODE] [SPEED] 
Mode:
* continous
* realtime
* unify

output speed in seconds (can also be floating points)

## AIC_Storm 
This submodule contains all needed dependencies in the maven pom.xml file.

## Kafka/Zookeeper
necessary kafka & zookeeper containers are available as docker containers. 
You will need docker and docker-compose, after that just run docker-compose up (-d) -> -d for running in background

## starup order
* kafka/zookeeper
* tDatasource
* aic_storm


## webserver
* simply execute 'python3 app.py' to activate the server 
