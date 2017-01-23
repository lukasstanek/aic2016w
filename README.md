# AIC 2016 - Stream Processing

## Startup Instructions


The Startup in local mode should be done in multiple terminals, one for each System. Firstly 
it is required to startup the docker containers with the command:  
docker-compose up -d (-d can be omitted for non daemon mode)  

The next step is to create the datasource:  
```python tDatasource/app.py```

Then start the local Storm cluster:  
```java -jar AIC_Storm/target/AIC_Storm-1.0-SNAPSHOT-jar-with-dependencies.jar```

Afterwards the web ui can be started:  
```node --use_strict webServer/index.js```









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
* webserver
* dashboard


## dashboard
* simply browse to 'localhost:5000' to view the dashboard
