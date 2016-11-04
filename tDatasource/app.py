from queue import Queue
from time import sleep, time

from os.path import isfile, join

import os

from TaxiLocation import TaxiLocation
from confluent_kafka import Producer

datapoints = []
path = '../data/06'

for file in os.listdir(path):
    if isfile(join(path, file)):
        print('reading file: ' + file)
        with open(join(path, file), 'r') as filehandle:
            for line in filehandle:
                datapoints.append(TaxiLocation(line))

datapoints.sort(key=lambda x: x.timestamp, reverse=False)

currentTime = datapoints[0].timestamp - 3

speed = .1

while True:

    while datapoints[0].timestamp <= currentTime:
        print('current time : ' + str(currentTime))
        print('sending ' + str(datapoints[0].timestamp))
        p = Producer({'bootstrap.servers': 'localhost'})
        p.produce('taxilocs', datapoints[0].json())
        p.flush()
        del datapoints[0]

    currentTime += 1
    sleep(1*speed)
