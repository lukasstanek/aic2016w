from queue import Queue
from time import sleep, time

from os.path import isfile, join

import os

import sys

from TaxiLocation import TaxiLocation
from confluent_kafka import Producer


datapoints = []
path = '../data/'
filename = '06.sorted.txt'

def writeListToFile(datapoints):
    out = open(join('../data/06.sorted.txt'), 'w')

    for item in datapoints:
        out.write("%s,%s,%s,%s\n" % (item.id,  item.timeAsDate, item.latitude,  item.longitude))

    out.close()

def unifyInputs():
    for file in os.listdir(path + '06/'):
        if isfile(join(path + '06/', file)):
            print('reading file: ' + file)
            with open(join(path + '06/', file), 'r') as filehandle:
                for line in filehandle:
                    datapoints.append(TaxiLocation(line))

    writeListToFile(datapoints)



for arg in sys.argv:
    cmd = arg

if cmd == 'unify':
    print('unifying the folder')
    unifyInputs()
else:

    print('reading file: ' + filename)
    with open(join(path, filename), 'r') as filehandle:
        for line in filehandle:
            datapoints.append(TaxiLocation(line))


    datapoints.sort(key=lambda x: x.timestamp, reverse=False)



    currentTime = datapoints[0].timestamp - 3

    speed = .1

    while True:
        print('current time : ' + str(currentTime))

        while datapoints[0].timestamp <= currentTime:
            print('position updated for taxi #' + str(datapoints[0].id))
            print('lat: ' + str(datapoints[0].latitude) + ' - lon: ' + str(datapoints[0].longitude))
            p = Producer({'bootstrap.servers': 'localhost'})
            p.produce('taxilocs', datapoints[0].json())
            p.flush()
            del datapoints[0]

        currentTime += 1
        sleep(1*speed)
