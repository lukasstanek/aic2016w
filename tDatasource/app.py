from queue import Queue
from time import sleep, time

from os.path import isfile, join

import os

import sys

from TaxiLocation import TaxiLocation
from confluent_kafka import Producer


datapoints = []
path = '../data/06/'
filename = '220.txt'
# filename = '06.sorted.txt'

def writeListToFile(datapoints):
    out = open(join('../data/06.sorted.txt'), 'w')

    for item in datapoints:
        out.write("%s,%s,%s,%s\n" % (item.id,  item.timeAsDate, item.longitude,  item.latitude))

    out.close()

def unifyInputs():
    for file in os.listdir(path + '06/'):
        if isfile(join(path + '06/', file)):
            print('reading file: ' + file)
            with open(join(path + '06/', file), 'r') as filehandle:
                for line in filehandle:
                    datapoints.append(TaxiLocation(line))
    datapoints.sort(key=lambda x: x.timestamp, reverse=False)

    writeListToFile(datapoints)

def send_data_continously(filehandle):
    for line in filehandle:
        location = TaxiLocation(line)

        print('emitting: ' + location.json())
        p = Producer({'bootstrap.servers': 'localhost'})
        p.produce('taxilocs', location.json())
        p.flush()

        sleep(1*speed)

def send_data_realtime(filehandle):
    firstLine = True
    currentTime = ''
    while True:
        line = filehandle.readline()
        if not line:
            break
        location = TaxiLocation(line)
        if firstLine:
            currentTime = location.timestamp - 3
            firstLine = False
        while location.timestamp > currentTime:
            currentTime += 1
            sleep(1*speed)
            print('current time: ' + str(currentTime))

        print('emitting: ' + location.json())
        p = Producer({'bootstrap.servers': 'localhost'})
        p.produce('taxilocs', location.json())
        p.flush()

def send_data():
    print('reading file: ' + filename)
    with open(join(path, filename), 'r') as filehandle:
        if mode == 'realtime':
            send_data_realtime(filehandle)
        elif mode == 'continous':
            send_data_continously(filehandle)


speed = 1
mode = 'realtime'

if len(sys.argv) > 1:
    mode = sys.argv[1]

if len(sys.argv) > 2:
    speed = float(sys.argv[2])

if mode == 'unify':
    print('unifying the folder')
    unifyInputs()
else:
    send_data()

