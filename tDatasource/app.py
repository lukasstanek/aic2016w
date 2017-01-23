from queue import Queue
from time import sleep, time

from os.path import isfile, join

import os

import sys

from TaxiLocation import TaxiLocation
from confluent_kafka import Producer
import argparse


datapoints = []
datapath = 'data/'
#filename = '220.txt'
# filename = '06.sorted.txt'

def writeListToFile(datapoints):
    out = open(join('data/' + args.importpath + '.sorted.txt'), 'w')

    for item in datapoints:
        out.write("%s,%s,%s,%s\n" % (item.id,  item.timeAsDate, item.longitude,  item.latitude))

    out.close()

def unifyInputs():
    for file in os.listdir(datapath + args.importpath + '/'):
        if isfile(join(datapath + args.importpath + '/', file)):
            print('reading file: ' + file)
            with open(join(datapath + args.importpath + '/', file), 'r') as filehandle:
                for line in filehandle:
                    datapoints.append(TaxiLocation(line))
    datapoints.sort(key=lambda x: x.timestamp, reverse=False)

    writeListToFile(datapoints)


def cache_first_send_later(filehandle):
    locationList = []
    for line in filehandle:
        locationList.append(TaxiLocation(line))

    p = Producer({'bootstrap.servers': 'localhost'})
    
    for location in locationList:
        # print('emitting: ' + location.json())
        p.produce('taxilocs', location.json())

    p.flush()

def send_data_continously(filehandle):
    p = Producer({'bootstrap.servers': 'localhost'})

    for line in filehandle:
        location = TaxiLocation(line)
        print('emitting: ' + location.json())
        p.produce('taxilocs', location.json())
        p.flush()

        sleep(1*args.speed)

def send_data_realtime(filehandle):
    p = Producer({'bootstrap.servers': 'localhost'})
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
            sleep(1*args.speed)
            print('current time: ' + str(currentTime))

        print('emitting: ' + location.json())
        p.produce('taxilocs', location.json())
        p.flush()

def send_data():
    print('reading file: ' + args.filename)
    with open(join(datapath, args.filename), 'r') as filehandle:
        if args.mode == 'realtime':
            send_data_realtime(filehandle)
        elif args.mode == 'continuous':
            send_data_continously(filehandle)
        elif args.mode == 'inmemory':
            cache_first_send_later(filehandle)



parser = argparse.ArgumentParser()
parser.add_argument("-f", "--filename", type=str, help="specify filename for reading data from", default="06.sorted.txt")
parser.add_argument("-m", "--mode", type=str, help="specifies the mode the data should be emitted, in realtime or continuous", default="realtime")
parser.add_argument("-s", "--speed", type=float, help="specifies how long 1 second will be within the program", default=1)
parser.add_argument("-u", "--unify", help="activates unification mode", action="store_true")
parser.add_argument("-ip", "--importpath", help="folder which contains files to be merged", default="06")
args = parser.parse_args()

print("Program is now in " + args.mode + " mode")
print("Speed set to " + str(args.speed) + " seconds")
print("Filename is " + args.filename)
print("Is in unification mode " + str(args.unify))

if args.unify:
    print('unifying the folder')
    unifyInputs()
else:
    send_data()

