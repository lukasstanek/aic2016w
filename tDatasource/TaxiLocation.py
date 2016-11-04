import datetime
import time
import re
import json

class TaxiLocation:
    def __init__(self, line):
        line = str.strip(line, '\n')
        splits = re.split(',', line)

        self.id = splits[0]
        timestamp = splits[1]
        self.timestamp = time.mktime(datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').timetuple())
        self.longitude = splits[2]
        self.latitude = splits[3]

    def print(self):
        print("ID: " + self.id + "\nTIME: " + str(self.timestamp) + "\nCOORDS: " + self.longitude + "/" + self.latitude)

    def json(self):
        payload = {'id':self.id, 'timestamp':self.timestamp, 'longitude':self.longitude, 'latitude':self.latitude}
        return json.dumps(payload)
