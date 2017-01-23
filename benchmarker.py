import requests
import sys
import time

# topologyId = 'taxilocSample-2-1485122294'

topologyId = sys.argv[1]
starttime = int(sys.argv[2])

polltimes = [ 120, 150, 180, 210, 240, 270, 300]


def getCurrentStats():
    r = requests.get('http://ec2-34-249-3-36.eu-west-1.compute.amazonaws.com:8080/api/v1/topology/' + topologyId)
    print(r.json())



while True:
    if int(time.time()) - starttime in polltimes:
        getCurrentStats()


    time.sleep(0.3)




