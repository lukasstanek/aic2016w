from ws4py.client.threadedclient import WebSocketClient

class DummyClient(WebSocketClient):
    def opened(self):
		pass
    def closed(self, code, reason=None):
        print "Closed down", code, reason

    def received_message(self, m):
        print m
        if len(m) == 175:
            self.close(reason='Bye bye')

if __name__ == '__main__':
    try:
        ws = DummyClient('ws://localhost:7080/v2/broker/?topics=taxilocs', protocols=['kafka-text'])
        ws.connect()
        ws.run_forever()
    except KeyboardInterrupt:
        ws.close()
