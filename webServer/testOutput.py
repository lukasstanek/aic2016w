from confluent_kafka import Consumer, KafkaException, KafkaError



consumerConfig = {'bootstrap.servers': 'localhost', 'group.id': 'output', 'session.timeout.ms': 6000, 'default.topic.config': {'auto.offset.reset': 'smallest'}}
consumer = Consumer(**consumerConfig)
topic = ['BoltOutput']
consumer.subscribe(topic)

def events():
	i = 0
	while True:
		msg = consumer.poll()
		if msg.value() != "":
			print(msg.value())


events()

