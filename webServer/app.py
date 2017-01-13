from confluent_kafka import Consumer, KafkaException, KafkaError
from flask import Flask, render_template, Response


app = Flask(__name__)

@app.route('/')
def index():
	return render_template("index.html")

@app.route('/stream')
def publish_hello():
	consumerConfig = {'bootstrap.servers': 'localhost', 'group.id': 'output', 'session.timeout.ms': 6000, 'default.topic.config': {'auto.offset.reset': 'smallest'}}
	consumer = Consumer(**consumerConfig)
	#topic = 'BoltOutput'
	topic = ['taxilocs']
	consumer.subscribe(topic)

	def events():
		i = 0
		while True:
			msg = consumer.poll()
			if msg.value() != "":
				yield 'data: {0}\n\n'.format(msg.value())

	return Response(events(), mimetype="text/event-stream")


if __name__=='__main__':
	app.run()
