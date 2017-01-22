from confluent_kafka import Consumer, KafkaException, KafkaError
from flask import Flask, render_template, Response

app = Flask(__name__)


@app.route('/')
def index():
    return render_template("dashboard.html")

if __name__ == '__main__':
    app.run(threaded=True)
