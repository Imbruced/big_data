import datetime
from flask import Flask, Response
from kafka import KafkaConsumer

# Fire up the Kafka Consumer
topic = "next_bike"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['localhost:9092'])


# Set the consumer in a Flask App
app = Flask(__name__)


@app.route('/', methods=['GET'])
def video():
    def generate():
        for message in consumer:
            yield '<br> ' + str(message.value) + ' </br>'
    return Response(generate(), mimetype="text/csv")

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)