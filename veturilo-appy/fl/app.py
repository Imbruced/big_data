import datetime
import time
from flask import Flask, Response
from kafka import KafkaConsumer
import json
from flask import Flask, render_template_string
from flask import stream_with_context

consumer = KafkaConsumer(
    "NextBike",
    bootstrap_servers=["localhost:9092"]
)

app = Flask(__name__)


@app.route("/bikes")
def server_1():
    def gen():
        template = html
        for el in data_stream(consumer):
            data = str([{"lat": el[0], "lon": el[1]}]).replace('\'', '')
            yield render_template_string(el, data=data)
            time.sleep(5)

    return Response(stream_with_context(gen()))


html = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Chart</title>
</head>
<body>
   <canvas id="myChart1" width="40px" height="40px"></canvas>
   <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.4.0/Chart.min.js"></script>
   <script>
   new Chart.Scatter(document.getElementById("myChart1"), {
   type: 'scatter',
   data: {
     datasets: [{
       label: 'Scatter Dataset',
       data: {{ data }},
       showLine: false,
       borderColor: "blue",
       backgroundColor: "blue"
     }]
   }
 });
  </script>
</body>
</html>
'''


def data_stream(consumer: KafkaConsumer):
    default_response = {"lat": 0, "lng": 0}
    for msg in consumer:
        time.sleep(10)
        try:
            j_data = json.loads(msg.value)
        except json.JSONDecodeError:
            j_data = default_response
        except ValueError:
            j_data = default_response

        yield str(j_data.get("lat")), str(j_data.get("lng"))


app.run(host='0.0.0.0', debug=True)
