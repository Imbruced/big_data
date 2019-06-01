import json
import random

from kafka import KafkaProducer


class KafkaConnector:

    """
    TODO observe error messages and create more precise exceptions
    """

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self, *args, **kwargs):
        try:
            producer = KafkaProducer(bootstrap_servers=kwargs.get("bootstrap_servers", ["localhost:9092"]),
                                     api_version=kwargs.get("api_version", (0, 10)))
        except Exception as e:
            raise ConnectionError("Can not connect to kafka server")
        self.producer = producer
        return self

    def __exit__(self, *args, **kwargs):
        self.producer.close()

    def send_message(self, topic_name: str, key: str, value: str):
        try:
            key_bytes = bytes(key, encoding='utf-8')
            value_bytes = bytes(value, encoding='utf-8')
            self.producer.send(topic_name, key=key_bytes, value=value_bytes)
            self.producer.flush()
        except Exception as ex:
            print(ex)
            print('Exception in publishing message')


lat = random.randint(40, 60) + random.randint(1, 10000)/10000
lon = random.randint(15, 25) + random.randint(1, 10000)/10000

data = dict(
    lat=lat,
    lon=lon
)


with KafkaConnector() as kaf:
    kaf.send_message('test1', '1', json.dumps(data).strip())


