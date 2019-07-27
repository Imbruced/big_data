import json

from kafka import KafkaProducer

from scraping import NextBikeCity
from config import logger


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


if __name__ == "__main__":
    with KafkaConnector() as kaf:
        next_bike = NextBikeCity.url()
        while True:
            for index, el in next_bike.stream():
                logger.info(f"{index}, {el}")
                kaf.send_message('next_bike', str(index), json.dumps(el).strip())


