from kafka_connector.kc import KafkaConnector
from sklearn.datasets import load_diabetes
import pandas as pd
from config import logger
import random
import time
import json
import datetime


data = pd.DataFrame(load_diabetes()["data"]).reset_index()
data = data.rename(columns={"index": "id"})

with KafkaConnector() as kaf:
    while True:
        for record in data.to_dict(orient="records"):
            dt = str(datetime.datetime.now())
            index = record.pop("id")
            record["date"] = dt
            kaf.send_message("diabetes", str(1), json.dumps(record))

        # random_value = random.randint(3, 20)
        # logger.info(f"Sleeping {random_value}")
        # time.sleep(random_value)
