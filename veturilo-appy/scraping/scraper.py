import json
import time

import requests
import attr

from config import NextBikeConf
from config import logger


@attr.s
class RequestHandler:

    def req(self, method: str, url: str, default_resp, **kwargs) -> requests.Response:
        resp = getattr(self, method)(url, **kwargs)
        if resp.status_code == 200:
            try:
                j_data = json.loads(resp.content)
            except json.JSONDecodeError:
                j_data = default_resp
        else:
            logger.warning("Can not load the data from website")
            j_data = default_resp

        return j_data

    def get(self, url, **kwargs) -> requests.Response:
        params = kwargs.get("params", {})
        try:
            resp = requests.get(url, params=params)
        except requests.exceptions.RequestException:
            resp = requests.Response()
        return resp


@attr.s
class NextBike:

    data = attr.ib()

    @classmethod
    def url(cls, url=getattr(NextBikeConf, "data_url"), **kwargs):
        return cls(RequestHandler().req("get", url, NextBike.empty(), **kwargs))

    @staticmethod
    def empty():
        """
        TODO add keys and values from nextbike site json to config.json
        :return:
        """
        return getattr(NextBikeConf, "default_resp")

    def stream(self):
        """
        TODO use kafka class and functions to append data to kafka
        :return:
        """

        for index, el in enumerate(self.data["countries"]):
            time.sleep(2)
            yield(index, el)

        logger.info("Sleeping 300 s")
        time.sleep(300)


@attr.s
class NextBikeCity(NextBike):

    data = attr.ib()
    city = attr.ib(default=210)

    @classmethod
    def url(cls, url=getattr(NextBikeConf, "data_url"), **kwargs):
        params = kwargs.get("params", {})
        city = kwargs.get("city", getattr(NextBikeConf, "default_city"))
        if "city" not in params:
            params["city"] = city

        return cls(RequestHandler().req("get", url, NextBike.empty(), params=params), city)

    def stream(self):
        """
        TODO use kafka class and functions to append data to kafka
        :return:
        """

        for el in self.data["countries"][0]["cities"][0]["places"]:
            index = el.pop("uid")

            yield (index, el)

        refresh_rate = int(self.data["countries"][0]["cities"][0]["refresh_rate"])
        logger.info(f"Sleeping {refresh_rate} s")
        time.sleep(refresh_rate)