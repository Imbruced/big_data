import json

import requests
import attr

from config import NextBikeConf


@attr.s
class RequestHandler:

    def req(self, method: str, url: str, default_resp) -> requests.Response:
        resp = getattr(self, method)(url)
        if resp.status_code == 200:
            return resp.content
        else:
            return default_resp

    def get(self, url) -> requests.Response:
        try:
            resp = requests.get(url)
        except requests.exceptions.RequestException:
            resp = requests.Response()
        return resp


@attr.s
class NextBike:

    data = attr.ib()

    @classmethod
    def url(cls, url=getattr(NextBikeConf, "data_url")):
        return cls(RequestHandler().req("get", url, NextBike.empty()))

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
