import json
from pprint import pprint
import time

import requests
import attr

from config import logger
from config import LivyStatementConf
from config import LivySessionConf
from lv.text_examples import code


@attr.s
class LivySession:
    livy_session_id = attr.ib(default=None)
    host = attr.ib(default=getattr(LivySessionConf, "host"))
    is_running = attr.ib(default=False)
    session_id = attr.ib(default=None, validator=[attr.validators.instance_of(int)])

    def kill(self):
        pass

    def __attrs_post_init__(self):
        self.session_name = getattr(LivySessionConf, "session_name")
        self.session_url = f"{self.host}/{self.session_name}/{str(self.session_id)}"

    @classmethod
    def create_new(cls,
                   host=getattr(LivySessionConf, "host"),
                   data=getattr(LivySessionConf, "data"),
                   headers=getattr(LivySessionConf, "headers")):
        response = requests.post(host + "/" + getattr(LivySessionConf, "session_name"),
                                 data=json.dumps(data),
                                 headers=headers)
        print(response.status_code)
        if response.status_code == 201:
            sesion_id = response.json()["id"]
            return cls(sesion_id, host, True, sesion_id)
        else:
            logger.info(f"Current code is {response.status_code}")
            raise ConnectionError("Response does not return code 201")

    @classmethod
    def from_existing(cls, host=getattr(LivySessionConf, "host"), session_id=0):
        return cls(session_id, host, True, session_id)


@attr.s
class LivyStatement:
    livy_session: LivySession = attr.ib()
    statement_id = attr.ib(default=None)

    def __attrs_post_init__(self):
        self.statements_url = self.livy_session.session_url + '/statements'
        self.statement_url = self.livy_session.session_url + '/statements' + f"/{self.statement_id}"

    def get_status(self):
        r = requests.get(self.statement_url,
                         headers=getattr(LivySessionConf, "headers"))
        if r.status_code != 200:
            logger.error("Response code is not equal to 200")
            raise ConnectionError("Request returned code: {r.status_code}")

        return r.json()

    @classmethod
    def from_file(cls, livy_session: LivySession, file_path: str):
        with open(file_path) as file:
            code = "\n".join(file.readlines())

        return cls.from_text(livy_session, code)

    @classmethod
    def from_text(cls, livy_session: LivySession, code: str, ):
        data = {'code': code}
        statement_id = cls.send_code(
            livy_session.host,
            livy_session.session_id,
            data,
            getattr(LivySessionConf, "headers"),
            getattr(LivyStatementConf, "statement_name")
        )

        return cls(livy_session, statement_id)

    @classmethod
    def send_code(cls, host: str, session_id: int, data: dict, headers: dict, statement_name:str):
        statement_url = cls.create_statements_url(
            host=host,
            session_id=session_id,
            statement_name=statement_name
        )
        response = requests.post(statement_url,
                                 data=json.dumps(data),
                                 headers=headers)
        if response.status_code != 201:
            logger.error(f"Returned code {response.status_code}")
            raise ConnectionError("Connection is failed")

        return response.json()["id"]

    @staticmethod
    def create_statements_url(host, session_id, statement_name):
        return f"{host}/sessions/{session_id}/{statement_name}"

# ls = LivySession.create_new()
ls = LivySession.from_existing(session_id=0)

# cd = code["geospark_prep"]

cd = "pGeom.show()"

lvst = LivyStatement.from_text(ls, cd)
statement_code = lvst.statement_id
state = "running"

while state != "available":
    lvst = LivyStatement(ls, statement_code)
    gstatus = lvst.get_status()
    state = gstatus["state"]
    progress = gstatus["progress"]

    try:
        stat = gstatus["output"]["status"]
    except TypeError:
        stat = "Unavialbale"

    logger.info(f"Status: {stat} | State: {state} | Progress: {progress}")

    time.sleep(2)
    if stat == "error":
        pprint(gstatus["output"])
# pprint(lvst.get_status())