import json
import pprint
import requests
from IPython import display
import attr
import urllib.parse
from lv.logs import logger


class SessionNotExistError(Exception):
    pass


@attr.s
class LivySession:
    livy_session_ids = []
    host = attr.ib()
    is_running = attr.ib(default=False)
    session_id = attr.ib(default=None, validator=[attr.validators.instance_of(int)])

    def kill(self):
        pass

    def __attrs_post_init__(self):
        self.session_url = urllib.parse.urljoin(self.host, str(self.session_id))

    @classmethod
    def create_new(cls, host, data, headers):
        response = requests.post(host + '/sessions',
                                 data=json.dumps(data),
                                 headers=headers)
        if response.status_code == 201:
            sesion_id = response.json()["id"]
            cls.livy_session_ids.append(sesion_id)
            return cls(host, True, sesion_id)
        else:
            logger.info(f"Current code is {response.status_code}")
            raise ConnectionError("Response does not return code 200")

    @classmethod
    def from_existing(cls, host, session_id):
        if session_id not in cls.livy_session_ids:
            raise SessionNotExistError("This session id does not exists")

        return cls(host, True, session_id)


@attr.s
class LivyStatement:
    livy_session: LivySession = attr.ib()
    is_send = attr.ib(default=False, init=False)
    statement_id = attr.ib(default=None, init=False)

    def __attrs_post_init__(self):
        self.statements_url = self.livy_session.session_url + '/statements'

    def get_status(self):

        r = requests.get(statement_url, headers=headers)

        if r.status_code != 200:
            logger.error("Response code is not equal to 200")
            raise ConnectionError("Request returned code: {r.status_code}")
        pprint.pprint(r.json())

    def send_from_text(self, code):
        data = {'code': code}
        if not self.is_send:
            self.is_send = True
            self.statement_id = self.send_code(data, headers)
            self.statement_url = urllib.parse.urljoin(self.statements_url,
                                                      str(self.statement_id))
        else:
            logger.info("This statement was sent")

    def send_from_file(self, code):
        """TODO add from file"""

    @staticmethod
    def send_code(data, headers={'Content-Type': 'application/json'}):
        response = requests.post(statements_url,
                                 data=json.dumps(data),
                                 headers=headers)

        if response.status_code != 201:
            logger.error(f"Returned code {response.status_code}")
            raise ConnectionError("Connection is failed")

        return r.json()["id"]
