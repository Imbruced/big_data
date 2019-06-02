import json
import os
import attr


SCRIPT_PARH = os.path.split(os.path.realpath(__file__))[0]


def load_config():
    with open(os.path.join(SCRIPT_PARH, "config.json")) as file:
        return json.load(file)


class ConfMeta(type):

    def __new__(mcs, *args, **kwargs):
        c_ = super().__new__(mcs, *args, **kwargs)
        c_.name = c_.__name__[:-4]
        mcs.__assign_conf(c_)
        mcs.__assign_attrs(c_)
        return c_

    def __assign_attrs(cls):
        for key, value in cls.config.items():
            setattr(cls, key, value)

    def __assign_conf(cls):
        try:
            cls.config = load_config()[cls.name]
        except KeyError:
            cls.config = {}


@attr.s
class NextBikeConf(metaclass=ConfMeta):
    pass


@attr.s
class LivySessionConf(metaclass=ConfMeta):
    pass


@attr.s
class LivyStatementConf(metaclass=ConfMeta):
    pass

