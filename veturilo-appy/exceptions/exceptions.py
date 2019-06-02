

class ImmutableException(Exception):

    def __init__(self, message):
        super().__init__(message)


class SessionNotExistError(Exception):
    pass
