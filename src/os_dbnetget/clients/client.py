import errno

RETRY_NETWORK_ERRNO = set([
    errno.ECONNREFUSED,
    errno.ECONNRESET,
    errno.ECONNABORTED,
    errno.ETIMEDOUT,
])


class Client(object):
    def __init__(self, address, port, **kwargs):
        self._address = address
        self._port = port

    def execute(self, qdb_proto):
        raise NotImplementedError

    def close(self):
        pass
