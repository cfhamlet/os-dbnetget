import socket
import time
import errno
import logging
from io import BytesIO
from .exceptions import RetryError


RETRY_NETWORK_ERRNO = set([111, ])

socket.setdefaulttimeout(10)

_logger = logging.getLogger(__file__)


class Client(object):
    def __init__(self, address, port, **kwargs):
        self._address = address
        self._port = port

    def execute(self, qdb_proto):
        raise NotImplementedError

    def close(self):
        pass


class SyncClient(Client):
    def __init__(self, address, port, **kwargs):
        super(SyncClient, self).__init__(address, port, **kwargs)
        self._timeout = kwargs.get('timeout', None)
        self._retry_max = kwargs.get('retry_max', 3)
        self._retry_interval = kwargs.get('retry_interval', 5)
        self._retry_count = 0
        self._socket = None

    def _reconnect(self):
        while self._retry_count < self._retry_max:
            if self._socket is not None:
                self._socket.close()
                self._socket = None
            try:
                self._socket = socket.create_connection(
                    (self._address, self._port), timeout=self._timeout)
                break
            except socket.error as e:
                if not isinstance(e, socket.timeout) and not int(e.errno) in RETRY_NETWORK_ERRNO:
                    raise e

                _logger.debug('Network error %s, retry in %ds, retry count %d' %
                              (str(e), self._retry_interval, self._retry_count))
                self._retry_count += 1
                time.sleep(self._retry_interval)

        if self._retry_count >= self._retry_max:
            raise RetryError('Exceeded retry limit %d/%d' %
                             (self._retry_count, self._retry_max))
        self._retry_count = 0

    def execute(self, qdb_proto):
        if self._socket is None:
            self._reconnect()
        while True:
            try:
                self._execute(qdb_proto)
                break
            except (socket.timeout, socket.error) as e:
                _logger.debug('Network error %s' % str(e))
                self._reconnect()

    def _execute(self, qdb_proto):
        for data in qdb_proto.upstream():
            self._socket.sendall(data)
        downstream = qdb_proto.downstream()
        read_size = next(downstream)
        while read_size >= 0:
            data = self._recvall(self._socket, read_size)
            read_size = downstream.send(data)

    def _recvall(self, s, size):
        buffer = BytesIO()
        left = size
        while left > 0:
            buffer.write(s.recv(left))
            left = size - buffer.tell()
        buffer.seek(0)
        return buffer.read()

    def close(self):
        if self._socket is not None:
            self._socket.close()
