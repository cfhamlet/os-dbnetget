import datetime
import functools
import logging
import socket

from tornado import gen, queues
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.util import TimeoutError

from ..exceptions import RetryLimitExceeded, ServerClosed, Unavailable
from .client import RETRY_NETWORK_ERRNO, Client

socket.setdefaulttimeout(10)

_logger = logging.getLogger(__name__)


class TornadoClient(Client):
    def __init__(self, address, port, **kwargs):
        super(TornadoClient, self).__init__(address, port, **kwargs)
        self._timeout = kwargs.get('timeout', socket.getdefaulttimeout())
        self._connect_timeout = kwargs.get('connect_timeout', self._timeout)
        self._recv_timeout = kwargs.get('recv_timeout', self._timeout)
        self._retry_max = kwargs.get('retry_max', 3)
        assert self._retry_max >= 1
        self._retry_interval = kwargs.get('retry_interval', 5)
        self._retry_count = 0
        self._stream = None
        self._closed = False

    @gen.coroutine
    def _reconnect(self):
        while self._retry_count < self._retry_max:
            self.__ensure_not_closed()
            self.__close_stream()
            try:
                self._stream = yield TCPClient().connect(self._address, self._port,
                                                         timeout=self._connect_timeout)
                break
            except (StreamClosedError, TimeoutError) as e:
                if not isinstance(e, TimeoutError):
                    e = e.real_error
                    if e.args[0] not in RETRY_NETWORK_ERRNO:
                        raise e
                else:
                    e = TimeoutError('time out')

                _logger.warn('Connect error: %s, retry in %ds, retry count %d' %
                             (str(e), self._retry_interval, self._retry_count))

                self._retry_count += 1
                if self._retry_count < self._retry_max:
                    yield gen.sleep(self._retry_interval)

        self.__ensure_not_closed()
        if self._retry_count >= self._retry_max:
            raise RetryLimitExceeded('Exceed retry limit %d/%d' %
                                     (self._retry_count, self._retry_max))
        self._retry_count = 0

    @gen.coroutine
    def _execute(self, qdb_proto):
        self.__ensure_not_closed()
        for data in qdb_proto.upstream():
            yield self._stream.write(data)
        downstream = qdb_proto.downstream()
        read_size = next(downstream)
        while read_size > 0:
            data = yield gen.with_timeout(
                datetime.timedelta(seconds=self._recv_timeout),
                self._stream.read_bytes(read_size),
                quiet_exceptions=(StreamClosedError,),
            )
            read_size = downstream.send(data)
        raise gen.Return(qdb_proto)

    @gen.coroutine
    def execute(self, qdb_proto):
        if self._stream is None:
            yield self._reconnect()
        while True:
            try:
                r = yield self._execute(qdb_proto)
                raise gen.Return(r)
            except (TimeoutError, StreamClosedError) as e:
                _logger.warn('Network error: %s' % str(e))
                yield self._reconnect()

    def __ensure_not_closed(self):
        if self._closed:
            raise Unavailable('Client already closed')

    def __close_stream(self):
        if self._stream is not None:
            try:
                self._stream.close()
            finally:
                self._stream = None

    def close(self):
        self.__close_stream()
        self._closed = True


class TornadoClientPool(object):

    def __init__(self, endpoints, max_concurrency=1, **kwargs):
        self._endpoints = endpoints
        self._candidates = dict.fromkeys(self._endpoints, max_concurrency)
        self._kwargs = kwargs
        self._clients = queues.Queue()
        self._clients_count = 0
        self._closed = False
        self._create_client()

    def _create_client(self):
        pass

    def close(self):
        self._closing = True
        while self._clients_count > 0:
            client = self._clients.get()
            self._release_client(client)
