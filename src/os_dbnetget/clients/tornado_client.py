import datetime
import functools
import logging
import random
import socket
from datetime import timedelta

from tornado import gen, queues
from tornado.iostream import StreamClosedError
from tornado.locks import Lock
from tornado.tcpclient import TCPClient
from tornado.util import TimeoutError

from os_dbnetget.clients.client import RETRY_NETWORK_ERRNO, Client
from os_dbnetget.exceptions import (ResourceLimit, RetryLimitExceeded,
                                    ServerClosed, Unavailable)
from os_dbnetget.utils import split_endpoint

socket.setdefaulttimeout(10)


class TornadoClient(Client):
    def __init__(self, address, port, **kwargs):
        super(TornadoClient, self).__init__(address, port, **kwargs)
        self._timeout = kwargs.get('timeout', socket.getdefaulttimeout())
        assert self._timeout > 0, 'timeout must be negative'
        self._connect_timeout = kwargs.get('connect_timeout', self._timeout)
        assert self._connect_timeout > 0, 'connect_timeout must be negative'
        self._recv_timeout = kwargs.get('recv_timeout', self._timeout)
        assert self._recv_timeout > 0, 'recv_timeout must be negative'
        self._retry_max = kwargs.get('retry_max', 3)
        assert 0 <= self._retry_max <= 120, 'retry_max must be [0, 120]'
        self._retry_interval = kwargs.get('retry_interval', 5)
        assert self._retry_interval >= 0, 'retry_interval must be non-negative'
        self._retry_count = -1
        self._stream = None
        self._closed = False
        self._logger = logging.getLogger(self.__class__.__name__)

    @gen.coroutine
    def _reconnect(self):
        while self._retry_count < self._retry_max:
            self.__close_stream()
            try:
                self._stream = yield TCPClient().connect(self._address, self._port,
                                                         timeout=self._connect_timeout)
                break
            except (StreamClosedError, TimeoutError) as e:
                raise_e = False
                if self._retry_max <= 0:
                    raise_e = True
                if not isinstance(e, TimeoutError):
                    e = e.real_error
                    if e.args[0] not in RETRY_NETWORK_ERRNO:
                        raise_e = True
                else:
                    e = TimeoutError('time out')

                self._logger.debug('Connect error {}:{}, {}'.format(
                    self._address, self._port, e))
                if raise_e:
                    raise e

                self._retry_count += 1
                if self._retry_count < self._retry_max:
                    self._logger.debug('Connect retry {}:{} in {}s, {}/{}'.format(
                        self._address, self._port, self._retry_interval,
                        self._retry_count + 1, self._retry_max))
                    yield gen.sleep(self._retry_interval)

        self.__ensure_not_closed()
        if self._retry_count >= self._retry_max:
            raise RetryLimitExceeded(
                'Exceed retry limit {}/{}'.format(self._retry_count, self._retry_max))
        self._retry_count = -1

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
                self._logger.warning('Network error {}:{} {}'.format(
                    self._address, self._port, e))
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
        self._create_lock = Lock()
        self._close_lock = Lock()
        self._clients_count = 0
        self._closed = False
        self._closing = False
        self._started = False
        self._logger = logging.getLogger(self.__class__.__name__)

    @gen.coroutine
    def _create_client(self):
        with (yield self._create_lock.acquire()):
            self.__ensure_not_closed()
            self.__ensure_not_closing()

            while len(self._candidates) > 0:
                if self._clients.qsize() > 0:
                    return
                endpoint = random.sample(self._candidates.keys(), 1)[0]
                if self._candidates[endpoint] <= 0:
                    self._candidates.pop(endpoint)
                    continue

                address, port = split_endpoint(endpoint)
                client = TornadoClient(address, port, **self._kwargs)
                self._candidates[endpoint] -= 1
                if self._candidates[endpoint] <= 0:
                    self._candidates.pop(endpoint)
                yield self._clients.put(client)
                self._clients_count += 1
                self._logger.debug('Create a new client {}'.format(endpoint))
                return

            raise ResourceLimit('No more available endpoint')

    def _exhausted(self):
        return self._clients_count <= 0 and len(self._candidates) <= 0

    def __ensure_not_closing(self):
        if self._closing:
            raise Unavailable('Closing')

    def __ensure_not_closed(self):
        if self._closed:
            raise Unavailable('Closed')

    def __ensure_not_exhuasted(self):
        if self._exhausted():
            raise ResourceLimit('No more available client')

    @gen.coroutine
    def execute(self, qdb_proto):
        while True:
            self.__ensure_not_closed()
            self.__ensure_not_closing()
            self.__ensure_not_exhuasted()
            if not self._started:
                try:
                    yield self._create_client()
                except:
                    continue
                finally:
                    self._started = True
            try:
                client = yield self._clients.get(timeout=timedelta(seconds=1))
            except TimeoutError as e:
                try:
                    yield self._create_client()
                except ResourceLimit:
                    pass
                continue

            try:
                r = yield client.execute(qdb_proto)
                yield self._clients.put(client)
            except (RetryLimitExceeded, StreamClosedError,
                    TimeoutError, socket.gaierror) as e:
                self._logger.warning(
                    'Not available, {} {}'.format(client.endpoint, e))
                self._release_client(client)
                continue

            except Exception as e:
                self._logger.error(
                    'Unexpected error, {} {}'.format(client.endpoint, e))
                self._release_client(client)
                continue
            finally:
                self._clients.task_done()

            raise gen.Return(r)

    def _release_client(self, client):
        try:
            if client:
                client.close()
        finally:
            self._clients_count -= 1

    @gen.coroutine
    def close(self):
        with (yield self._close_lock.acquire()):
            if self._closed:
                return

            self._closing = True
            with (yield self._create_lock.acquire()):
                while self._clients_count > 0:
                    try:
                        client = yield self._clients.get(timeout=timedelta(seconds=0.1))
                    except TimeoutError:
                        continue

                    try:
                        self._release_client(client)
                    finally:
                        self._clients.task_done()
                self._closed = True
            self._closing = False
