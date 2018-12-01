import errno
import logging
import random
import socket
import sys
import threading
import time
from io import BytesIO

from ..exceptions import (ResourceLimit, RetryLimitExceeded, ServerClosed,
                          Unavailable)
from .client import RETRY_NETWORK_ERRNO, Client

_PY3 = sys.version_info[0] == 3

if _PY3:
    import queue as Queue
else:
    import Queue

socket.setdefaulttimeout(10)

_logger = logging.getLogger(__name__)


class SyncClient(Client):
    def __init__(self, address, port, **kwargs):
        super(SyncClient, self).__init__(address, port, **kwargs)
        self._timeout = kwargs.get('timeout', socket.getdefaulttimeout())
        self._retry_max = kwargs.get('retry_max', 3)
        assert self._retry_max >= 1
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
                if not isinstance(e, socket.timeout):
                    if e.args[0] not in RETRY_NETWORK_ERRNO:
                        raise e

                _logger.warn('Connect error: %s, retry in %ds, retry count %d' %
                             (str(e), self._retry_interval, self._retry_count))

                self._retry_count += 1
                if self._retry_count < self._retry_max:
                    time.sleep(self._retry_interval)

        if self._retry_count >= self._retry_max:
            raise RetryLimitExceeded('Exceed retry limit %d/%d' %
                                     (self._retry_count, self._retry_max))
        self._retry_count = 0

    def execute(self, qdb_proto):
        return qdb_proto
        if self._socket is None:
            self._reconnect()
        while True:
            try:
                return self._execute(qdb_proto)
            except (socket.timeout, socket.error) as e:
                _logger.warn('Network error: %s' % str(e))
                self._reconnect()

    def _execute(self, qdb_proto):
        for data in qdb_proto.upstream():
            self._socket.sendall(data)
        downstream = qdb_proto.downstream()
        read_size = next(downstream)
        while read_size > 0:
            data = self._recvall(self._socket, read_size)
            read_size = downstream.send(data)
        return qdb_proto

    def _recvall(self, s, size):
        buffer = BytesIO()
        left = size
        while left > 0:
            data = s.recv(left)
            if not data:
                raise ServerClosed
            buffer.write(s.recv(left))
            left = size - buffer.tell()
        buffer.seek(0)
        return buffer.read()

    def close(self):
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None
                self._retry_count = 0


class SyncClientPool(object):
    def __init__(self, endpoints, max_concurrency=1, **kwargs):
        self._endpoints = endpoints
        self._candidates = dict.fromkeys(self._endpoints, max_concurrency)
        self._max_concurrency = max_concurrency
        self._kwargs = kwargs
        self._clients = Queue.Queue()
        self._clients_count = 0
        self._create_lock = threading.RLock()
        self._close_lock = threading.RLock()
        self._closing = False
        self._create_client()

    def _split_endpoint(self, endpint):
        address, port = endpint.split(':')
        port = int(port)
        return address, port

    def _create_client(self):
        if self._closing:
            raise Unavailable('Closing')
        try:
            if not self._create_lock.acquire(False):
                return

            while len(self._candidates) > 0:
                endpoint = random.sample(self._candidates.keys(), 1)[0]
                if self._candidates[endpoint] <= 0:
                    self._candidates.pop(endpoint)
                    continue

                address, port = self._split_endpoint(endpoint)
                client = SyncClient(address, port, **self._kwargs)
                self._candidates[endpoint] -= 1
                if self._candidates[endpoint] <= 0:
                    self._candidates.pop(endpoint)
                self._clients.put(client)
                self._clients_count += 1
                return

            raise ResourceLimit('No more available endpoint')

        finally:
            self._create_lock.release()

    def _exhausted(self):
        return self._clients_count <= 0 and len(self._candidates) <= 0

    def execute(self, qdb_proto):
        while True:
            if self._closing:
                raise Unavailable('Closing')
            if self._exhausted():
                raise ResourceLimit('No more available client')
            try:
                client = self._clients.get(block=True, timeout=1)
                r = client.execute(qdb_proto)
                self._clients.put(client)
                return r
            except Queue.Empty:
                try:
                    self._create_client()
                except ResourceLimit:
                    continue

            except Exception as e:
                self._release_client(client)

    def _release_client(self, client):
        try:
            client.close()
        except Exception as e:
            pass
        finally:
            self._clients_count -= 1

    def close(self):
        try:
            if not self._close_lock.acquire(False):
                return
            self._closing = True
            while self._clients_count > 0:
                client = self._clients.get()
                self._release_client(client)
        finally:
            self._close_lock.release()
