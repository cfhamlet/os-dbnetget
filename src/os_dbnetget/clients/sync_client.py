import errno
import logging
import random
import socket
import sys
import threading
import time
from io import BytesIO

from os_dbnetget.clients.client import RETRY_NETWORK_ERRNO, Client
from os_dbnetget.exceptions import (ResourceLimit, RetryLimitExceeded,
                                    ServerClosed, Unavailable)
from os_dbnetget.utils import Queue, split_endpoint

socket.setdefaulttimeout(10)


class SyncClient(Client):
    def __init__(self, address, port, **kwargs):
        super(SyncClient, self).__init__(address, port, **kwargs)
        self._timeout = kwargs.get('timeout', socket.getdefaulttimeout())
        assert self._timeout > 0, 'timeout must be negative'
        self._retry_max = kwargs.get('retry_max', 3)
        assert 0 <= self._retry_max <= 120, 'retry_max must be [0, 120]'
        self._retry_interval = kwargs.get('retry_interval', 5)
        assert self._retry_interval >= 0, 'retry_interval must be non-negative'
        self._logger = logging.getLogger(self.__class__.__name__)
        self._retry_count = -1
        self._socket = None
        self._closed = False

    def _reconnect(self):
        while self._retry_count < self._retry_max:
            self.__ensure_not_closed()
            self.__close_socket()
            try:
                self._socket = socket.create_connection(
                    (self._address, self._port), timeout=self._timeout)
                break
            except socket.error as e:
                raise_e = False
                if self._retry_max <= 0:
                    raise_e = True
                if not isinstance(e, socket.timeout):
                    if e.args[0] not in RETRY_NETWORK_ERRNO:
                        raise_e = True

                self._logger.debug('Connect error {}:{}, {}'.format(
                    self._address, self._port, e))
                if raise_e:
                    raise e
                self._retry_count += 1
                if self._retry_count < self._retry_max:
                    self._logger.debug('Connect retry {}:{} in {}s, {}/{}'.format(
                        self._address, self._port, self._retry_interval,
                        self._retry_count + 1, self._retry_max))
                    time.sleep(self._retry_interval)

        self.__ensure_not_closed()
        if self._retry_count >= self._retry_max:
            raise RetryLimitExceeded(
                'Exceed retry limit {}/{}'.format(self._retry_count, self._retry_max))
        self._retry_count = -1

    def __ensure_not_closed(self):
        if self._closed:
            raise Unavailable('Client already closed')

    def execute(self, qdb_proto):
        if self._socket is None:
            self._reconnect()
        while True:
            try:
                return self._execute(qdb_proto)
            except (socket.timeout, socket.error) as e:
                self._logger.warning('Network error {}:{} {}'.format(
                    self._address, self._port, e))
                self._reconnect()

    def _execute(self, qdb_proto):
        self.__ensure_not_closed()

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
            buffer.write(data)
            left = size - buffer.tell()
        buffer.seek(0)
        return buffer.read()

    def __close_socket(self):
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None

    def close(self):
        self.__close_socket()
        self._closed = True


class SyncClientPool(object):
    def __init__(self, endpoints, max_concurrency=1, **kwargs):
        self._endpoints = endpoints
        self._candidates = dict.fromkeys(self._endpoints, max_concurrency)
        self._kwargs = kwargs
        self._clients = Queue.Queue()
        self._clients_count = 0
        self._create_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._closing = False
        self._closed = False
        self._started = False
        self._logger = logging.getLogger(self.__class__.__name__)

    def _create_client(self):
        with self._create_lock:
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
                client = SyncClient(address, port, **self._kwargs)
                self._candidates[endpoint] -= 1
                if self._candidates[endpoint] <= 0:
                    self._candidates.pop(endpoint)
                self._clients.put(client)
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

    def execute(self, qdb_proto):

        while True:
            self.__ensure_not_closed()
            self.__ensure_not_closing()
            self.__ensure_not_exhuasted()
            if not self._started:
                try:
                    self._create_client()
                except:
                    continue
                finally:
                    self._started = True

            client = None
            try:
                client = self._clients.get(timeout=1)
                r = client.execute(qdb_proto)
                self._clients.put(client)
                return r
            except Queue.Empty:
                try:
                    self._create_client()
                except ResourceLimit:
                    continue
            except (RetryLimitExceeded, socket.error) as e:
                self._logger.warning(
                    'Not available, {} {}'.format(client.endpoint, e))
                self._release_client(client)
            except Exception as e:
                self._logger.error(
                    'Unexpected error, {} {}'.format(client.endpoint, e))
                self._release_client(client)

    def _release_client(self, client):
        try:
            if client:
                client.close()
        finally:
            self._clients_count -= 1

    def available(self):
        if self._closing or self._closed:
            return False
        return True

    def closed(self):
        return self._closed

    def close(self):
        with self._close_lock:
            if self._closed:
                return

            self._closing = True
            with self._create_lock:
                while self._clients_count > 0:
                    client = None
                    try:
                        client = self._clients.get(timeout=0.1)
                    except Queue.Empty:
                        pass
                    self._release_client(client)
                self._closed = True
            self._closing = False
