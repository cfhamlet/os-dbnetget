import socket

import pytest
from os_dbnetget.clients.sync_client import SyncClient, SyncClientPool
from os_dbnetget.exceptions import RetryLimitExceeded, ResourceLimit


def test_sync_client_with_wrong_endpoint():
    host = 'notexisthostname.com'
    port = 8012
    c = SyncClient(host, port)
    with pytest.raises(socket.gaierror):
        c.execute(None)


def test_sync_client_connect_fail(monkeypatch):
    def connect_timeout(self, sa):
        raise socket.timeout

    host = 'localhost'
    port = 8012
    with monkeypatch.context() as m:
        m.setattr(socket.socket, 'connect', connect_timeout)

        c = SyncClient(host, port, retry_max=0)
        with pytest.raises(socket.timeout):
            c.execute(None)

        c = SyncClient(host, port, retry_max=1, retry_interval=0)
        with pytest.raises(RetryLimitExceeded):
            c.execute(None)

    c = SyncClient(host, port, retry_max=0)
    with pytest.raises(socket.error):
        c.execute(None)

    c = SyncClient(host, port, retry_max=1, retry_interval=0)
    with pytest.raises(RetryLimitExceeded):
        c.execute(None)


def test_client_pool_try_out():
    endpoints = ['localhost:8011', 'localhost:8012']
    pool = SyncClientPool(endpoints, retry_max=1, retry_interval=0)
    with pytest.raises(ResourceLimit):
        pool.execute(None)
