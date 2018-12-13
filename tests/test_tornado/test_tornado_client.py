import socket

from tornado.testing import AsyncTestCase, gen_test

import pytest
from os_dbnetget.clients.tornado_client import TornadoClient, TornadoClientPool
from os_dbnetget.exceptions import RetryLimitExceeded, ResourceLimit


class TestTornadoClient(AsyncTestCase):
    @gen_test
    def test_client_with_wrong_endpoint(self):
        host = 'notexisthostname.com'
        port = 8012
        c = TornadoClient(host, port)
        with pytest.raises(socket.gaierror):
            yield c.execute(None)

    @gen_test
    def test_client_connect_fail(self):
        host = 'localhost'
        port = 8012
        c = TornadoClient(host, port, retry_max=1, retry_interval=0)

        with pytest.raises(RetryLimitExceeded):
            yield c.execute(None)

    @gen_test
    def test_client_pool_try_out(self):

        endpoints = ['localhost:8011', 'localhost:8012']
        pool = TornadoClientPool(endpoints, retry_max=1, retry_interval=0)
        with pytest.raises(ResourceLimit):
            yield pool.execute(None)
