import socket
import struct

from os_qdb_protocal import create_protocal
from tornado import gen
from tornado.testing import AsyncTestCase, bind_unused_port, gen_test

import pytest
from os_dbnetget.clients.tornado_client import TornadoClient, TornadoClientPool
from os_dbnetget.commands.qdb import qdb_key
from os_dbnetget.exceptions import ResourceLimit, RetryLimitExceeded

from ..fake_server.tornado_qdb_server import QDBServer


class TestConnect(AsyncTestCase):
    @gen_test
    def test_client_with_wrong_endpoint(self):
        host = 'notexisthostname.com'
        _, port = bind_unused_port()
        c = TornadoClient(host, port)
        with pytest.raises(socket.gaierror):
            yield c.execute(None)

    @gen_test
    def test_client_connect_fail(self):
        host = 'localhost'
        c = TornadoClient(host, 8012, retry_max=1, retry_interval=0)

        with pytest.raises(RetryLimitExceeded):
            yield c.execute(None)

    @gen_test
    def test_client_pool_try_out(self):

        endpoints = ['localhost:{}'.format(i) for i in range(8012, 8014)]
        pool = TornadoClientPool(endpoints, retry_max=1, retry_interval=0)
        with pytest.raises(ResourceLimit):
            yield pool.execute(None)


class TestTornadoClient(AsyncTestCase):

    def setUp(self):
        super(TestTornadoClient, self).setUp()
        self.server = None

    def tearDown(self):
        self.stop_server()
        super(TestTornadoClient, self).tearDown()

    def stop_server(self):
        if self.server is not None:
            self.server.stop()

    def start_server(self, expected_code, output_func):
        sock, port = bind_unused_port()
        self.server = QDBServer(expected_code, output_func)
        self.server.add_socket(sock)
        return port

    @gen.coroutine
    def start(self, expected_code, output_func, proto_type, expected_value):
        client = None
        try:
            port = self.start_server(expected_code, output_func)
            endpoints = ['localhost:{}'.format(port)]
            client = TornadoClientPool(endpoints, retry_max=0)
            key = qdb_key('xxx')
            proto = create_protocal(proto_type, key)
            p = yield client.execute(proto)
            assert p.value == expected_value

        finally:
            if client is not None:
                yield client.close()

    @gen_test
    def test_get_qdb(self):
        data = b'hello world!'

        def hello_world():
            l = len(data)
            return struct.pack('>ii%ds' % l, 0, l, data)

        yield self.start(1, hello_world, 'get', data)

    @gen_test
    def test_check_not_in_qdb(self):
        def not_in_qdb():
            return struct.pack('>i', 1)

        yield self.start(12, not_in_qdb, 'test', False)
