import socket
import struct

from os_qdb_protocal import create_protocal
from tornado.stack_context import NullContext
from tornado.testing import AsyncTestCase, bind_unused_port, gen_test

import pytest
from os_dbnetget.clients.tornado_client import TornadoClient, TornadoClientPool
from os_dbnetget.commands.qdb import qdb_key
from os_dbnetget.exceptions import ResourceLimit, RetryLimitExceeded

from ..fake_server.qdb_server import QDBServer


class TestTornadoClient(AsyncTestCase):
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

    @gen_test
    def test_check_not_in_qdb(self):
        def not_in_qdb():
            return struct.pack('>i', 1)
        server = None
        client = None
        try:
            sock, port = bind_unused_port()
            with NullContext():
                server = QDBServer(12, not_in_qdb)
                server.add_socket(sock)
            endpoints = ['localhost:{}'.format(port)]
            client = TornadoClientPool(endpoints, retry_max=0)
            key = qdb_key('xxx')
            proto = create_protocal('test', key)
            p = yield client.execute(proto)
            assert p.value == 0

        finally:
            if server is not None:
                server.stop()
            if client is not None:
                yield client.close()

    @gen_test
    def test_get_qdb(self):
        data = b'hello world!'

        def hello_world():
            l = len(data)
            return struct.pack('>ii%ds' % l, 0, l, data)
        server = None
        client = None
        try:
            sock, port = bind_unused_port()
            with NullContext():
                server = QDBServer(1, hello_world)
                server.add_socket(sock)
            endpoints = ['localhost:{}'.format(port)]
            client = TornadoClientPool(endpoints, retry_max=0)
            key = qdb_key('xxx')
            proto = create_protocal('get', key)
            p = yield client.execute(proto)
            assert p.value == data

        finally:
            if server is not None:
                server.stop()
            if client is not None:
                yield client.close()
