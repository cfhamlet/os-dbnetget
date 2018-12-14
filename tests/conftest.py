import pytest
from xprocess import ProcessStarter

from .utils import unused_port


@pytest.fixture
def fake_qdb_never_exist_server_port(xprocess):
    port = unused_port()

    class Starter(ProcessStarter):
        pattern = ''
        args = [
            'python',
            '../../tests/fake_server/qdb_server.py',
            '--output-func', 'output_not_exist',
            '--port', '{}'.format(port),
            '--expected-code', '12'
        ]
    s = 'fake_qdb_nerver_exist_server'
    xprocess.ensure(s, Starter)
    yield port
    xprocess.getinfo(s).terminate()


@pytest.fixture
def fake_qdb_helloworld_server_port(xprocess):
    port = unused_port()

    class Starter(ProcessStarter):
        pattern = ''
        args = [
            'python',
            '../../tests/fake_server/qdb_server.py',
            '--output-func', 'output_helloworld',
            '--port', '{}'.format(port),
            '--expected-code', '1'
        ]
    s = 'fake_qdb_helloworld_server'
    xprocess.ensure(s, Starter)
    yield port
    xprocess.getinfo(s).terminate()
