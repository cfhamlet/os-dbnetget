import pytest
from xprocess import ProcessStarter

from ..cmd_runner import call
from ..utils import unused_port


def test_cmd_help():
    expected = [b'qdb-commands:', b'show this help message and exit']
    cmdlines = ['', '-h']
    for c in cmdlines:
        stdout, _ = call(c)
        for e in expected:
            assert e in stdout


def test_command_help():
    data = [
        ('get -h', b'Get data from qdb'),
        ('test -h', b'Check if data exist in qdb')
    ]
    for c, expect in data:
        stdout, _ = call(c)
        assert expect in stdout


def test_not_exist_cmd():
    _, stderr = call('not_exist_cmd')
    assert b'invalid choice' in stderr
