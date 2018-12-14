from ..cmd_runner import call


def test_command_help():
    data = [
        ('get --engine m3 -h', b'engine: [m3]'),
        ('get --engine m3 -h', b'--thread-num'),
        ('test --engine m3 -h', b'engine: [m3]')
    ]
    for c, expect in data:
        stdout, _ = call(c)
        assert expect in stdout


def test_test_command(fake_qdb_never_exist_server_port, tmpdir):
    data = [
        ('i_am_not_in_qdb', b'N\ti_am_not_in_qdb')
    ]
    count = 0
    for c, expect in data:
        count += 1
        f = tmpdir.join('testfile_{}'.format(count))
        f.write(c)
        ff = [
            'test -E localhost:{} -i {}',
            'test --engine m3 -E localhost:{} -i {}'
        ]
        for fs in ff:
            stdout, _ = call(
                fs.format(fake_qdb_never_exist_server_port, f.strpath))
        assert expect in stdout


def test_get_command(fake_qdb_helloworld_server_port, tmpdir):
    data = [
        ('i_am_a_key', b'hello world!')
    ]
    count = 0
    for c, expect in data:
        count += 1
        f = tmpdir.join('testfile_{}'.format(count))
        f.write(c)
        ff = [
            'get -E localhost:{} -i {}',
            'get --engine m3 -E localhost:{} -i {}'
        ]
        for fs in ff:
            stdout, _ = call(
                fs.format(fake_qdb_helloworld_server_port, f.strpath))
        assert expect in stdout
