from io import BytesIO

from os_dbnetget.commands.qdb.test import Test as TestCommand
from os_dbnetget.commands.qdb.tornado_runner import TornadoRunner


class Runner(TornadoRunner):
    def _process_result(self, data, proto):
        status = 'N'
        if proto is None:
            status = 'E'
        if status != 'E':
            if isinstance(proto.value, bool):
                if proto.value:
                    status = 'Y'
                else:
                    status = 'N'
            elif proto.value is None:
                status = 'U'
            else:
                status = str(proto.value)
        b = BytesIO()
        b.write(status.encode())
        b.write(b'\t')
        b.write(data)
        b.write(b'\n')
        b.seek(0)
        self.config.output.write(b.read())
        self._logger.info('%s\t%s' % (data, status))


class Test(TestCommand):
    ENGINE_NAME = 'tornado'

    def __init__(self, config=None):
        super(Test, self).__init__(config)
        self.config.cmd = 'test'
        self._runner = Runner(self.config)
