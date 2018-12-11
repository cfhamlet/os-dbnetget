import logging
from io import BytesIO

from os_m3_engine.core.backend import Backend

from os_dbnetget.commands.qdb.m3_runner import M3Runner
from os_dbnetget.commands.qdb.test import Test as TestCommand


class StoreBackend(Backend):
    _logger = logging.getLogger('StoreBackend')

    def process(self, data):
        data, proto = data
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
    ENGINE_NAME = 'm3'

    def __init__(self, config=None):
        super(Test, self).__init__(config)
        self.config.cmd = 'test'
        self.config.backend_cls = StoreBackend
        self._runner = M3Runner(self.config)
