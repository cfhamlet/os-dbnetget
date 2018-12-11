import logging
from os_dbnetget.commands.qdb.get import Get as GetCommand
from os_dbnetget.commands.qdb.m3_runner import M3Runner

from os_m3_engine.core.backend import Backend


class StoreBackend(Backend):
    _logger = logging.getLogger('StoreBackend')

    def process(self, data):
        data, proto = data
        status = 'N'
        if proto is None:
            status = 'E'
        if status != 'E':
            if proto.value:
                status = 'Y'
                self.config.output.write(proto.value)
        self._logger.info('%s\t%s' % (data, status))


class Get(GetCommand):
    ENGINE_NAME = 'm3'

    def __init__(self, config=None):
        super(Get, self).__init__(config)
        self.config.cmd = 'get'
        self.config.backend_cls = StoreBackend
        self._runner = M3Runner(self.config)
