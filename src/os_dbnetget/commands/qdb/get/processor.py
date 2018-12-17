import logging
from os_dbnetget.commands.qdb.processor import Processor as BaseProcessor


class Processor(BaseProcessor):

    def process(self, data, proto):
        status = 'N'
        if proto is None:
            status = 'E'
        if status != 'E':
            if proto.value:
                status = 'Y'
                self.config.output.write(proto.value)
                self.config.output.write(b'\n')
        self._logger.info('%s\t%s' % (data, status))
