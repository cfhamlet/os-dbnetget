from io import BytesIO

from os_dbnetget.commands.qdb.processor import Processor as BaseProcessor


class Processor(BaseProcessor):

    def process(self, data, proto):
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
