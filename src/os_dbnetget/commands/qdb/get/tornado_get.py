from os_dbnetget.commands.qdb.get import Get as GetCommand
from os_dbnetget.commands.qdb.tornado_runner import TornadoRunner


class Get(GetCommand):
    ENGINE_NAME = 'tornado'

    def __init__(self, config=None):
        super(Get, self).__init__(config)
        self._runner = TornadoRunner(self.config)
