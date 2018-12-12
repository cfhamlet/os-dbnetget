from os_dbnetget.commands.qdb.get import Get as GetCommand
from os_dbnetget.commands.qdb.m3_runner import M3Runner


class Get(GetCommand):
    ENGINE_NAME = 'm3'

    def __init__(self, config=None):
        super(Get, self).__init__(config)
        self._runner = M3Runner(self.config)
