from os_dbnetget.commands.qdb.default_runner import DefaultRunner
from os_dbnetget.commands.qdb.get import Get as GetCommand


class Get(GetCommand):
    ENGINE_NAME = 'default'

    def __init__(self, config=None):
        super(Get, self).__init__(config)
        self._runner = DefaultRunner(self.config)
