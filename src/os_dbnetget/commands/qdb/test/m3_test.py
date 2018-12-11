from os_dbnetget.commands.qdb.m3_runner import M3Runner
from os_dbnetget.commands.qdb.test import Test as TestCommand


class Test(TestCommand):
    ENGINE_NAME = 'm3'

    def __init__(self, config=None):
        super(Test, self).__init__(config)
        self._runner = M3Runner(self.config)
