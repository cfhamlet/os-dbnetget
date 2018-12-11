from os_dbnetget.commands.qdb.test import Test as TestCommand
from os_dbnetget.commands.qdb.tornado_runner import TornadoRunner


class Test(TestCommand):
    ENGINE_NAME = 'tornado'

    def __init__(self, config=None):
        super(Test, self).__init__(config)
        self._runner = TornadoRunner(self.config)
