from os_dbnetget.commands.qdb.test import Test as TestCommand
from os_dbnetget.commands.qdb.default_runner import DefaultRunner


class Test(TestCommand):
    ENGINE_NAME = 'default'

    def __init__(self, config=None):
        super(Test, self).__init__(config)
        self._runner = DefaultRunner(self.config)
