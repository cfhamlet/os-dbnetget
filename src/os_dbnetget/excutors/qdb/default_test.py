from os_dbnetget.commands.qdb.test import Test as TestCommand
from os_dbnetget.engines.default_engine import Default


class Test(TestCommand, Default):
    pass
