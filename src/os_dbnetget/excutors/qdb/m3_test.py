from os_dbnetget.commands.qdb.test import Test as TestCommand
from os_dbnetget.engines.m3_engine import M3


class Test(TestCommand, M3):
    pass
