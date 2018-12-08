from os_dbnetget.commands.qdb.get import Get as GetCommand
from os_dbnetget.engines.m3_engine import M3


class Get(GetCommand, M3):
    pass
