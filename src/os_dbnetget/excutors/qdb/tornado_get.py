from os_dbnetget.commands.qdb.get import Get as GetCommand
from os_dbnetget.engines.tornado_engine import Tornado


class Get(GetCommand, Tornado):
    pass
