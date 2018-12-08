from os_dbnetget.commands.qdb.get import Get as GetCommand
from os_dbnetget.engines.tornado_engine import Tornado


class Get(GetCommand, Tornado):
    def description(self):
        return 'Get data from qdb\n    engine: [tornado]'
