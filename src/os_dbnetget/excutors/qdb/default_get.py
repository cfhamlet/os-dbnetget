from os_dbnetget.commands.qdb.get import Get as GetCommand
from os_dbnetget.engines.default_engine import Default


class Get(GetCommand, Default):
    def description(self):
        return 'Get data from qdb\n    engine: [default]'
