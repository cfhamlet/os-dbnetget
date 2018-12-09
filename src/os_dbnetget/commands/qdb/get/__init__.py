from os_dbnetget.commands.qdb import QDB


class Get(QDB):
    HELP = 'get data from qdb'
    DESCRIPTION = 'Get data from qdb'

    def description(self):
        return 'Get data from qdb\n    engine: [%s]' % self.ENGINE_NAME
