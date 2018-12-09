from os_dbnetget.commands.qdb import QDB


class Test(QDB):
    HELP = 'check if data exist in qdb'

    def description(self):
        return 'Check if data exist in qdb\n    engine: [%s]' % self.ENGINE_NAME