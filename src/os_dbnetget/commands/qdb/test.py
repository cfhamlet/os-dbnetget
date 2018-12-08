from os_dbnetget.commands.qdb import QDB


class Test(QDB):
    HELP = 'check if data exist in qdb'
    CMD = 'test'
