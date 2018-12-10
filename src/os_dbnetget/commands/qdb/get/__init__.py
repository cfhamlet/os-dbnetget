import argparse
from os_dbnetget.utils import binary_stdout
from os_dbnetget.commands.qdb import QDB


class Get(QDB):
    HELP = 'get data from qdb'
    DESCRIPTION = 'Get data from qdb'

    def description(self):
        return 'Get data from qdb\n    engine: [%s]' % self.ENGINE_NAME

    def add_arguments(self, parser):
        super(Get, self).add_arguments(parser)
        parser.add_argument('-o', '--output',
                            help='output file (default: stdout)',
                            nargs='?',
                            dest='output',
                            )
        try:
            import os_rotatefile
            parser.add_argument('-t', '--output-type',
                                help='output file type (default: single)',
                                choices=('single', 'rotate'),
                                default='single',
                                dest='output_type',
                                )
        except:
            pass
