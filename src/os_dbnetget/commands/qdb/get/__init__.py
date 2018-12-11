from os_dbnetget.utils import binary_stdout
from os_dbnetget.commands.qdb import QDB
from os_dbnetget.commands.qdb.get.processor import Processor


class Get(QDB):
    HELP = 'get data from qdb'
    DESCRIPTION = 'Get data from qdb'

    def __init__(self, config=None):
        super(Get, self).__init__(config)
        self.config.cmd = 'get'
        self.config.processor = Processor(self.config)

    def description(self):
        return 'Get data from qdb\n    engine: [%s]' % self.ENGINE_NAME

    def process_arguments(self, args):
        super(Get, self).process_arguments(args)
        output = None
        if args.output is None:
            output = binary_stdout
        else:
            if not hasattr(args, 'output_type'):
                output = open(args.output, 'ab')
            elif args.output_type == 'single':
                output = open(args.output, 'ab')
            elif args.output_type == 'rotate':
                from os_rotatefile import open_file
                output = open_file(args.output, 'w')
        self.config.output = output

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

    def run(self, args):
        try:
            super(Get, self).run(args)
        finally:
            try:
                self.config.output.close()
            except:
                pass
