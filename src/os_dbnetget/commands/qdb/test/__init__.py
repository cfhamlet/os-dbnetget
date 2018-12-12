from os_dbnetget.utils import binary_stdout
from os_dbnetget.commands.qdb import QDB
from os_dbnetget.commands.qdb.test.processor import Processor


class Test(QDB):
    HELP = 'check if data exist in qdb'

    def __init__(self, config=None):
        super(Test, self).__init__(config)
        self.config.cmd = 'test'
        self.config.processor = Processor(self.config)

    def description(self):
        return 'Check if data exist in qdb\n    engine: [%s]' % self.ENGINE_NAME

    def process_arguments(self, args):
        super(Test, self).process_arguments(args)
        output = None
        if args.output is None:
            output = binary_stdout
        else:
            output = open(args.output, 'wb')
        self.config.output = output

    def add_arguments(self, parser):
        super(Test, self).add_arguments(parser)
        parser.add_argument('-o', '--output',
                            help='output file (default: stdout)',
                            nargs='?',
                            dest='output',
                            )

    def run(self, args):
        try:
            super(Test, self).run(args)
        finally:
            try:
                self.config.output.close()
            except:
                pass
