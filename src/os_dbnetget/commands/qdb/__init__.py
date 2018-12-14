import argparse

from os_dbnetget.commands import Command
from os_dbnetget.utils import binary_stdin
from os_dbnetget.exceptions import UsageError
from os_docid import docid


def qdb_key(url_or_docid):
    return docid(url_or_docid).bytes[16:]


class QDB(Command):

    def __init__(self, config=None):
        super(QDB, self).__init__(config)
        self._runner = None

    def add_arguments(self, parser):
        super(QDB, self).add_arguments(parser)
        parser.add_argument('-i', '--inputs',
                            help='input files to be processed (default: stdin)',
                            nargs='+',
                            type=argparse.FileType('rb'),
                            default=[binary_stdin],
                            dest='inputs',
                            )

        parser.add_argument('-E', '--endpoints',
                            help='comma-separated qdb endpoints(host:ip)',
                            nargs='?',
                            dest='endpoints',
                            )

        parser.add_argument('-L', '--endpoints-list-file',
                            help='qdb endpoints(host:ip) list file, one endpoint per line, \
                            can be overridden by \'-E\' argument',
                            nargs='?',
                            type=argparse.FileType('rb'),
                            dest='endpoints_list',
                            )
        self._runner.add_arguments(parser)

    def process_arguments(self, args):
        super(QDB, self).process_arguments(args)
        if not (args.endpoints or args.endpoints_list):
            raise UsageError('No endpoints, add \'-E\' or \'-L\' argument')
        endpoints = None
        if args.endpoints:
            endpoints = tuple([e.strip()
                               for e in args.endpoints.split(',') if e.strip()])
        else:
            endpoints = tuple([e.strip()
                               for e in args.endpoints_list if e.strip()])
        if not endpoints:
            raise UsageError('No endpoints, check your arguments')

        self.config.endpoints = endpoints
        self._runner.process_arguments(args)

    def run(self, args):
        self._runner.run(args)
