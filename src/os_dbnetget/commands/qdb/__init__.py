import argparse
import signal

from os_dbnetget.commands import Command
from os_dbnetget.utils import binary_stdin
from os_dbnetget.exceptions import UsageError


class QDB(Command):

    def add_arguments(self, parser):
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
                            can be override by \'-E\' argument',
                            nargs='?',
                            type=argparse.FileType('rb'),
                            dest='endpoints_list',
                            )

    def process_arguments(self, args):
        if not (args.endpoints or args.endpoints_list):
            raise UsageError('No endpoints, add \'-E\' or \'-L\' argument')
