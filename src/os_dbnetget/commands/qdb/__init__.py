
import argparse

from os_dbnetget.commands.command import Command
from os_dbnetget.utils import binary_stdin


class QDB(Command):
    CMD = None

    def add_argument(self, parser):
        parser.add_argument('-i', '--inputs',
                            help='input files to be processed (default: stdin)',
                            nargs='+',
                            types=argparse.FileType('rb'),
                            default=[binary_stdin],
                            dest='inputs',
                            )
