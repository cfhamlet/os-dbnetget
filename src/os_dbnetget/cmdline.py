"""
Command line
"""
from __future__ import print_function

import inspect
import logging
import sys
import warnings
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from commands.command import Command
from logging.config import dictConfig

import os_dbnetget
from engines.engine import Engine
from os_dbnetget.utils import iter_classes
from utils import CustomArgumentParser

_LOG_LEVELS = ['NOTSET', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']

_DEFAULT_LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'incremental': True,
}


def _config_logging(log_level):
    dictConfig(_DEFAULT_LOGGING)
    if log_level == 'NOTSET':
        handler = logging.NullHandler()
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        fmt='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logging.root.setLevel(logging.NOTSET)
    handler.setFormatter(formatter)
    handler.setLevel(log_level)
    logging.root.addHandler(handler)


def _get_engine_cls(cmd_cls):
    for c in inspect.getmro(cmd_cls):
        if cmd_cls != c and issubclass(c, Engine) and c != Engine:
            return c


def _find_commands(cmds, cmds_path):
    sub_cmds = {}
    for cmd_cls in iter_classes(cmds_path, (Engine, Command)):
        cmd_name = cmd_cls.__name__.lower()
        if cmd_name in cmds:
            warnings.warn('Find duplicated command %s, %s' %
                          (cmd_name, cmd_cls))
            continue
        engine_name = cmd_cls.engine_name if hasattr(
            cmd_cls, 'engine_name') else None
        if engine_name is None:
            engine_cls = _get_engine_cls(cmd_cls)
            engine_name = engine_cls.engine_name if hasattr(
                engine_cls, 'engine_name') else engine_cls.__name__.lower()
        if cmd_name not in sub_cmds:
            sub_cmds[cmd_name] = {}
        engines = sub_cmds[cmd_name]
        if engine_name in engines:
            warnings.warn('Find duplicated engine %s, %s' %
                          (engine_name, cmd_cls))
        else:
            engines[engine_name] = cmd_cls()

    cmds.update(sub_cmds)
    return sub_cmds


def _install_commands(parser, cmds, config):
    sub_parser = parser.add_subparsers(
        title=config['title'],
        help=config['help'],
        dest='command',
    )
    for cmd_name in cmds:
        engines = cmds[cmd_name]
        cmd = list(engines.values())[0]
        engine_kwargs = {
            'choices': engines.keys(),
        }
        desc_string = cmd.DESCRIPTION
        if 'default' in engines:
            desc_string = engines['default'].description()
            engine_kwargs['default'] = 'default'

        cmd_parser = sub_parser.add_parser(cmd_name,
                                           prog=parser.prog,
                                           description=desc_string,
                                           help=cmd.HELP,
                                           usage=_usage(cmd_name, '[OPTIONS]'),
                                           formatter_class=RawDescriptionHelpFormatter)
        cmd_parser.add_argument('--engine', **engine_kwargs)


def _add_global_argument(parser):
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {version}'.format(version=os_dbnetget.__version__))
    parser.add_argument('-l', '--log-level',
                        dest='log_level',
                        choices=_LOG_LEVELS,
                        default='NOTSET',
                        action='store',
                        type=lambda s: s.upper())


def _usage(o, c):
    return '\r{}\n\nusage: %(prog)s {} {}'.format(
        'os-dbnetget {}'.format(os_dbnetget.__version__).ljust(len('usage:')),
        o, c)


def _create_parser(parser_cls):
    return parser_cls(
        description='Command line tool not just for qdb',
        usage=_usage('[OPTIONS]', 'SUBCOMMAND'),
        formatter_class=RawDescriptionHelpFormatter)


def _run(cmds, args):
    print(args)
    pass


_COMMANDS = [
    {'title': 'qdb-commands', 'help': None,
        'cmds_path': 'os_dbnetget.excutors.qdb'},
]


def execute(argv=None):
    argv = argv or sys.argv[1:]
    pre_parser = _create_parser(CustomArgumentParser)
    run_parser = _create_parser(ArgumentParser)
    _add_global_argument(pre_parser)
    _add_global_argument(run_parser)

    cmds = {}
    for c in _COMMANDS:
        sub_cmds = _find_commands(cmds, c['cmds_path'])
        _install_commands(pre_parser, sub_cmds, c)

    if not argv:
        pre_parser.print_help()
        sys.exit(0)
    pre_args = pre_parser.parse_args(args=argv)
#    _install_commands(run_parser, cmds, pre_args.engine)
#    run_args = run_parser.parse_args(args=argv)
#    _run(cmds, run_args)


if __name__ == '__main__':
    execute()
