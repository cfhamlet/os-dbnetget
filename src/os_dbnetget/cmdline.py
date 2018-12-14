"""
Command line
"""
from __future__ import print_function

import inspect
import logging
import sys
import warnings
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from logging.config import dictConfig

import os_dbnetget
from os_dbnetget.commands import Command
from os_dbnetget.exceptions import UsageError
from os_dbnetget.utils import iter_classes, CustomArgumentParser

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


def _find_commands(cmds, cmds_path):
    sub_cmds = {}
    for cmd_cls in iter_classes(cmds_path,  Command):
        cmd_name = cmd_cls.__name__.lower()
        if cmd_name in cmds:
            warnings.warn('Find duplicated command %s, %s' %
                          (cmd_name, cmd_cls))
            continue
        if not hasattr(cmd_cls, 'ENGINE_NAME') or cmd_cls.ENGINE_NAME is None:
            continue
        engine_name = cmd_cls.ENGINE_NAME
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
    if not cmds:
        return
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
        if len(engines) == 1:
            desc_string = cmd.description()
            engine_kwargs['default'] = cmd.ENGINE_NAME
        elif 'default' in engines:
            desc_string = engines['default'].description()
            engine_kwargs['default'] = 'default'

        cmd_parser = sub_parser.add_parser(cmd_name,
                                           prog=parser.prog,
                                           description=desc_string,
                                           help=cmd.HELP,
                                           usage=_usage(cmd_name, '[OPTIONS]'),
                                           formatter_class=RawDescriptionHelpFormatter,
                                           add_help=False,
                                           )
        cmd_parser.add_argument('--engine', **engine_kwargs)


def _install_command(parser, cmds, cmd_name, engine):
    sub_parser = parser.add_subparsers(
        dest='command',
    )
    engines = cmds[cmd_name]
    cmd = engines[engine]
    engine_kwargs = {
        'choices': engines.keys(),
    }
    desc_string = cmd.DESCRIPTION

    if engine in engines:
        engine_kwargs['default'] = engine
        desc_string = engines[engine].description()

    cmd_parser = sub_parser.add_parser(cmd_name,
                                       prog=parser.prog,
                                       description=desc_string,
                                       help=cmd.HELP,
                                       usage=_usage(cmd_name, '[OPTIONS]'),
                                       formatter_class=RawDescriptionHelpFormatter,
                                       )

    cmd_parser.add_argument('--engine', **engine_kwargs)
    if engine:
        cmd.add_arguments(cmd_parser)


def _add_global_argument(parser):
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {version}'.format(version=os_dbnetget.__version__))
    parser.add_argument('-l', '--log-level',
                        dest='log_level',
                        help='log level (default: INFO)',
                        choices=_LOG_LEVELS,
                        default='INFO',
                        action='store',
                        type=lambda s: s.upper())


def _usage(o, c):
    return '\r{}\n\nusage: %(prog)s {} {}'.format(
        'os-dbnetget {}'.format(os_dbnetget.__version__).ljust(len('usage:')),
        o, c)


def _create_parser(parser_cls, **kwargs):
    return parser_cls(
        description='Command line tool not just for qdb',
        usage=_usage('[OPTIONS]', 'SUBCOMMAND'),
        formatter_class=RawDescriptionHelpFormatter, **kwargs)


_COMMANDS = [
    {'title': 'qdb-commands', 'help': None,
        'cmds_path': 'os_dbnetget.commands.qdb'},
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
    _config_logging(pre_args.log_level)
    _install_command(run_parser, cmds, pre_args.command, pre_args.engine)
    run_args = run_parser.parse_args(args=argv)
    cmd = cmds[run_args.command][run_args.engine]
    try:
        cmd.process_arguments(run_args)
        cmd.run(run_args)
    except UsageError as e:
        print('Error: %s' % str(e))
        sys.exit(2)


if __name__ == '__main__':
    execute()
