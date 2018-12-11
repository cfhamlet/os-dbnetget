import inspect
import sys
from argparse import ArgumentError, ArgumentParser
from importlib import import_module
from pkgutil import iter_modules

_PY3 = sys.version_info[0] == 3

if _PY3:
    import queue as Queue
    binary_stdin = sys.stdin.buffer
    binary_stdout = sys.stdout.buffer
else:
    import Queue
    if sys.platform == "win32":
        # set sys.stdin to binary mode
        import os
        import msvcrt
        msvcrt.setmode(sys.stdin.fileno(), os.O_BINARY)
    binary_stdin = sys.stdin
    binary_stdout = sys.stdout


class Config(object):
    pass


def walk_modules(module_path, skip_fail=True):

    mods = []
    mod = None
    try:
        mod = import_module(module_path)
        mods.append(mod)
    except Exception as e:
        if not skip_fail:
            raise e

    if mod and hasattr(mod, '__path__'):
        for _, subpath, ispkg in iter_modules(mod.__path__):
            fullpath = '.'.join((module_path, subpath))
            if ispkg:
                mods += walk_modules(fullpath, skip_fail)
            else:
                try:
                    submod = import_module(fullpath)
                    mods.append(submod)
                except Exception as e:
                    if not skip_fail:
                        raise e
    return mods


def iter_classes(module_path, base_class, include_base_class=False, skip_fail=True):
    for module in walk_modules(module_path, skip_fail=skip_fail):
        for obj in vars(module).values():
            if inspect.isclass(obj) and \
                    issubclass(obj, base_class) and \
                    obj.__module__ == module.__name__ and \
                    (include_base_class or
                     all([obj != base for base in base_class])
                     if isinstance(base_class, tuple)
                     else obj != base_class):
                yield obj


def split_endpoint(endpint):
    address, port = endpint.split(':')
    port = int(port)
    return address, port


class CustomArgumentParser(ArgumentParser):
    def parse_args(self, args=None, namespace=None):
        args, argv = self.parse_known_args(args, namespace)
        return args
