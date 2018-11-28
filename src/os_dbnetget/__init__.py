import pkgutil
import inspect
import sys
from .protocal import Protocal
from importlib import import_module

_PROTOCALS = {}


def init(protocals):

    module = import_module('os_qdb_protocal.protocal')
    for obj in vars(module).values():
        if inspect.isclass(obj) and \
                issubclass(obj, Protocal) and \
                obj.__module__ == module.__name__ and \
                obj != Protocal:
            protocals[obj.__name__.lower()] = obj


init(_PROTOCALS)


def create_protocal(cmd_name, key):
    return _PROTOCALS[cmd_name](key)


__all__ = ['__version__', 'version_info', 'create_protocal']

__version__ = pkgutil.get_data(__package__, 'VERSION').decode('ascii').strip()
version_info = tuple(int(v) if v.isdigit() else v
                     for v in __version__.split('.'))
del pkgutil
del Protocal
del import_module
del inspect
del init
del sys
