import inspect
from importlib import import_module
from pkgutil import iter_modules


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


def iter_classes(module_path, base_class, include_base_class=False):
    for module in walk_modules(module_path):
        for obj in vars(module).values():
            if inspect.isclass(obj) and \
                    issubclass(obj, base_class) and \
                    obj.__module__ == module.__name__ and \
                    (include_base_class or obj != base_class):
                yield obj


def split_endpoint(endpint):
    address, port = endpint.split(':')
    port = int(port)
    return address, port
