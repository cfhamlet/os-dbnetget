from ..commands.get import Get as GetCommand
from ..engines.simple_engine import SimpleEngine


class Get(GetCommand, SimpleEngine):
    pass
