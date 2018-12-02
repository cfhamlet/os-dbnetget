
class Configurable(object):
    def __init__(self, config):
        self._config = config

    @property
    def config(self):
        return self._config


class Config(object):
    pass
