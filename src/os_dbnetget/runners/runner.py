class Runner(object):

    def __init__(self, config):
        self._config = config

    @property
    def config(self):
        return self._config

    def stop(self):
        pass

    def setup(self):
        pass

    def cleanup(self):
        pass

    def start(self):
        pass
