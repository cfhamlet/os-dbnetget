import logging


class Processor(object):
    def __init__(self, config):
        self.config = config
        self._logger = logging.getLogger(self.__class__.__name__)

    def process(self, data, proto):
        pass
