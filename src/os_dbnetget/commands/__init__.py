from os_dbnetget.utils import Config


class Command(object):

    HELP = ''
    DESCRIPTION = ''
    ENGINE_NAME = None

    def __init__(self, config=None):
        if config is None:
            config = Config()
        self.config = config

    def add_arguments(self, parser):
        pass

    def process_arguments(self, args):
        pass

    def description(self):
        return self.DESCRIPTION

    def run(self, args):
        pass
