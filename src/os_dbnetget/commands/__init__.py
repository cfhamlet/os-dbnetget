

class Command(object):

    HELP = ''
    DESCRIPTION = ''
    ENGINE_NAME = None

    def add_arguments(self, parser):
        pass

    def process_arguments(self, args):
        pass

    def description(self):
        return self.DESCRIPTION

    def run(self, args):
        pass
