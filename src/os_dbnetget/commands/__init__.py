class Command(object):

    HELP = ''
    DESCRIPTION = ''

    def add_argument(self, parser):
        pass

    def description(self):
        return self.DESCRIPTION


    def run(self, args):
        pass