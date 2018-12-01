class RetryLimitExceeded(Exception):
    pass


class ServerClosed(Exception):
    pass


class ResourceLimit(Exception):
    pass


class Unavailable(Exception):
    pass