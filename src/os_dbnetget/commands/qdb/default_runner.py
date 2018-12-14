import logging
import signal
from functools import partial
from itertools import chain

from os_qdb_protocal import create_protocal

from os_dbnetget.clients.sync_client import SyncClientPool
from os_dbnetget.commands import Command
from os_dbnetget.commands.qdb import qdb_key
from os_dbnetget.utils import check_range


class DefaultRunner(Command):

    def __init__(self, config):
        self.config = config
        self._client = None
        self._logger = logging.getLogger(self.__class__.__name__)
        self._stop = False

    def add_arguments(self, parser):

        parser.add_argument('--client-retry-max',
                            help='client retry max (0-10, default: 0)',
                            type=partial(check_range, int, 0, 10),
                            default=0,
                            dest='client_retry_max',
                            )
        parser.add_argument('--client-retry-interval',
                            help='client retry interval seconds (0-60 default: 5)',
                            type=partial(check_range, float, 0, 60),
                            default=5,
                            dest='client_retry_interval',
                            )
        parser.add_argument('--client-timeout',
                            help='client timeout seconds (1-60 default: 10)',
                            type=partial(check_range, float, 1, 60),
                            default=10,
                            dest='client_timeout',
                            )

    def process_arguments(self, args):
        self._client = SyncClientPool(self.config.endpoints,
                                      timeout=args.client_timeout,
                                      retry_max=args.client_retry_max,
                                      retry_interval=args.client_retry_interval)

    def _close(self):
        self._stop = True
        try:
            if self._client:
                self._client.close()
        except:
            pass

    def _on_stop(self, signum, frame):
        self._close()

    def _register_signal(self):
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT):
            signal.signal(sig, self._on_stop)

    def _run(self, args):
        for line in chain.from_iterable(args.inputs):
            if self._stop:
                break
            data = line.strip()
            try:
                q_key = qdb_key(data)
            except NotImplementedError:
                self.config.processor.process(data, None)
                continue
            proto = create_protocal(self.config.cmd, q_key)
            p = self._client.execute(proto)
            self.config.processor.process(data, p)

    def run(self, args):
        self._register_signal()
        try:
            self._run(args)
        except Exception as e:
            self._logger.error('Error {}'.format(e))
        finally:
            self._close()
