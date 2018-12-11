import logging
import signal
from itertools import chain

from os_docid import docid
from os_qdb_protocal import create_protocal

from os_dbnetget.clients.sync_client import SyncClientPool
from os_dbnetget.commands import Command


class DefaultRunner(Command):

    def __init__(self, config):
        self.config = config
        self._client = None
        self._logger = logging.getLogger(self.__class__.__name__)
        self._stop = False

    def add_arguments(self, parser):

        parser.add_argument('--client-retry-max',
                            help='client retry max (default: 1)',
                            type=int,
                            default=1,
                            dest='client_retry_max',
                            )
        parser.add_argument('--client-retry-interval',
                            help='client retry interval (default: 5)',
                            type=int,
                            default=5,
                            dest='client_retry_interval',
                            )
        parser.add_argument('--client-timeout',
                            help='client timeout(default: 10)',
                            type=int,
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

    def _process_result(self, data, proto):
        status = 'N'
        if proto is None:
            status = 'E'
        if status != 'E':
            if proto.value:
                status = 'Y'
                self.config.output.write(proto.value)
        self._logger.info('%s\t%s' % (data, status))

    def _run(self, args):
        for line in chain.from_iterable(args.inputs):
            if self._stop:
                break
            line = line.strip()
            try:
                d = docid(line)
            except NotImplementedError:
                self._process_result(line, None)
                continue
            proto = create_protocal(self.config.cmd, d.bytes[16:])
            p = self._client.execute(proto)
            self._process_result(line, p)

    def run(self, args):
        self._register_signal()
        try:
            self._run(args)
        except Exception as e:
            self._logger.error('Error %s' % str(e))
        finally:
            self._close()
