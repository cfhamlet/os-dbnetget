import logging
from functools import partial
from itertools import chain

from os_m3_engine.core.backend import Backend
from os_m3_engine.core.frontend import Frontend
from os_m3_engine.core.transport import Transport
from os_m3_engine.launcher import create
from os_qdb_protocal import create_protocal

from os_dbnetget.clients.sync_client import SyncClientPool
from os_dbnetget.commands.qdb import qdb_key
from os_dbnetget.commands.qdb.default_runner import DefaultRunner
from os_dbnetget.utils import Config, check_range


class InputsFrontend(Frontend):
    def produce(self):
        for line in chain.from_iterable(self.config.inputs):
            line = line.strip()
            yield line


class QDBTransport(Transport):

    def transport(self, data):
        try:
            q_key = qdb_key(data)
        except NotImplementedError:
            return (data, None)
        proto = create_protocal(self.config.cmd, q_key)

        p = self.config.client.execute(proto)
        return (data, p)


class StoreBackend(Backend):

    def process(self, data):
        data, proto = data
        self.config.processor.process(data, proto)


class M3Runner(DefaultRunner):

    def __init__(self, config):
        super(M3Runner, self).__init__(config)
        self._engine = None

    def _close(self):
        try:
            self._client.close()
        except:
            pass

    def _on_stop(self, signum, frame):
        try:
            self._engine.stop()
        except:
            pass

    def add_arguments(self, parser):
        super(M3Runner, self).add_arguments(parser)
        parser.add_argument('--thread-num',
                            help='thread num (1-100 default: 10)',
                            type=partial(check_range, int, 1, 100),
                            default=10,
                            dest='thread_num',
                            )

    def process_arguments(self, args):
        super(M3Runner, self).process_arguments(args)
        self.config.inputs = args.inputs
        self.config.client = self._client
        engine_transport_config = Config()
        engine_transport_config.thread_num = args.thread_num
        self._engine = create(frontend_cls=InputsFrontend,
                              transport_cls=QDBTransport,
                              backend_cls=StoreBackend,
                              engine_transport_config=engine_transport_config,
                              app_config=self.config)

    def _run(self, args):
        self._engine.start()
