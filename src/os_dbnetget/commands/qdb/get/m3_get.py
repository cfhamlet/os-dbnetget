import signal
import logging
from itertools import chain

from os_docid import docid
from os_m3_engine.core.backend import Backend
from os_m3_engine.core.frontend import Frontend
from os_m3_engine.core.transport import Transport
from os_m3_engine.core.engine import RuntimeContext
from os_m3_engine.launcher import create
from os_qdb_protocal import create_protocal

from os_dbnetget.commands.qdb.get.default_get import Get as GetCommand


class FileFrontend(Frontend):
    def produce(self):
        for line in chain.from_iterable(self.config.inputs):
            line = line.strip()
            yield line


class GetTransport(Transport):

    def transport(self, data):
        try:
            d = docid(data)
        except NotImplementedError:
            return (data, None)
        proto = create_protocal('get', d.bytes[16:])
        p = self._runtime_context.pool.execute(proto)
        return (data, p)


class StoreBackend(Backend):
    _logger = logging.getLogger('StoreBackend')

    def process(self, data):
        data, proto = data
        status = 'N'
        if proto is None:
            status = 'E'
        if status != 'E':
            if proto.value:
                status = 'Y'
                self._runtime_context.output.write(proto.value)
        self._logger.info('%s\t%s' % (data, status))


class Get(GetCommand):
    ENGINE_NAME = 'm3'

    def run(self, args):

        runtime_context = RuntimeContext()
        runtime_context.pool = self._create_client_pool(args)
        runtime_context.output = self._create_output(args)
        config = RuntimeContext()
        config.thread_num = 20

        engine = create(frontend_cls=FileFrontend,
                        transport_cls=GetTransport,
                        backend_cls=StoreBackend,
                        engine_transport_config=config,
                        app_config=args,
                        runtime_context=runtime_context)

        def on_stop(signum, frame):
            try:
                engine.stop()
            except:
                pass

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT):
            signal.signal(sig, on_stop)

        try:
            engine.start()
        finally:
            try:
                engine.stop()
            except:
                pass
