from datetime import timedelta
from functools import partial
from itertools import chain

from os_qdb_protocal import create_protocal
from tornado import gen, queues
from tornado.ioloop import IOLoop
from tornado.util import TimeoutError

from os_dbnetget.clients.tornado_client import TornadoClientPool
from os_dbnetget.commands.qdb import qdb_key
from os_dbnetget.commands.qdb.default_runner import DefaultRunner
from os_dbnetget.utils import check_range


class TornadoRunner(DefaultRunner):

    def __init__(self, config):
        super(TornadoRunner, self).__init__(config)

    def add_arguments(self, parser):
        super(TornadoRunner, self).add_arguments(parser)
        parser.add_argument('--concurrency',
                            help='concurrency (1-200 default: 10)',
                            type=partial(check_range, int, 1, 200),
                            default=10,
                            dest='concurrency',
                            )

    def process_arguments(self, args):
        self.config.inputs = args.inputs
        self.config.concurrency = args.concurrency
        self._client = TornadoClientPool(self.config.endpoints,
                                         timeout=args.client_timeout,
                                         retry_max=args.client_retry_max,
                                         retry_interval=args.client_retry_interval)
        self._queue = queues.Queue(maxsize=args.concurrency * 3)

    @gen.coroutine
    def _loop_read(self):
        for line in chain.from_iterable(self.config.inputs):
            if self._stop:
                break
            line = line.strip()
            yield self._queue.put(line)
        yield gen.multi([self._queue.put(None) for _ in range(0, self.config.concurrency)])

    @gen.coroutine
    def _process(self, data):
        try:
            q_key = qdb_key(data)
        except NotImplementedError:
            self.config.processor.process(data, None)
            return

        proto = create_protocal(self.config.cmd, q_key)
        p = yield self._client.execute(proto)
        self.config.processor.process(data, p)

    @gen.coroutine
    def _run(self, args):
        try:
            IOLoop.current().spawn_callback(self._loop_read)
            yield gen.multi([self._loop_process(args) for _ in range(0, self.config.concurrency)])
        except Exception as e:
            self._logger.error('Error {}'.format(e))
        finally:
            yield self._close()

    @gen.coroutine
    def _loop_process(self, args):
        while True:
            if self._stop and self._queue.qsize() <= 0:
                break
            try:
                data = yield self._queue.get(timeout=timedelta(seconds=0.1))
            except TimeoutError:
                continue
            try:
                if data is None:
                    break
                yield self._process(data)
            finally:
                self._queue.task_done()

    @gen.coroutine
    def _close(self):
        try:
            yield self._client.close()
        except:
            pass

    def _on_stop(self, signum, frame):
        self._stop = True

    def run(self, args):
        self._register_signal()
        IOLoop.current().run_sync(partial(self._run, args))
