import logging
import signal
import sys
import threading
import time
from itertools import chain

from ..config import Configurable
from .engine import Engine

_PY3 = sys.version_info[0] == 3

if _PY3:
    import queue as Queue
else:
    import Queue


class Processor(Configurable):

    def process(self, data):
        raise NotImplementedError


class Driver(Configurable):

    def __init__(self, config, current_thread):
        super(Driver, self).__init__(config)
        self._current_thead = current_thread

    @property
    def current_thread(self):
        return self._current_thead

    @property
    def runtime_context(self):
        return self.config.runtime_context

    def run(self):
        raise NotImplementedError


class FileinputFrontendDriver(Driver):

    @property
    def queue(self):
        return self.runtime_context.frontend_thread_queue

    def run(self):
        for data in chain.from_iterable(self.runtime_context.files):
            data = data.strip()
            while True:
                if not data:
                    if self.current_thread.stopping():
                        return
                    else:
                        break
                try:
                    self.queue.put(data, block=False, timeout=1)
                    break
                except Queue.Full:
                    continue
            if self.current_thread.stopping():
                break


class TransportDriver(Driver):
    def __init__(self, config, current_thread):
        super(TransportDriver, self).__init__(config, current_thread)
        self._processor = self.config.engine.transport.processor_cls(config)
        self._backend_exists = False
        if hasattr(self.runtime_context, 'backend_thread'):
            self._backend_exists = True

    @property
    def input_queue(self):
        return self.runtime_context.frontend_thread_queue

    def _process(self, data):
        self._processor.process(data)

    def run(self):
        while True:
            if self.current_thread.stopping():
                if self.input_queue.qsize() <= 0:
                    break
                elif self._backend_exists:
                    if self.runtime_context.backend_thread.stopped():
                        break

            try:
                data = self.input_queue.get(block=True, timeout=0.1)
                self._process(data)
            except Queue.Empty:
                pass


class BridgeDriver(TransportDriver):

    @property
    def output_queue(self):
        return self.runtime_context.backend_thread_queue

    def _process(self, data):
        p_data = self._processor.process(data)
        while True:
            try:
                self.output_queue.put(p_data, block=True, timeout=0.1)
                break
            except Queue.Full:
                pass


class BackendDriver(Driver):
    def __init__(self, config, current_thread):
        super(BackendDriver, self).__init__(config, current_thread)
        self._transport_exists = False
        if hasattr(self.runtime_context, 'transport_thread'):
            self._transport_exists = True
            self._queue = self.runtime_context.backend_thread_queue
        else:
            self._queue = self.runtime_context.frontend_thread_queue

        self._processor = config.engine.backend.processor_cls(config)

    @property
    def queue(self):
        return self._queue

    def run(self):
        while True:
            if self.current_thread.stopping():
                if self.queue.qsize() <= 0:
                    if not self._transport_exists:
                        break
                    elif self.runtime_context.transport_thread.stopped():
                        break

            try:
                data = self.queue.get(block=True, timeout=0.1)
                self._processor.process(data)
            except Queue.Empty:
                pass


class MultithreadManager(Configurable):
    def __init__(self, config, driver_cls, thread_cls, num):
        super(MultithreadManager, self).__init__(config)
        started = threading.Event()
        self._started = started
        self._threads = [thread_cls(config, started, driver_cls, name='%s.%d' % (
            thread_cls.__name__, i)) for i in range(0, num)]

    def start(self):
        while not self._started.wait(0.1):
            if self._started.is_set():
                break
            list(map(lambda t: t.start(), self._threads))

    def setDaemon(self, daemonic):
        list(map(lambda t: t.setDaemon(daemonic), self._threads))

    def join(self):
        list(map(lambda t: t.join(), self._threads))

    def stop(self):
        list(map(lambda t: t.stop(), self._threads))

    def stopped(self):
        return not any([t.isAlive() for t in self._threads])


def getatters(obj, attrs):
    return [getattr(obj, attr) for attr in attrs if hasattr(obj, attr)]


class OThread(Configurable, threading.Thread):

    def __init__(self, config, started, driver_cls, **kwargs):
        Configurable.__init__(self, config)
        threading.Thread.__init__(self, **kwargs)

        self._stopping = False
        self._started = started
        self._logger = logging.getLogger(self.name)
        self._driver_cls = driver_cls

    @property
    def runtime_context(self):
        return self.config.runtime_context

    def run(self):
        driver = None
        try:
            self._started.set()
            self._logger.debug('Start')
            driver = self._driver_cls(self.config, self)
            driver.run()
        except Exception as e:
            self._logger.error('Unexpected exception, %s' % str(e))
        self._logger.debug('Stop')

    def stopping(self):
        return self._stopping

    def stop(self):
        self._stopping = True


class FrontendThread(OThread):

    def run(self):
        super(FrontendThread, self).run()
        if hasattr(self.runtime_context, 'transport_thread'):
            self.runtime_context.transport_thread.stop()
        elif hasattr(self.runtime_context, 'backend_thread'):
            self.runtime_context.backend_thread.stop()


class TransportThread(OThread):

    def run(self):
        super(TransportThread, self).run()
        self.runtime_context.frontend_thread.stop()
        if hasattr(self.runtime_context, 'backend_thread'):
            self.runtime_context.backend_thread.stop()


class BackendThread(OThread):

    def run(self):
        super(BackendThread, self).run()
        self.runtime_context.frontend_thread.stop()
        if hasattr(self.runtime_context, 'transport_thread'):
            self.runtime_context.transport_thread.stop()


class MultithreadEngine(Engine):

    def __init__(self, config):
        super(MultithreadEngine, self).__init__(config)
        self.__ensure()

    def __ensure(self):
        engine_config = self.config.engine
        assert any([hasattr(engine_config, x)
                    for x in ('transport', 'backend')])

    def setup(self):
        runtime_context = self.config.runtime_context
        engine_config = self.config.engine

        runtime_context.frontend_thread_queue = Queue.Queue(
            engine_config.frontend.queue_size)

        runtime_context.frontend_thread = MultithreadManager(
            self.config, engine_config.frontend.driver_cls,
            FrontendThread, engine_config.frontend.max_thread)
        runtime_context.frontend_thread.setDaemon(True)

        if hasattr(engine_config, 'transport'):

            if hasattr(engine_config, 'backend'):
                runtime_context.backend_thread_queue = Queue.Queue(
                    engine_config.backend.queue_size)

            runtime_context.transport_thread = MultithreadManager(
                self.config, engine_config.transport.driver_cls,
                TransportThread, engine_config.transport.max_thread)

        if hasattr(engine_config, 'backend'):
            runtime_context.backend_thread = MultithreadManager(
                self.config, engine_config.backend.driver_cls,
                BackendThread, engine_config.backend.max_thread)

    def start(self):

        runtime_context = self.config.runtime_context
        m = getatters(runtime_context, [
                      'backend_thread', 'transport_thread', 'frontend_thread'])

        list(map(lambda x: x.start(), m))

        while not any([x.stopped() for x in m]):
            time.sleep(0.1)

        m = getatters(runtime_context, ['transport_thread', 'backend_thread'])
        list(map(lambda x: x.join(), m))

    def stop(self, what=()):
        runtime_context = self.config.runtime_context
        runtime_context.frontend_thread.stop()
        if hasattr(runtime_context, 'transport_thread'):
            runtime_context.transport_thread.stop()
        elif hasattr(runtime_context, 'backend_thread'):
            runtime_context.backend_thread.stop()
