import sys
import logging
import signal
import threading
import uuid
import time
from itertools import chain
from os_qdb_protocal import create_protocal

from ..clients.sync_client import SyncClientPool
from .runner import Runner

_PY3 = sys.version_info[0] == 3

if _PY3:
    import queue as Queue
else:
    import Queue


class MultithreadManager(object):
    def __init__(self, threads):
        self._threads = threads

    def start(self):
        list(map(lambda t: t.start(), self._threads))

    def setDaemon(self, daemonic):
        list(map(lambda t: t.setDaemon(daemonic), self._threads))

    def join(self):
        list(map(lambda t: t.join(), self._threads))

    def stop(self):
        list(map(lambda t: t.stop(), self._threads))

    def stopped(self):
        return not any([t.isAlive() for t in self._threads])


class OThread(threading.Thread):

    def __init__(self, config, **kwargs):
        if 'tid' in kwargs:
            self._tid = str(kwargs.pop('tid'))
        else:
            self._tid = str(uuid.uuid1())[:8]

        super(OThread, self).__init__(**kwargs)
        self._config = config
        self._stopping = False
        self._logger = logging.getLogger(
            self.__class__.__name__ + '.%s' % self._tid)

    @property
    def runtime_context(self):
        return self.config.runtime_context

    @property
    def config(self):
        return self._config

    def run(self):
        try:
            self._logger.debug('Start')
            self._run()
            self._logger.debug('Stop')
        except Exception as e:
            self._logger.error('Unexpected exception, %s' % str(e))

    def stop(self):
        self._stopping = True


class Reader(OThread):

    @property
    def queue(self):
        return self.runtime_context.input_queue

    def _run(self):
        for data in chain.from_iterable(self.runtime_context.files):
            data = data.strip()
            while True:
                if not data:
                    if self._stopping:
                        return
                    else:
                        break
                try:
                    self.queue.put(data, block=False, timeout=1)
                    break
                except Queue.Full:
                    continue
            if self._stopping:
                break

    def run(self):
        super(Reader, self).run()
        [self.queue.put(None)
         for _ in range(0, self.config.max_operator)]
        self.runtime_context.operator.stop()


class Operator(OThread):

    @property
    def input_queue(self):
        return self.runtime_context.input_queue

    @property
    def output_queue(self):
        return self.runtime_context.output_queue

    def _process(self, data):
        pass

    def _process_and_send(self, data):
        p_data = self._process(data)
        while True:
            try:
                self.output_queue.put(p_data, block=True, timeout=0.1)
                break
            except Queue.Full:
                pass

    def _run(self):
        while True:
            if self._stopping:
                if self.input_queue.qsize() <= 0:
                    break
            try:
                data = self.input_queue.get(block=True, timeout=0.1)
                if data is None:
                    break
                self._process_and_send(data)
            except Queue.Empty:
                pass

    def run(self):
        super(Operator, self).run()
        list(map(lambda x: x.stop(), [
             self.runtime_context.operator, self.runtime_context.processor]))


class Processor(OThread):

    @property
    def queue(self):
        return self.runtime_context.output_queue

    def _process(self, data):
        pass

    def _run(self):
        while True:
            if self._stopping:
                if self.queue.qsize() <= 0 and self.runtime_context.operator.stopped():
                    break
            try:
                data = self.queue.get(block=True, timeout=0.1)
                self._process(data)
            except Queue.Empty:
                pass

    def run(self):
        super(Processor, self).run()
        list(map(lambda x: x.stop(), [
             self.runtime_context.reader, self.runtime_context.operator]))


class MultithreadRunner(Runner):

    def __init__(self, config):
        super(MultithreadRunner, self).__init__(config)
        queue_size = self.config.max_operator * 2
        config.runtime_context.input_queue = Queue.Queue(queue_size)
        config.runtime_context.output_queue = Queue.Queue(queue_size)

    def setup(self):
        runtime_context = self.config.runtime_context
        runtime_context.reader = MultithreadManager(
            [self.config.reader_cls(self.config, tid=i)
             for i in range(0, self.config.max_reader)]
        )
        runtime_context.reader.setDaemon(True)

        runtime_context.operator = MultithreadManager(
            [self.config.operator_cls(self.config, tid=i)
             for i in range(0, self.config.max_operator)]
        )

        runtime_context.processor = MultithreadManager(
            [self.config.processor_cls(self.config, tid=i)
             for i in range(0, self.config.max_processor)]
        )

    def start(self):
        runtime_context = self.config.runtime_context
        m = [runtime_context.reader, runtime_context.operator,
             runtime_context.processor]
        list(map(lambda x: x.start(), m))

        while not any([x.stopped() for x in m]):
            time.sleep(1)

        list(map(lambda x: x.join(), [
             runtime_context.operator, runtime_context.processor]))

    def stop(self, what=()):
        runtime_context = self.config.runtime_context
        list(map(lambda x: x.stop(), [
             runtime_context.operator, runtime_context.processor]))
