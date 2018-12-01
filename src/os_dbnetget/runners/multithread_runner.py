import sys
import logging
import signal
import threading
import uuid
import time
from itertools import chain

from ..clients.sync_client import SyncClient
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

    def __init__(self, runner, **kwargs):
        if 'tid' in kwargs:
            self._tid = str(kwargs.pop('tid'))
        else:
            self._tid = str(uuid.uuid1())[:8]

        super(OThread, self).__init__(**kwargs)
        self._runner = runner
        self._stopping = False
        self._logger = logging.getLogger(
            self.__class__.__name__ + '.%s' % self._tid)

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
        return self._runner.input_queue

    @property
    def config(self):
        return self._runner.config

    def _run(self):
        for data in chain.from_iterable(self.config.files):
            data = data.strip()
            while True:
                if not data:
                    if self._stopping:
                        return
                    else:
                        break
                try:
                    self.queue.put(data, block=False, timeout=1)
                    self._logger.info(data)
                    break
                except Queue.Full:
                    continue
            if self._stopping:
                break

    def run(self):
        super(Reader, self).run()
        [self.queue.put(None)
         for _ in range(0, self.config.max_conn_concurrency)]
        self._runner.get_handler('reciever').stop()


class Reciever(OThread):

    @property
    def input_queue(self):
        return self._runner.input_queue

    @property
    def output_queue(self):
        return self._runner.output_queue

    def _process(self, data):
        while True:
            try:
                self._logger.info(data)
                self.output_queue.put(data, block=True, timeout=0.1)
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
                self._process(data)
            except Queue.Empty:
                pass

    def run(self):
        super(Reciever, self).run()
        list(map(lambda x: x.stop(), [self._runner.get_handler(n)
                                      for n in ('reciever', 'processor')]))


class Processor(OThread):

    @property
    def queue(self):
        return self._runner.output_queue

    def _run(self):
        while True:
            if self._stopping:
                if self.queue.qsize() <= 0 \
                        and self._runner.get_handler('reciever').stopped():
                    break
            try:
                data = self.queue.get(block=True, timeout=0.1)
                self._logger.info(data)
            except Queue.Empty:
                pass

    def run(self):
        super(Processor, self).run()
        list(map(lambda x: x.stop(), [self._runner.get_handler(n)
                                      for n in ('reader', 'reciever')]))


class MultithreadRunner(Runner):

    def __init__(self, config):
        super(MultithreadRunner, self).__init__(config)
        queue_size = self.config.max_conn_concurrency * 2
        self._input_queue = Queue.Queue(queue_size)
        self._output_queue = Queue.Queue(queue_size)
        self._handlers = {}

    @property
    def input_queue(self):
        return self._input_queue

    @property
    def output_queue(self):
        return self._output_queue

    def get_handler(self, name):
        return self._handlers[name]

    def setup(self):
        self._handlers['reader'] = MultithreadManager([Reader(self, tid=0), ])
        self._handlers['reader'].setDaemon(True)

        self._handlers['reciever'] = MultithreadManager(
            [Reciever(self, tid=i) for i in range(0, self.config.max_conn_concurrency)])

        self._handlers['processor'] = MultithreadManager(
            [Processor(self, tid=i) for i in range(0, self.config.max_proc_concurrency)])

    def start(self):
        m = self._handlers.values()
        list(map(lambda x: x.start(), m))

        while not any([x.stopped() for x in m]):
            time.sleep(1)

        list(map(lambda x: x.join(), [self.get_handler(x)
                                      for x in ('reciever', 'processor')]))

    def stop(self, what=()):
        list(map(lambda x: x.stop(), [self.get_handler(n)
                                      for n in ('reader', 'reciever')]))
