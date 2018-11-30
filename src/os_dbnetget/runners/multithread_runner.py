import logging
import Queue
import signal
import threading
import uuid
import time
from itertools import chain

from ..clients.sync_client import SyncClient
from .runner import Runner


class MultithreadManager(object):
    def __init__(self, workers):
        self._workers = workers

    def start(self):
        map(lambda w: w.start(), self._workers)

    def setDaemon(self, daemonic):
        map(lambda w: w.setDaemon(daemonic), self._workers)

    def join(self):
        map(lambda w: w.join(), self._workers)

    def stop(self):
        map(lambda w: w.stop(), self._workers)

    def stopped(self):
        return not any([w.isAlive() for w in self._workers])


class OThread(threading.Thread):

    def __init__(self, runner, **kwargs):
        if 'tid' in kwargs:
            self._tid = str(kwargs.pop('tid'))
        else:
            self._tid = str(uuid.uuid1())[:8]

        super(OThread, self).__init__(**kwargs)
        self._runner = runner
        self._stop = False
        self._logger = logging.getLogger(
            self.__class__.__name__ + '.%s' % self._tid)

    def run(self):
        try:
            self._run()
        except Exception as e:
            self._logger.error('Unexpected exception, %s' % str(e))

    def stop(self):
        self._stop = True


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
            while not self._stop or data:
                if not data:
                    break
                try:
                    self.queue.put(data, block=False, timeout=0.3)
                    self._logger.info(data)
                    break
                except Queue.Full:
                    continue
            if self._stop:
                break

    def run(self):
        super(Reader, self).run()
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
            if self._stop:
                if self._runner.get_handler('processor').stopped():
                    break
            try:
                self._logger.info(data)
                self.output_queue.put(data, block=True, timeout=0.1)
                break
            except Queue.Full:
                pass

    def _run(self):
        while True:
            if self._stop:
                if self.input_queue.qsize() <= 0 \
                        or self._runner.get_handler('processor').stopped():
                    break
            try:
                data = self.input_queue.get(block=True, timeout=1)
                self._process(data)
            except Queue.Empty:
                pass

    def run(self):
        super(Reciever, self).run()
        map(lambda x: x.stop(), [self._runner.get_handler(n)
                                 for n in ('reciever', 'processor')])


class Processor(OThread):

    @property
    def queue(self):
        return self._runner.output_queue

    def _run(self):
        while True:
            if self._stop:
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
        map(lambda x: x.stop(), [self._runner.get_handler(n)
                                 for n in ('reader', 'reciever')])


class MultithreadRunner(Runner):

    def __init__(self, config):
        super(MultithreadRunner, self).__init__(config)
        self._input_queue = Queue.Queue()
        self._output_queue = Queue.Queue()
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
            [Reciever(self, tid=i) for i in range(0, self._config.max_conn_concurrency)])

        self._handlers['processor'] = MultithreadManager(
            [Processor(self, tid=i) for i in range(0, self._config.max_proc_concurrency)])

    def run(self):
        m = self._handlers.values()
        map(lambda x: x.start(), m)

        while not any([x.stopped() for x in m]):
            time.sleep(1)

        map(lambda x: x.join(), [self.get_handler(x)
                                 for x in ('reciever', 'processor')])

    def stop(self, what=()):
        map(lambda x: x.stop(), [self.get_handler(n)
                                 for n in ('reader', 'reciever')])
