import argparse
import struct
from importlib import import_module

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.tcpserver import TCPServer


def load_func(func_path):
    module_path, func_name = func_path.rsplit('.', 1)[-1]
    mod = import_module(module_path)
    return getattr(mod, func_name)


class QDBServer(TCPServer):
    def __init__(self, expected_code, output_func):
        super(QDBServer, self).__init__()
        self._expected_code = expected_code
        self._output_func = output_func

    @gen.coroutine
    def handle_stream(self, stream, address):
        while True:
            try:
                data = yield stream.read_bytes(1+4+16+4)
                cmd, key_length, key, flag = struct.unpack('>bi16si', data)
                print('Recv {}'.format((cmd, key_length, str(key), flag)))

                if cmd != self._expected_code:
                    stream.close()
                    break
                yield stream.write(self._output_func())
            except StreamClosedError:
                print('Closed {}'.format(address))
                try:
                    stream.close()
                except:
                    pass
                break


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, dest='port')
    parser.add_argument('--expected-code', type=int, dest='expected_code')
    parser.add_argument('--ouput-func', dest='output_func')
    args = parser.parse_args()
    server = QDBServer(parser.expected_code, load_func(parser.output_func))
    server.listen(args.port)
    IOLoop.current().start()
