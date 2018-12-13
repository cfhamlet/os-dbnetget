from tornado.tcpserver import TCPServer
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
import struct


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
    def not_exist():
        return struct.pack('>i', 1)
    server = QDBServer(12, not_exist)
    server.listen(8888)
    IOLoop.current().start()
