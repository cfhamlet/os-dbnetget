try:
    import SocketServer as socketserver
except:
    import socketserver

import socket
import argparse
import struct
from importlib import import_module


def load_func(func_path):
    module_path, func_name = func_path.rsplit('.', 1)
    mod = import_module(module_path)
    return getattr(mod, func_name)


class QDBHandler(socketserver.BaseRequestHandler):

    def output_helloworld(self):
        data = b'hello world!'
        l = len(data)
        return struct.pack('>ii%ds' % l, 0, l, data)

    def output_not_exist(self):
        return struct.pack('>i', 1)

    def handle(self):
        while True:
            try:
                data = self.request.recv(1+4+16+4)
                if not data:
                    self.request.close()
                    break
                cmd, key_length, key, flag = struct.unpack('>bi16si', data)
                print('Recv {}'.format((cmd, key_length, str(key), flag)))

                if cmd != self.expected_code:
                    self.request.close()
                    break
                self.request.sendall(self.output_func())
            except socket.error as e:
                print('Error {} {}'.format(e, self.request))
                try:
                    self.request.close()
                except:
                    pass
                break


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, dest='port')
    parser.add_argument('--expected-code', type=int, dest='expected_code')
    parser.add_argument('--output-func', dest='output_func')
    args = parser.parse_args()
    QDBHandler.expected_code = args.expected_code
    QDBHandler.output_func = getattr(QDBHandler, args.output_func)
    server = socketserver.TCPServer(('localhost', args.port), QDBHandler)
    server.serve_forever()
