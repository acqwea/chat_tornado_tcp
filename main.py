__author__ = 'Administrator'

from tornado.tcpserver import TCPServer
from tornado.ioloop  import IOLoop
import connection


class ChatServer(TCPServer):
    def __init__(self, io_loop=None, **kwargs):
        TCPServer.__init__(self, io_loop=io_loop, **kwargs)

    def handle_stream(self, stream, address):
        try:
            connection.Connection(stream, address, io_loop=self.io_loop)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    print("chat server start ......")
    try:
        server = ChatServer()
        server.listen(8889)
        IOLoop.instance().start()
    except Exception as e:
        print(e)