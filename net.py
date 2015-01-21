# encoding: utf-8
import socket
import threading
import select
import Queue
import async
from util import HierarchyLogger


class Listener(threading.Thread):
    CONNECTION_ACCEPTED = object()

    def __init__(self, port, processor, logger):
        super(Listener, self).__init__()
        self.processor = processor
        self.port = port
        self.running = False
        self.sock = None
        self.logger = HierarchyLogger(lambda: u"Listener", logger)

    def run(self):
        self.running = True
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(5)
        self.logger.info(u"Listening to port %s" % self.port)
        while self.running:
            client_sock, remote_address = self.sock.accept()
            if not self.running:
                self.logger.info(u"Shut down")
                return
            self.logger.info(u"Accepted connection from: %s", unicode(remote_address))
            self.processor.signal(self.CONNECTION_ACCEPTED, client_sock)
        self.logger.info(u"Shut down")

    def stop(self):
        self.running = False
        self.logger.info(u"Stopping...")
        if self.sock:
            try:
                sock = socket.socket()
                sock.connect(("localhost", self.port))
                sock.close()
                self.logger.info(u"Closed socket")
            except Exception as e:
                self.logger.warn(u"Could not shut down socket: %s", e)


class Client(object):
    def __init__(self):
        self.incoming = Queue.Queue()
        self.outgoing = Queue.Queue()


class Network(async.EventThread):
    CLIENT_ACCEPTED = object()

    def __init__(self, processor, *args, **kwargs):
        super(Network, self).__init__(*args, **kwargs)
        self.processor = processor
        self.clients = {}
        self.outstanding_writes = {}
        self.empty = []
        self.select_args = []
        if self.async:
            self.select_args = [0]

    def connect(self, address):
        sock = socket.socket()
        sock.connect(address)

        client = Client()
        self.clients[sock] = client
        return client

    def on_connection_accepted(self, signal_code, sock):
        client = Client()
        self.clients[sock] = client
        self.processor.signal(self.CLIENT_ACCEPTED, client)

    def stop(self):
        self.running = False
        self.join()

    def setup(self):
        self.processor.register(Listener.CONNECTION_ACCEPTED, self.on_connection_accepted)

    def step(self):
        read_list, write_list, x_list = select.select(self.clients, self.clients, self.empty, *self.select_args)
        print read_list, write_list, x_list
        for sock in read_list:
            self.clients[sock].incoming.put(sock.read(65536))
        for sock in write_list:
            data = None
            if sock not in self.outstanding_writes:
                try:
                    data = self.clients[sock].outgoing.get_nowait()
                    self.outstanding_writes[sock] = data
                except Queue.Empty:
                    pass
            else:
                data = self.outstanding_writes[sock]
            if data:
                sent = sock.send(data)
                data = data[sent:]
                if data:
                    self.outstanding_writes[sock] = data
                else:
                    del self.outstanding_writes[sock]
        if not read_list and not write_list:
            return True

    def teardown(self):
        self.processor.unregister(Listener.CONNECTION_ACCEPTED, self.on_connection_accepted)



