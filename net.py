# encoding: utf-8
import socket
import select
import Queue
import async
import signals
from util import HierarchyLogger
import errno
from socket import error as socket_error


class Listener(async.EventThread):
    CONNECTION_ACCEPTED = signals.Signal('Listener.CONNECTION_ACCEPTED')

    def __init__(self, port, processor, logger, *args, **kwargs):
        super(Listener, self).__init__(*args, **kwargs)
        self.processor = processor
        self.port = port
        self.running = False
        self.sock = None
        self.logger = HierarchyLogger(lambda: u"Listener", logger)

    def setup(self):
        self.sock = socket.socket()
        if self.async:
            self.sock.setblocking(False)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(5)
        self.logger.info(u"Listening to port %s" % self.port)

    def step(self):
        try:
            client_sock, remote_address = self.sock.accept()
        except socket_error as e:
            if e.errno == errno.EAGAIN:
                return True
            else:
                raise

        self.logger.info(u"Accepted connection from: %s", unicode(remote_address))
        self.processor.signal(self.CONNECTION_ACCEPTED, (client_sock, remote_address))

    def teardown(self):
        self.logger.info(u"Shut down")
        self.sock.close()


class Client(object):
    def __init__(self, async=True):
        self.async = async
        self.incoming = Queue.Queue()
        self.outgoing = Queue.Queue()
        self.incoming_get = self.incoming.get_nowait if self.async else self.incoming.get

    def send(self, data):
        self.outgoing.put(data)

    def recv(self):
        return self.incoming_get()


class Network(async.EventThread):
    CLIENT_ACCEPTED = signals.Signal('Network.CLIENT_ACCEPTED')

    def __init__(self, processor, *args, **kwargs):
        super(Network, self).__init__(*args, **kwargs)
        self.processor = processor
        self.clients = {}
        self.outstanding_writes = {}
        self.empty = []
        self.select_args = []
        self.logger = HierarchyLogger(lambda: u"Network")
        self.listeners = {}
        if self.async:
            self.select_args = [0]

    def connect(self, address):
        sock = socket.socket()
        sock.connect(address)

        client = Client()
        self.clients[sock] = client
        return client

    def on_connection_accepted(self, signal_code, data):
        sock, sender_address = data
        client = Client()
        self.clients[sock] = client
        self.processor.signal(self.CLIENT_ACCEPTED, client)

    def listen(self, port, async=True):
        if port not in self.listeners:
            listener = Listener(port, self.processor, self.logger, async=async)
            self.listeners[port] = listener
            listener.run()
            return listener
        else:
            raise Exception(u"Already listening on port %s!" % port)

    def stop_listening(self, port=None):
        if port is None:
            for port_, listener in self.listeners.items():
                listener.stop()
                del self.listeners[port_]
        else:
            self.listeners[port].stop()
            del self.listeners[port]

    def setup(self):
        self.processor.register(Listener.CONNECTION_ACCEPTED, self.on_connection_accepted)

    def step(self):
        read_list, write_list, x_list = select.select(self.clients.keys(), self.clients.keys(), self.empty, *self.select_args)
        for sock in read_list:
            self.clients[sock].incoming.put(sock.recv(65536))

        sent_data = False
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
                sent_data = True
                data = data[sent:]
                if data:
                    self.outstanding_writes[sock] = data
                else:
                    del self.outstanding_writes[sock]

        listeners_done = True
        if self.async and self.listeners:
            for listener in self.listeners.values():
                if not listener.step():
                    listeners_done = False

        if listeners_done and not read_list and not sent_data:
            return True

    def teardown(self):
        self.stop_listening()
        self.processor.unregister(Listener.CONNECTION_ACCEPTED, self.on_connection_accepted)



