# encoding: utf-8
import socket
import select
import queue
import struct
import asyncio
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
        self.logger = HierarchyLogger(lambda: "Listener", logger)
        self.server = None

    def client_connected_cb(self, reader, writer):
        self.logger.info("Accepted connection")
        self.processor.signal(self.CONNECTION_ACCEPTED, (reader, writer))

    @async.task
    def setup(self):
        self.server = yield from asyncio.start_server(self.client_connected_cb,
                                                 "0.0.0.0", self.port)
        self.logger.info("Listening to port %s" % self.port)
        return

    @async.task
    def step(self):
        return

    @async.task
    def teardown(self):
        return

    def stop(self):
        if self.server:
            self.server.close()


class Client(object):
    class NoDataReceived(Exception):
        pass

    def __init__(self, address, reader, writer, is_outgoing):
        self.async = async
        self.address = address
        self.reader = reader
        self.writer = writer
        self.is_outgoing = is_outgoing
        self.id = None

    def __repr__(self):
        return "Client(%s: %s, %s)" % (self.id, self.address, "out" if self.is_outgoing else "in")

    def send(self, data):
        print("Send:", data)
        self.writer.write(data)

    @async.task
    def recv(self):
        data = yield from self.reader.read(1024)
        return data

    def sendmessage(self, msg):
        self.send(struct.pack("!I", len(msg)) + msg)

    def senddata(self, format, *data):
        packed = struct.pack("!" + format, *data)
        self.send(packed)

    @async.task
    def recvdata(self, format):
        data = yield from self.recvbytes(struct.calcsize(format))
        return struct.unpack("!" + format, data)

    @async.task
    def recvmessage(self):
        bytes = yield from self.recvbytes(4)
        num = struct.unpack("!I", bytes)[0]
        msg = yield from self.recvbytes(num)
        return msg

    @async.task
    def recvbytes(self, num):
        data = yield from self.reader.readexactly(num)
        return data


class Network(async.EventThread):
    CLIENT_ACCEPTED = signals.Signal('Network.CLIENT_ACCEPTED')
    DO_CONNECT = signals.Signal('Network.DO_CONNECT')
    FAILED_DO_CONNECT = signals.Signal('Network.FAILED_DO_CONNECT')

    def __init__(self, processor, *args, **kwargs):
        super(Network, self).__init__(*args, **kwargs)
        self.processor = processor
        self.clients = {}
        self.outstanding_writes = {}
        self.empty = []
        self.select_args = []
        self.logger = HierarchyLogger(lambda: "Network")
        self.listeners = {}
        if self.async:
            self.select_args = [0]
        self.outstanding_connects = queue.Queue()

    @async.task
    def connect(self, address):
        reader, writer = yield from asyncio.open_connection(*address)

        client = Client(address, reader, writer, True)
        self.clients[(reader, writer)] = client
        return client

    def disconnect_client(self, client):
        for sock, client_ in self.clients:
            if client_ == client:
                sock.close()
                del self.clients[sock]
                return
        raise Exception("Client %s not connected!")

    def on_connection_accepted(self, signal_code, data):
        reader, writer = data
        client = Client(None, reader, writer, False)
        self.clients[((reader, writer))] = client
        self.processor.signal(self.CLIENT_ACCEPTED, client)

    def listen(self, port, async=True):
        if port not in self.listeners:
            listener = Listener(port, self.processor, self.logger, async=async)
            self.listeners[port] = listener
            return listener.step_until_done()
        else:
            raise Exception("Already listening on port %s!" % port)

    def stop_listening(self, port=None):
        if port is None:
            for port_, listener in list(self.listeners.items()):
                listener.stop()
                del self.listeners[port_]
        else:
            self.listeners[port].stop()
            del self.listeners[port]

    def stop(self):
        self.stop_listening()
        self.processor.unregister(Listener.CONNECTION_ACCEPTED, self.on_connection_accepted)
        self.processor.unregister(Network.DO_CONNECT, self.on_do_connect)

    def on_do_connect(self, signal_code, address):
        client_fut = self.connect(address)
        client_fut.add_done_callback(lambda client: self.processor.signal(self.CLIENT_ACCEPTED, client.result()))

    @async.task
    def setup(self):
        self.processor.register(Listener.CONNECTION_ACCEPTED, self.on_connection_accepted)
        self.processor.register(Network.DO_CONNECT, self.on_do_connect)
        return

    @async.task
    def step(self):
        return
        read_list, write_list, x_list = select.select(list(self.clients.keys()), list(self.clients.keys()), self.empty, *self.select_args)
        for sock in read_list:
            self.clients[sock].incoming.put(sock.recv(65536))

        sent_data = False
        for sock in write_list:
            data = None
            if sock not in self.outstanding_writes:
                try:
                    data = self.clients[sock].outgoing.get_nowait()
                    self.outstanding_writes[sock] = data
                except queue.Empty:
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
            for listener in list(self.listeners.values()):
                if not listener.step():
                    listeners_done = False

        did_connect = False
        try:
            signal_code, address = self.outstanding_connects.get_nowait()
            did_connect = True
            try:
                client = self.connect(address)
                self.processor.signal(Network.CLIENT_ACCEPTED, client)
            except socket_error as e:
                if e.errno == errno.ECONNREFUSED:
                    self.processor.signal(Network.FAILED_DO_CONNECT, address)
                else:
                    raise
        except queue.Empty:
            pass

        if listeners_done and not read_list and not sent_data and not did_connect:
            return True

    @async.task
    def teardown(self):
        return



