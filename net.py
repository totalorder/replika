# encoding: utf-8
import socket
import select
import Queue
import struct
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
    class NoDataReceived(Exception):
        pass

    def __init__(self, address, is_outgoing, async=True):
        self.async = async
        self.address = address
        self.is_outgoing = is_outgoing
        self.incoming = Queue.Queue()
        self.outgoing = Queue.Queue()
        self.incoming_get = self.incoming.get_nowait if self.async else self.incoming.get
        self.outstanding_received_data = ""
        self.id = None

    def __repr__(self):
        return "Client(%s: %s, %s)" % (self.id, self.address, "out" if self.is_outgoing else "in")

    def send(self, data):
        print "Send:", data
        self.outgoing.put(data)

    def recv(self):
        try:
            return self.incoming_get()
        except Queue.Empty:
            raise Client.NoDataReceived

    def sendmessage(self, msg):
        self.send(struct.pack("!I", len(msg)) + msg)

    def senddata(self, format, *data):
        packed = struct.pack("!" + format, *data)
        self.send(packed)

    def recvdata(self, format):
        data = self.recvbytes(struct.calcsize(format))
        return struct.unpack("!" + format, data)

    def recvmessage(self, async=False):
        def async_recvmessage():
            async_bytes = self.recvbytes(4, async)
            print "async_bytes, async", async_bytes, async
            bytes = None
            if async:
                for result in async_bytes:
                    if result is None:
                        yield
                    else:
                        bytes = result
                        break
            else:
                bytes = async_bytes

            num = struct.unpack("!I", bytes)[0]
            msg = self.recvbytes(num, async)
            if async:
                for result in msg:
                    yield result
            else:
                yield msg

        if async:
            return async_recvmessage()
        else:
            return async_recvmessage().next()

    def recvbytes(self, num, async=False):
        def async_recvbytes():
            chunks = []
            bytes_recd = 0
            if self.outstanding_received_data:
                chunks.append(self.outstanding_received_data)
                bytes_recd += len(self.outstanding_received_data)

            while 1:
                if bytes_recd >= num:
                    data = "".join(chunks)
                    self.outstanding_received_data = data[num:]
                    yield data[:num]
                    return
                while 1:
                    try:
                        chunk = self.recv()
                        break
                    except Client.NoDataReceived:
                        self.outstanding_received_data = ''.join(chunks)
                        if async:
                            yield
                        else:
                            raise
                #print "Recv: ", chunk
                chunks.append(chunk)
                bytes_recd += len(chunk)
        if not async:
            return async_recvbytes().next()
        else:
            return async_recvbytes()


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
        self.logger = HierarchyLogger(lambda: u"Network")
        self.listeners = {}
        if self.async:
            self.select_args = [0]
        self.outstanding_connects = Queue.Queue()

    def connect(self, address):
        sock = socket.socket()
        sock.connect(address)

        client = Client(address, True)
        self.clients[sock] = client
        return client

    def disconnect_client(self, client):
        for sock, client_ in self.clients:
            if client_ == client:
                sock.close()
                del self.clients[sock]
                return
        raise Exception("Client %s not connected!")

    def on_connection_accepted(self, signal_code, data):
        sock, sender_address = data
        client = Client(sender_address, False)
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
        self.processor.register(Network.DO_CONNECT, self.outstanding_connects)

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
        except Queue.Empty:
            pass

        if listeners_done and not read_list and not sent_data and not did_connect:
            return True

    def teardown(self):
        self.stop_listening()
        self.processor.unregister(Listener.CONNECTION_ACCEPTED, self.on_connection_accepted)
        self.processor.unregister(Network.DO_CONNECT, self.outstanding_connects)



