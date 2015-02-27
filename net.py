# encoding: utf-8
import struct
import asyncio
import async
from util import HierarchyLogger


class Client(object):
    def __init__(self, address, reader, writer, is_outgoing):
        self.address = address
        self.reader = reader
        self.writer = writer
        self.is_outgoing = is_outgoing
        self.id = None

    def __repr__(self):
        return "Client(%s: %s, %s)" % (self.id, self.address, "out" if self.is_outgoing else "in")

    def close(self):
        self.writer.close()

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


class Network(object):
    def __init__(self, client_accepted_cb=None):
        self.clients = {}
        self.logger = HierarchyLogger(lambda: "Network")
        self.listeners = {}
        self._client_accepted_cb = client_accepted_cb

    def set_client_accepted_cb(self, client_accepted_cb):
        self._client_accepted_cb = client_accepted_cb

    @async.task
    def connect(self, address):
        reader, writer = yield from asyncio.open_connection(*address)
        return self._create_client(reader, writer, True)

    def _create_client(self, reader, writer, is_outgoing):
        client = Client(writer.transport.get_extra_info('peername'), reader, writer, is_outgoing)
        self.clients[((reader, writer))] = client
        return client

    def _connection_accepted_cb(self, reader, writer):
        self._client_accepted_cb(self._create_client(reader, writer, False))

    def disconnect_client(self, client):
        client.close()
        del self.clients[(client.reader, client.writer)]

    @async.task
    def listen(self, port):
        if port not in self.listeners:
            listener = yield from asyncio.start_server(
                self._connection_accepted_cb,
                "0.0.0.0",
                port,
                reuse_address=True
            )
            self.listeners[port] = listener
            return
        else:
            raise Exception("Already listening on port %s!" % port)

    def stop_listening(self, port=None):
        if port is None:
            for port_, listener in list(self.listeners.items()):
                listener.close()
                del self.listeners[port_]
        else:
            self.listeners[port].close()
            del self.listeners[port]

    def stop(self):
        self.stop_listening()



