# encoding: utf-8
import functools
import os
import struct
import asyncio
import tempfile
from py2py import async
from util import HierarchyLogger


class Client(object):
    def __init__(self, address, reader, writer, is_outgoing):
        self.address = address
        self.reader = reader
        self.writer = writer
        self.is_outgoing = is_outgoing

    def __repr__(self):
        return "Client(%s, %s)" % (self.address, "out" if self.is_outgoing else "in")

    def close(self):
        self.writer.close()

    def send(self, data):
        print("Send:", data)
        self.writer.write(data)

    def _sendfile_read_ready(self, file, done_fut):
        try:
            data = file.read(65536)
            if data:
                self.writer.write(data)
            else:
                asyncio.get_event_loop().remove_reader(file)
                done_fut.set_result(file)
        except Exception as e:
            done_fut.set_exception(e)

    def sendfile(self, file, metadata):
        done_fut = asyncio.futures.Future()
        self.sendmessage(metadata)
        file_size = os.fstat(file.fileno()).st_size

        self.send(struct.pack("!I", file_size))

        asyncio.get_event_loop().add_reader(
            file.fileno(), functools.partialmethod(self._sendfile_read_ready, file, done_fut))
        return done_fut

    @async.task
    def recvfile(self):
        metadata = yield from self.recvmessage()
        f = tempfile.TemporaryFile()
        file_size = (yield from self.recvdata("I"))[0]
        while file_size > 0:
            data = yield from self.reader.read(min(65536, file_size))
            data_size = len(data)
            f.write(data)
            if data_size == 0:
                break

            file_size -= data_size
        f.seek(0)
        return f, metadata

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



