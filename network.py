# encoding: utf-8
import Queue
from collections import namedtuple
import struct
import socket
import threading
import time
from util import HierarchyLogger


class EventType(object):
    FETCH = 0
    CREATE = 1
    DELETE = 2
    MODIFY = 3
    MOVE = 4

    fields = {
        FETCH: [('source_path', 's')],
        CREATE: [('source_path', 's')],
        DELETE: [('source_path', 's')],
        MODIFY: [('source_path', 's'), ('size', 'H')],
        MOVE: [('source_path', 's'), ('destination_path', 's')]
    }

    header = "!BB"
    header_size = struct.calcsize(header)

    pack_format = {event_type: "".join([field_type if field_type != 's' else 'H'
                                        for field_name, field_type in fields_])
                   for event_type, fields_ in fields.items()}

    pack_size = {event_type: struct.calcsize(pack_format_[event_type])
                 for event_type, pack_format_ in
                 zip(fields.keys(), [pack_format] * len(fields))}

    type = {id: namedtuple("Event%s" % id, ['type', 'sync_point'] + [field_name for field_name, field_type in fields_])
            for id, fields_ in fields.items()}

    @staticmethod
    def serialize(evt):
        pack = [evt.type, len(evt.sync_point)]
        strings = [evt.sync_point]
        for field_name, field_type in EventType.fields[evt.type]:
            if field_type != 's':
                pack.append(getattr(evt, field_name))
            else:
                string = getattr(evt, field_name)
                pack.append(len(string))
                strings.append(string)
        return struct.pack(EventType.header + EventType.pack_format[evt.type], *pack) + "".join(strings)

    @staticmethod
    def deserialize(msg):
        pos = EventType.header_size
        event_type, sync_point_len = struct.unpack(EventType.header, msg[:pos])

        unpacked = struct.unpack("!" + EventType.pack_format[event_type],
                                 msg[pos:pos + EventType.pack_size[event_type]])
        pos += EventType.pack_size[event_type]

        sync_point = msg[pos:pos + sync_point_len]
        pos += sync_point_len

        fields = []
        for idx, field in enumerate(unpacked):
            field_name, field_type = EventType.fields[event_type][idx]
            if field_type != 's':
                fields.append(field)
            else:
                fields.append(msg[pos:pos + field])
                pos += field
        return EventType.type[event_type](event_type, sync_point, *fields)

    @staticmethod
    def create(type, sync_point, *args, **kwargs):
        return EventType.type[type](type, sync_point, *args, **kwargs)


class Peer(threading.Thread):
    def __init__(self, id, ring, sock, address, recevied_messages, logger):
        super(Peer, self).__init__()
        self.logger = HierarchyLogger(lambda: u"Peer %s" % self.id, logger)
        self.id = id
        self.ring = ring
        self.sock = sock
        self.address = address
        self.running = False
        self.recevied_messages = recevied_messages
        self.unsent_messages = Queue.Queue()

    def stop(self):
        if self.running:
            self.logger.info(u"Stopping peer")
            self.running = False
            self.sock.close()

    def run(self):
        self.running = True
        self.logger.info(u"Peer receiving")
        t = threading.Thread(target=self._send_messages)
        t.daemon = True
        t.start()

        while self.running:
            msg = self.recvmessage(self.sock)
            evt = EventType.deserialize(msg)
            # logging.info(u"%s: Received event: %s", self.id, evt)
            self.recevied_messages.put((self.id, evt))

    def send(self, evt):
        self.unsent_messages.put(evt)

    def _send_messages(self):
        while self.running:
            try:
                evt = self.unsent_messages.get_nowait()
                msg = EventType.serialize(evt)
                self.sendmessage(self.sock, msg)
            except Queue.Empty:
                pass
            time.sleep(1)

    @classmethod
    def create_from_accepted_socket(cls, sock, address, id, ring, recevied_messages, logger):
        remote_id = cls.recvmessage(sock)
        remote_ring = cls.recvmessage(sock)
        cls.sendmessage(sock, id)
        cls.sendmessage(sock, ring)
        return cls(remote_id, remote_ring, sock, address, recevied_messages, logger)

    @classmethod
    def create_by_connecting(cls, address, id, ring, recevied_messages, logger):
        sock = socket.socket()
        sock.connect(address)
        cls.sendmessage(sock, id)
        cls.sendmessage(sock, ring)
        remote_id = cls.recvmessage(sock)
        remote_ring = cls.recvmessage(sock)
        return cls(remote_id, remote_ring, sock, address, recevied_messages, logger)

    @classmethod
    def sendmessage(cls, sock, msg):
        # logging.info(u"Send: %s", msg)
        cls.sendbytes(sock, struct.pack("!I", len(msg)) + msg)

    @classmethod
    def recvmessage(cls, sock):
        num = struct.unpack("!I", cls.recvbytes(sock, 4))[0]
        msg = cls.recvbytes(sock, num)
        # logging.info(u"Recv: %s", msg)
        return msg

    @staticmethod
    def recvbytes(sock, num):
        chunks = []
        bytes_recd = 0
        while bytes_recd < num:
            chunk = sock.recv(min(num - bytes_recd, 2048))
            if chunk == '':
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        return ''.join(chunks)

    @staticmethod
    def sendbytes(sock, msg):
        totalsent = 0
        msglen = len(msg)
        while totalsent < msglen:
            sent = sock.send(msg[totalsent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent


class ClientListener(threading.Thread):
    def __init__(self, port, connection_accepted_callback, logger):
        self.port = port
        self.connection_accepted_callback = connection_accepted_callback
        self.running = False
        self.sock = None
        self.logger = HierarchyLogger(lambda: u"Listener", logger)
        super(ClientListener, self).__init__()

    def run(self):
        self.running = True
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(5)
        self.logger.info(u"Listening to port %s" % self.port)
        while self.running:
            clientsock, remote_address = self.sock.accept()
            if not self.running:
                self.logger.info(u"Shut down")
                return
            self.logger.info(u"Accepted connection from: %s", unicode(remote_address))
            callback = threading.Thread(target=self.connection_accepted_callback, args=(clientsock, remote_address))
            callback.daemon = True
            callback.start()
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