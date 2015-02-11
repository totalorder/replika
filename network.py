# encoding: utf-8
import queue
from collections import namedtuple
import os
from os.path import getsize, join as jn, exists, dirname
from os import makedirs, sep
import shutil
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
        CREATE: [('source_path', 's'), ('is_directory', '?')],
        DELETE: [('source_path', 's'), ('is_directory', '?')],
        MODIFY: [('source_path', 's'), ('size', 'I'), ('hash', 's'), ('modified_date', 'd')],
        MOVE: [('source_path', 's'), ('destination_path', 's'), ('is_directory', '?')]
    }

    header = "!BB"
    header_size = struct.calcsize(header)

    pack_format = {event_type: "".join([field_type if field_type != 's' else 'H'
                                        for field_name, field_type in fields_])
                   for event_type, fields_ in list(fields.items())}

    pack_size = {event_type: struct.calcsize("!" + pack_format_[event_type])
                 for event_type, pack_format_ in
                 zip(list(fields.keys()), [pack_format] * len(fields))}

    type = {id: namedtuple("Event%s" % id, ['type', 'sync_point'] + [field_name for field_name, field_type in fields_])
            for id, fields_ in list(fields.items())}

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


class ConnectionType(object):
    PEER = 0
    FILE_PIPE = 1


class NoDataReceivedException(Exception):
    pass


class Peer(threading.Thread):
    def __init__(self, id, client_id, ring, sock, address, received_messages, peer_dead_callback, get_sync_point_info,
                 signal, outgoing, logger):
        super(Peer, self).__init__()
        self.outgoing = outgoing
        self.direction = 'out' if self.outgoing else 'in'
        self.logger = HierarchyLogger(lambda: "Peer %s (%s)" % (self.id, self.direction), logger)
        self.id = id
        self.ring = ring
        self.sock = sock
        self.address = address
        self.running = False
        self.received_messages = received_messages
        self.unsent_messages = queue.Queue()
        self.peer_dead_callback = peer_dead_callback
        self.file_pipe = FilePipe(client_id, self.address, get_sync_point_info, signal, self.logger)

    def __repr__(self):
        return "Peer %s (%s)" % (self.id, self.direction)

    def stop(self):
        if self.running:
            self.logger.info("Stopping peer")
            self.running = False
            self.sock.close()
        else:
            self.logger.info("Closing peer")
            self.sock.close()

    def run(self):
        self.running = True
        self.logger.info("Peer receiving")
        t = threading.Thread(target=self._send_messages)
        t.daemon = True
        t.start()

        while self.running:
            try:
                msg = self.recvmessage(self.sock)
                evt = EventType.deserialize(msg)
                self.received_messages.put((self.id, evt))
            except NoDataReceivedException:
                self.logger.error("Socket is dead. Exiting")
                self.running = False
                self.peer_dead_callback(self)
                return

    def send_event(self, evt):
        self.unsent_messages.put(evt)

    def _send_messages(self):
        while self.running:
            try:
                evt = self.unsent_messages.get_nowait()
                msg = EventType.serialize(evt)
                self.sendmessage(self.sock, msg)
            except queue.Empty:
                pass
            time.sleep(1)

    def add_file_pipe(self, sock):
        self.file_pipe.create_receiver(sock)

    def send_file(self, sync_point, mount_path, path):
        self.file_pipe.send_file(sync_point, mount_path, path)

    @classmethod
    def create_from_accepted_socket(cls, sock, address, id, ring, received_messages, peer_dead, get_sync_point_names,
                                    signal, logger):
        remote_id = cls.recvmessage(sock)
        remote_ring = cls.recvmessage(sock)
        listen_port = cls.recvdata(sock, "H")
        logger.info("Connected from %s (listen: %s)", remote_id, listen_port)
        cls.sendmessage(sock, id)
        cls.sendmessage(sock, ring)
        return cls(remote_id, id, remote_ring, sock, (address[0], listen_port), received_messages, peer_dead,
                   get_sync_point_names, signal, False, logger)

    @classmethod
    def create_by_connecting(cls, address, id, ring, listen_port, recevied_messages, peer_dead, get_sync_point_names,
                             signal, logger):
        sock = socket.socket()
        sock.connect(address)
        cls.senddata(sock, "B", ConnectionType.PEER)
        cls.sendmessage(sock, id)
        cls.sendmessage(sock, ring)
        cls.senddata(sock, "H", listen_port)
        remote_id = cls.recvmessage(sock)
        remote_ring = cls.recvmessage(sock)
        logger.info("Connected to %s", remote_id)
        return cls(remote_id, id, remote_ring, sock, address, recevied_messages, peer_dead, get_sync_point_names,
                   signal, True, logger)

    @classmethod
    def sendmessage(cls, sock, msg):
        cls.sendbytes(sock, struct.pack("!I", len(msg)) + msg)

    @classmethod
    def senddata(cls, sock, format, data):
        packed = struct.pack("!" + format, data)
        cls.sendbytes(sock, packed)

    @classmethod
    def recvdata(cls, sock, format):
        data = cls.recvbytes(sock, struct.calcsize(format))
        return struct.unpack("!" + format, data)[0]

    @classmethod
    def recvmessage(cls, sock):
        num = struct.unpack("!I", cls.recvbytes(sock, 4))[0]
        msg = cls.recvbytes(sock, num)
        return msg

    @staticmethod
    def recvbytes(sock, num):
        chunks = []
        bytes_recd = 0
        while bytes_recd < num:
            chunk = sock.recv(min(num - bytes_recd, 2048))
            if chunk == '':
                raise NoDataReceivedException("socket connection broken")
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        return ''.join(chunks)

    @staticmethod
    def sendbytes(sock, msg):
        total_sent = 0
        msg_len = len(msg)
        while total_sent < msg_len:
            sent = sock.send(msg[total_sent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            total_sent = total_sent + sent


class ClientListener(threading.Thread):
    def __init__(self, port, connection_accepted_callback, logger):
        self.port = port
        self.connection_accepted_callback = connection_accepted_callback
        self.running = False
        self.sock = None
        self.logger = HierarchyLogger(lambda: "Listener", logger)
        super(ClientListener, self).__init__()

    def run(self):
        self.running = True
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(5)
        self.logger.info("Listening to port %s" % self.port)
        while self.running:
            clientsock, remote_address = self.sock.accept()
            if not self.running:
                self.logger.info("Shut down")
                return
            self.logger.info("Accepted connection from: %s", str(remote_address))
            callback = threading.Thread(target=self.connection_accepted_callback, args=(clientsock, remote_address))
            callback.daemon = True
            callback.start()
        self.logger.info("Shut down")

    def stop(self):
        self.running = False
        self.logger.info("Stopping...")
        if self.sock:
            try:
                sock = socket.socket()
                sock.connect(("localhost", self.port))
                sock.close()
                self.logger.info("Closed socket")
            except Exception as e:
                self.logger.warn("Could not shut down socket: %s", e)


class FilePipeSender(threading.Thread):
    def __init__(self, client_id, address, queue, sender_dead_callback, logger):
        self.queue = queue
        self.client_id = client_id
        self.address = address
        self.sender_dead_callback = sender_dead_callback
        self.logger = HierarchyLogger(lambda: "PipeSender", logger)
        self.attempts = 0
        self.max_attemps = 3
        super(FilePipeSender, self).__init__()

    def run(self):
        while 1:
            try:
                self._run()
                return
            except Exception as e:
                self.attempts += 1
                if self.attempts >= self.max_attemps:
                    self.logger.error("Unknown error: %s (stopping after %s attempts)", e, self.max_attemps)
                    return
                else:
                    self.logger.error("Unknown error: %s, address: %s (restarting)", e, self.address)
                    time.sleep(5)

    def _run(self):
        sock = socket.socket()
        sock.connect(self.address)
        self.logger.info("Connecting to %s", str(self.address))
        Peer.senddata(sock, "B", ConnectionType.FILE_PIPE)
        self.logger.info("Sending client_id: %s", self.client_id)
        Peer.sendmessage(sock, self.client_id)
        while 1:
            try:
                sync_point, mount_path, path = self.queue.get_nowait()
            except queue.Empty:
                break
            try:
                self.logger.info("Sending file %s - %s", sync_point, path)
                full_path = jn(mount_path, path)
                file_size = getsize(full_path)
                Peer.sendmessage(sock, sync_point)
                Peer.sendmessage(sock, path)
                Peer.senddata(sock, "I", file_size)
                Peer.senddata(sock, "d", os.path.getmtime(full_path))
                with open(full_path, 'rb') as f:
                    while 1:
                        chunk = f.read(65536)
                        if not chunk:
                            break  # EOF
                        sock.sendall(chunk)
            except Exception:
                self.queue.put((sync_point, mount_path, path))
                raise
            self.attempts = 0
        self.sender_dead_callback(self)


class FilePipeReceiver(threading.Thread):
    def __init__(self, sock, get_sync_point_info, signal, receiver_dead_callback, logger):
        self.sock = sock
        self.logger = HierarchyLogger(lambda: "PipeReceiver", logger)
        # self.logger.off()
        self.get_sync_point_info = get_sync_point_info
        self.receiver_dead_callback = receiver_dead_callback
        self.signal = signal
        super(FilePipeReceiver, self).__init__()

    def run(self):
        while 1:
            try:
                sync_point = Peer.recvmessage(self.sock)
            except NoDataReceivedException:
                self.logger.info("No more files to receive. Shutting down")
                self.sock.close()
                self.receiver_dead_callback(self)
                return

            if sync_point not in list(self.get_sync_point_info().keys()):
                self.logger.error("Receiving unknown sync point: %s", sync_point)
                self.sock.close()
                self.receiver_dead_callback(self)
                return
            file_path = Peer.recvmessage(self.sock).strip(sep)
            file_size = Peer.recvdata(self.sock, "I")
            file_modified_date = Peer.recvdata(self.sock, "d")
            self.logger.info("Receiving file: %s - %s (%s)", sync_point, file_path, file_size)
            sync_path = jn(".tmp", sync_point)
            full_path = jn(sync_path, file_path)
            file_dir = dirname(full_path)
            if not exists(file_dir):
                makedirs(file_dir)

            with open(full_path, 'wb') as f:
                bytes_read = 0
                while 1:
                    chunk = self.sock.recv(min(65536, file_size - bytes_read))
                    bytes_read += len(chunk)
                    f.write(chunk)

                    if bytes_read == file_size:
                        break
                    if not chunk:
                        raise Exception("No bytes left!")

            self.logger.info("Updating file: %s - %s (%s)", sync_point, file_path, file_size)
            target_path = jn(self.get_sync_point_info()[sync_point], file_path)
            target_dir = dirname(full_path)
            if not exists(target_dir):
                self.signal(sync_point, file_path, EventType.CREATE)
                makedirs(target_dir)
            os.utime(full_path, (os.path.getatime(full_path), file_modified_date))
            if exists(target_path):
                self.signal(sync_point, file_path, EventType.MODIFY)
            else:
                self.signal(sync_point, file_path, EventType.MODIFY)
            shutil.copy2(full_path, target_path)
        self.receiver_dead_callback(self)


class FilePipe(object):
    def __init__(self, client_id, address, get_sync_point_info, signal, logger, max_size=5):
        self.senders = []
        self.receivers = []
        self.queue = queue.Queue()
        self.max_size = max_size
        self.client_id = client_id
        self.address = address
        self.logger = logger
        self.get_sync_point_info = get_sync_point_info
        self.signal = signal

    def send_file(self, sync_point, mount_path, path):
        self.queue.put((sync_point, mount_path, path))
        if len(self.senders) < self.max_size:
            worker = FilePipeSender(self.client_id, self.address, self.queue, self.sender_dead, self.logger)
            worker.daemon = True
            self.senders.append(worker)
            worker.start()

    def create_receiver(self, sock):
        receiver = FilePipeReceiver(sock, self.get_sync_point_info, self.signal, self.receiver_dead, self.logger)
        self.receivers.append(receiver)
        receiver.start()

    def receiver_dead(self, receiver):
        self.receivers.remove(receiver)

    def sender_dead(self, sender):
        self.senders.remove(sender)