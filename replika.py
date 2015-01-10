# encoding: utf-8
import Queue
from collections import namedtuple
import sys
import threading
import time
import logging
import socket
import struct
import random
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, os
import errno
from socket import error as socket_error
from os.path import exists, getsize, join as jn


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


class LoggingEventHandler(FileSystemEventHandler):
    def __init__(self, id, send):
        self.id = id
        self.send = send
        super(LoggingEventHandler, self).__init__()

    def on_moved(self, event):
        super(LoggingEventHandler, self).on_moved(event)

        what = u'directory' if event.is_directory else u'file'
        msg = u"%s: Moved %s: from %s to %s" % (self.id, what, event.src_path, event.dest_path)
        # logging.info(msg)
        self.send(EventType.create(EventType.MOVE, self.id, source_path=event.src_path,
                                   destination_path=event.dest_path))

    def on_created(self, event):
        super(LoggingEventHandler, self).on_created(event)

        what = u'directory' if event.is_directory else u'file'
        msg = u"%s: Created %s: %s" % (self.id, what, event.src_path)
        # logging.info(msg)
        self.send(EventType.create(EventType.CREATE, self.id, source_path=event.src_path))

    def on_deleted(self, event):
        super(LoggingEventHandler, self).on_deleted(event)

        what = u'directory' if event.is_directory else u'file'
        msg = u"%s: Deleted %s: %s" % (self.id, what, event.src_path)
        # logging.info(msg)
        self.send(EventType.create(EventType.DELETE, self.id, source_path=event.src_path))

    def on_modified(self, event):
        super(LoggingEventHandler, self).on_modified(event)

        what = u'directory' if event.is_directory else u'file'
        msg = u"%s: Modified %s: %s" % (self.id, what, event.src_path)
        # logging.info(msg)
        self.send(EventType.create(EventType.MODIFY, self.id, source_path=event.src_path,
                                   size=getsize(event.src_path)))


class RemoteEventHandler(object):
    def __init__(self, mount_point, send, logger):
        self.logger = logger
        self.mount_point = mount_point
        self.send = send
        self.handlers = {EventType.FETCH: self.on_fetch,
                    EventType.CREATE: self.on_create,
                    EventType.DELETE: self.on_delete,
                    EventType.MODIFY: self.on_modify,
                    EventType.MOVE: self.on_move}

    def on_fetch(self, evt, sender):
        self.logger.info(u"FETCH %s from %s", evt, sender)

    def on_create(self, evt, sender):
        self.logger.info(u"CREATE %s from %s", evt, sender)

    def on_delete(self, evt, sender):
        self.logger.info(u"DELETE %s from %s", evt, sender)

    def on_modify(self, evt, sender):
        self.logger.info(u"MODIFY %s from %s", evt, sender)
        source_path = jn(self.mount_point, evt.source_path)
        source_exists = exists(source_path)
        if not source_exists or (source_exists and getsize(source_path) != evt.size):
            self.send(EventType.create(EventType.FETCH, evt.sync_point, evt.source_path), sender)
        else:
            print "Skip"

    def on_move(self, evt, sender):
        self.logger.info(u"MOVE %s from %s", evt, sender)

    def on_event(self, evt, sender):
        return self.handlers[evt.type](evt, sender)


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


class SyncPoint(object):
    def __init__(self, id, mount_path, send, logger):
        self.id = id
        self.mount_path = mount_path
        self.started = False
        self.watch = None
        self.send = send
        self.logger = HierarchyLogger(lambda: u"SyncPoint %s" % self.id, logger)
        self.remote_handler = RemoteEventHandler(mount_path, send, logger)

    def start(self, observer):
        if self.started:
            return False
        else:
            event_handler = LoggingEventHandler(self.id, self.send)
            self.watch = observer.schedule(event_handler, self.mount_path, recursive=True)
            return True

    def stop(self, observer):
        if self.started:
            observer.unschedule(self.watch)
            self.started = False
            self.watch = None
            return True
        else:
            return False

    def on_event(self, evt, sender):
        self.remote_handler.on_event(evt, sender)


class Listener(threading.Thread):
    def __init__(self, port, connection_accepted_callback, logger):
        self.port = port
        self.connection_accepted_callback = connection_accepted_callback
        self.running = False
        self.sock = None
        self.logger = HierarchyLogger(lambda: u"Listener", logger)
        super(Listener, self).__init__()

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


class HierarchyLogger(object):
    def __init__(self, get_prefix, parent=None):
        self.get_prefix = get_prefix
        self.parent = parent

    def prefix(self):
        if self.parent is not None:
            return self.parent.prefix() + u", " + self.get_prefix()
        else:
            return self.get_prefix()

    def info(self, logstr, *args, **kwargs):
        logging.info(self.prefix() + u": " + logstr, *args, **kwargs)

    def warn(self, logstr, *args, **kwargs):
        logging.warn(self.prefix() + u": " + logstr, *args, **kwargs)

    def error(self, logstr, *args, **kwargs):
        logging.error(self.prefix() + u": " + logstr, *args, **kwargs)

    def debug(self, logstr, *args, **kwargs):
        logging.debug(self.prefix() + u": " + logstr, *args, **kwargs)

class Client(threading.Thread):
    def __init__(self, id, ring):
        super(Client, self).__init__()
        self.id = id
        self.ring = ring
        self.peers = {}
        self.sync_points = {}
        self.observer = Observer()
        self.running = False
        self.logger = HierarchyLogger(lambda: u"Client %s" % self.id)
        self.listener = Listener(5000 + int(self.id), self.connection_accepted, self.logger)
        self.connections_pending = []
        self.recevied_messages = Queue.Queue()


    def connection_accepted(self, sock, address):
        peer = Peer.create_from_accepted_socket(sock, address, self.id, self.ring, self.recevied_messages, self.logger)
        self._add_peer(peer)

    def _add_peer(self, peer):
        if peer.ring != self.ring:
            self.logger.info(u"Remote %s ring %s differs from local ring %s", peer.id, peer.ring, self.id)
            return
        if peer.id in self.peers:
            self.logger.info(u"Remote peer %s already connected", peer.id)
            return

        self.logger.info(u"Adding peer %s", peer.id)

        self.peers[peer.id] = peer
        peer.start()

    def run(self):
        if not self.running:
            self.logger.info(u"Starting")
            self.observer.start()
            self.listener.start()
            self.running = True

        while self.running:
            self._connect_peers()
            self._process_messages()
            time.sleep(1)

    def stop(self):
        if self.running:
            self.running = False
            self.logger.info(u"Stopping client")
            [peer.stop() for peer in self.peers.values()]
            self.observer.stop()
            self.observer.join()
            self.listener.stop()
            self.listener.join()
            self.join()

    def connect_peer(self, address):
        self.connections_pending.append({"next_run": time.time(), "address": address, "attempts": 0})

    def _connect_peer(self, pending):
        try:
            peer = Peer.create_by_connecting(pending['address'], self.id, self.ring, self.recevied_messages,
                                             self.logger)
            self._add_peer(peer)
        except socket_error as e:
            if e.errno != errno.ECONNREFUSED:
                raise
            pending["attempts"] += 1
            pending["next_run"] = time.time() + pending["attempts"]**2
            self.connections_pending.append(pending)

            self.logger.info(u"Failed to connect to %s after %s attempts. Retrying",
                         pending["address"], pending["attempts"])

    def _connect_peers(self):
        still_pending = []
        should_run = []
        for pending in self.connections_pending:
            if pending["next_run"] < time.time():
                should_run.append(pending)
            else:
                still_pending.append(pending)
        self.connections_pending = still_pending
        for pending in should_run:
            t = threading.Thread(target=self._connect_peer, args=(pending,))
            t.daemon = True
            t.start()

    def _process_messages(self):
        while 1:
            try:
                peer_id, evt = self.recevied_messages.get_nowait()
                # self.logger.warn(u"Received evt %s from %s", evt, peer_id)
                if evt.sync_point in self.sync_points:
                    self.sync_points[evt.sync_point].on_event(evt, peer_id)
                else:
                    self.logger.warn(u"Received unknown sync point from %s: %s", peer_id, evt)
            except Queue.Empty:
                break

    def _send(self, msg, recipient=None):
        if recipient is None:
            [peer.send(msg) for peer in self.peers.values()]
        else:
            self.peers[recipient].send(msg)

    def create_sync_point(self, id, mount_path):
        if id in self.sync_points:
            if self.sync_points[id].mount_path != mount_path:
                self.logger.warn(u"SyncPoint already exists for id %s with mount %s, cannot create at mount %s",
                             id, self.sync_points[id].mount_path, mount_path)
                raise Exception(u"SyncPoint already exists")
            else:
                self.logger.warn(u"SyncPoint already exists for id %s", id)
                raise Exception(u"SyncPoint already exists")
        else:
            sync_point = SyncPoint(id, mount_path, self._send, self.logger)
            self.sync_points[sync_point.id] = sync_point
            sync_point.start(self.observer)
            return self.sync_points[sync_point.id]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format=u'%(message)s',
                        # format=u'%(asctime)s - %(message)s',
                        # datefmt=None)
                        datefmt=u'%Y-%m-%d %H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    clients = []
    for n in range(2):
        client = Client(str(n), "ring1")
        client.start()
        client.create_sync_point("home", "client%s" % n)
        clients.append(client)

    for client in clients:
        client.connect_peer(("localhost", 5000 + int(random.choice([c for c in clients if c != client]).id)))

    print u"Replika started!"
    try:
        while 1:
            time.sleep(1)
    except KeyboardInterrupt:
        print u"Stopping Replika!"
        [client.stop() for client in clients]
    print u"Replika stopped!"
