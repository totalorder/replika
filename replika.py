# encoding: utf-8
import asyncio
import functools
import os
import queue
import shutil
import struct
import sys
import threading
import time
import logging
import random
from watchdog.observers import Observer
from event import EventType
from py2py import overlay, net, async
from sync import SyncPoint
from util import HierarchyLogger
from os.path import join as jn, exists, dirname
from os import makedirs


class Peer:
    class MessageReceivedType:
        MESSAGE = 0
        FILE = 1

    def __init__(self, ring, overlay_peer, incoming_messages):
        self.ring = ring
        self.incoming_messages = incoming_messages
        self.overlay_peer = overlay_peer

    @asyncio.coroutine
    def _recvmessage(self):
        message = yield from self.overlay_peer.recvmessage()
        self.incoming_messages.put((self.MessageReceivedType.MESSAGE, self.overlay_peer.id, message))
        asyncio.async(self._recvmessage)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.ident)

    @property
    def ident(self):
        return "{}-{}".format(self.ring, self.overlay_peer.id)

    def send_event(self, evt):
        msg = EventType.serialize(evt)
        print("send_event")
        self.overlay_peer.sendmessage(msg)

    def sendfile(self, sync_point, mount_path, path):
        full_path = jn(mount_path, path)

        sync_point = struct.pack("!I", len(sync_point)) + sync_point.encode('utf-8')
        path = struct.pack("!I", len(path)) + path.encode('utf-8')
        file_time = struct.pack("I", os.path.getmtime(full_path))
        metadata = sync_point + path + file_time

        file = open(full_path, 'rb')
        self.overlay_peer.sendfile(file, metadata)

    @asyncio.coroutine
    def _recvfile(self):
        file, metadata = yield from self.overlay_peer.recvfile()
        sync_point, pos = self._unpackmessage(metadata, 0)
        path, pos = self._unpackmessage(metadata, pos)
        file_time, _ = self._unpackdata("I", metadata, pos)

        self.incoming_messages.put((self.MessageReceivedType.FILE, self.overlay_peer.id, (file, sync_point, path, file_time)))
        asyncio.async(self._recvfile)

    def _unpackmessage(self, message, pos):
        pos += 4
        num = struct.unpack("!I", message[:pos])[0]
        return message[pos:pos + num], pos + num

    def _unpackdata(self, format, data, pos):
        struct_size = struct.calcsize(format)
        chunk = data[pos:pos + struct_size]
        return struct.unpack("!" + format, chunk), pos + struct_size


class Client(threading.Thread):
    def __init__(self, id, ring, loop=None):
        super(Client, self).__init__()
        self.id = id
        self.ring = ring
        self.peers = {}
        self.sync_points = {}
        self.observer = Observer()
        self.running = False
        self.logger = HierarchyLogger(lambda: "Client %s" % self.id)
        self.overlay = overlay.Overlay(self.id, 5000 + int(self.id), net.Network(), self.accept_peer)
        self.received_messages = queue.Queue()
        self.peers_to_add = queue.Queue()
        self.loop = None

    @property
    def ident(self):
        return "{}-{}".format(self.ring, self.id)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.ident)

    @async.task
    def accept_peer(self, overlay_peer):
        if overlay_peer.is_outgoing:
            overlay_peer.sendstring(self.ring)
            remote_ring = yield from overlay_peer.recvstring()
        else:
            remote_ring = yield from overlay_peer.recvstring()
            overlay_peer.sendstring(self.ring)

        peer = Peer(remote_ring, overlay_peer, self.received_messages)
        self.peers[peer.ident] = peer
        return peer

    def run(self):
        if not self.running:
            self.running = True
            if self.loop is None:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)

            self.logger.info("Starting")
            listen_fut = self.overlay.listen()
            self.loop.run_until_complete(listen_fut)
            self.observer.start()

            self.loop.call_soon(self._process_messages)
            self.loop.call_at(self.loop.time() + 1, self.loop.stop)

            print(self.id, "Running loop!")
            self.loop.run_forever()
            print(self.id, "LOOP DEAD")

    def stop(self):
        if self.running:
            self.running = False
            self.logger.info("Stopping client")
            [peer.stop() for peer in list(self.peers.values())]
            self.observer.stop()
            self.observer.join()
            self.overlay.stop()
            self.join()

    def add_peer(self, address):
        self.peers_to_add.put(address)

    @async.task
    def _add_peer(self, address):
        overlay_peer = yield from self.overlay.add_peer(address)
        peer = yield from self.accept_peer(overlay_peer)
        return peer

    def _get_sync_point_info(self):
        return {sync_point.id: sync_point.mount_path for sync_point in list(self.sync_points.values())}

    def _signal(self, sync_point, source_path, event_type):
        self.sync_points[sync_point].signal(source_path, event_type)

    def _process_messages(self):
        while 1:
            try:
                address = self.peers_to_add.get_nowait()
                self._add_peer(address)
            except queue.Empty:
                break

        while 1:
            print("process message")
            try:
                received_message_type, peer_id, message = self.received_messages.get_nowait()
                if received_message_type == Peer.MessageReceivedType.MESSAGE:
                    if message.sync_point in self.sync_points:
                        self.sync_points[message.sync_point].on_event(message, peer_id)
                    else:
                        self.logger.warn("Received unknown sync point from %s: %s", peer_id, message)
                elif received_message_type == Peer.MessageReceivedType.FILE:
                    file, sync_point, path, file_time = message
                    self._receive_file(file, sync_point, path, file_time)
            except queue.Empty:
                break
        self.loop.call_at(self.loop.time() + 0.1, self._process_messages)

    def _receive_file(self, file, sync_point, file_path, file_modified_date):
        full_path = file.name
        target_path = jn(self._get_sync_point_info()[sync_point], file_path)
        target_dir = dirname(full_path)
        if not exists(target_dir):
            self._signal(sync_point, file_path, EventType.CREATE)
            makedirs(target_dir)
        os.utime(full_path, (os.path.getatime(full_path), file_modified_date))
        if exists(target_path):
            self._signal(sync_point, file_path, EventType.MODIFY)
        else:
            self._signal(sync_point, file_path, EventType.MODIFY)
        shutil.copy2(full_path, target_path)

    def _send_event(self, evt, recipient=None):
        if recipient is None:
            [peer.send_event(evt) for peer in list(self.peers.values())]
        else:
            self.peers[recipient].send_event(evt)

    def _send_file(self, sync_point, path, recipient):
        self.peers[recipient].sendfile(sync_point, self.sync_points[sync_point].mount_path, path)

    def create_sync_point(self, id, mount_path):
        if id in self.sync_points:
            if self.sync_points[id].mount_path != mount_path:
                self.logger.warn("SyncPoint already exists for id %s with mount %s, cannot create at mount %s",
                                 id, self.sync_points[id].mount_path, mount_path)
                raise Exception("SyncPoint already exists")
            else:
                self.logger.warn("SyncPoint already exists for id %s", id)
                raise Exception("SyncPoint already exists")
        else:
            sync_point = SyncPoint(id, mount_path, self._send_event, self._send_file, self.logger)
            self.sync_points[sync_point.id] = sync_point
            sync_point.start(self.observer)
            return self.sync_points[sync_point.id]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(message)s',
                        # format=u'%(asctime)s - %(message)s',
                        # datefmt=None)
                        datefmt='%Y-%m-%d %H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    clients = []
    for n in range(2):
        client = Client(str(n), "ring1")
        client.start()
        client.create_sync_point("home", "client%s" % n)
        clients.append(client)

    for client in clients:
        client.add_peer(("localhost", 5000 + int(random.choice([c for c in clients if c != client]).id)))

    print("Replika started!")
    try:
        while 1:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping Replika!")
        [client.stop() for client in clients]
    print("Replika stopped!")
