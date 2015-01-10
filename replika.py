# encoding: utf-8
import Queue
import sys
import threading
import time
import logging
import random
from watchdog.observers import Observer
import errno
from socket import error as socket_error
from network import ClientListener, Peer
from sync import SyncPoint
from util import HierarchyLogger


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
        self.listener = ClientListener(5000 + int(self.id), self.connection_accepted, self.logger)
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
