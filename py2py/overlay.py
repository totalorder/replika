# encoding: utf-8
import asyncio
from py2py import async
from py2py import net


class Peer(net.Client):
    def __init__(self, id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id = id
        self.file_client = None

    def set_file_client(self, client):
        self.file_client = client

    @classmethod
    def from_client(cls, id, client):
        return cls(id, client.address, client.reader, client.writer, client.is_outgoing)

    def sendfile(self, path, metadata=None):
        self.file_client.sendfile(path, metadata)

    @async.task
    def recvfile(self):
        yield from self.file_client.recvfile()


class Overlay(object):
    class ConnectionType(object):
        PEER = 0
        FILE_CLIENT = 1

    def __init__(self, id, port, network, peer_accepted_cb):
        self.network = network
        self.peers = {}
        self.file_clients = {}
        self.unbound_file_clients = {}
        self.port = port
        self.id = id
        self._peer_accepted_cb = peer_accepted_cb
        self.network.set_client_accepted_cb(self.client_accepted_cb)
        self.retry_wait = 5

    def client_accepted_cb(self, client):
        peer_fut = self._accept_client(client)
        peer_fut.add_done_callback(self._peer_accepted_cb)

    @async.task
    def _negotiate_client_info(self, client, file_client):
        if client.is_outgoing:
            connection_type = self.ConnectionType.FILE_CLIENT if file_client else self.ConnectionType.PEER
            client.sendmessage(bytes(self.id, 'utf-8'))
            client.senddata('b', connection_type)
            message = yield from client.recvmessage()
            remote_id = str(message, encoding='utf-8')
        else:
            message = yield from client.recvmessage()
            remote_id = str(message, encoding='utf-8')
            connection_type = (yield from client.recvdata('b'))[0]
            client.sendmessage(bytes(self.id, 'utf-8'))
        return client, remote_id, connection_type

    def _bind_file_client(self, peer, is_file_client):
        if is_file_client:
            if peer.id in self.peers:
                self.peers[peer.id].set_file_client(peer)
            else:
                self.unbound_file_clients[peer.id] = peer
        else:
            if peer.id in self.unbound_file_clients:
                peer.set_file_client(self.unbound_file_clients[peer.id])
                del self.unbound_file_clients[peer.id]

    def _accept_peer(self, remote_id, client, connection_type):
        if connection_type == self.ConnectionType.PEER:
            is_file_client = False
        elif connection_type == self.ConnectionType.FILE_CLIENT:
            is_file_client = True
        else:
            raise Exception("Invalid connection type: {}".format(connection_type))

        if is_file_client:
            existing_peers = self.file_clients
        else:
            existing_peers = self.peers

        peer = Peer.from_client(remote_id, client)
        if peer.id in existing_peers:
            if not peer.is_outgoing:
                if peer.id > self.id:
                    self.network.disconnect_client(existing_peers[peer.id])
                    existing_peers[peer.id] = peer
                    self._bind_file_client(peer, is_file_client)
                else:
                    self.network.disconnect_client(peer)
            else:
                if self.id > peer.id:
                    self.network.disconnect_client(existing_peers[peer.id])
                    existing_peers[peer.id] = peer
                    self._bind_file_client(peer, is_file_client)
                else:
                    self.network.disconnect_client(peer)
        else:
            existing_peers[peer.id] = peer
            self._bind_file_client(peer, is_file_client)
        return peer

    @async.task
    def _accept_client(self, client, file_client=False):
        client, remote_id, connection_type = yield from self._negotiate_client_info(client, file_client)
        return self._accept_peer(remote_id, client, connection_type)

    @async.task
    def _connect_with_retries(self, address, num_connections=2):
        conns = [{} for _ in range(num_connections)]
        for conn in conns:
            conn['retries'] = 0
            conn['client'] = None

        while [1 for conn in conns if conn['client'] is None and conn['retries'] < 3]:
            for conn in conns:
                if conn['client'] is None:
                    client_fut = self.network.connect(address)
                    try:
                        conn['client'] = yield from client_fut
                    except ConnectionRefusedError:
                        conn['retries'] += 1
                        yield from asyncio.sleep(self.retry_wait)

        return [conn['client'] for conn in conns]

    @async.task
    def add_peer(self, address):
        client, file_client = yield from self._connect_with_retries(address)

        yield from self._accept_client(file_client, file_client=True)

        peer = yield from self._accept_client(client)
        return peer

    @async.task
    def listen(self):
        yield from self.network.listen(self.port)
        return

    def stop(self):
        self.network.stop()
