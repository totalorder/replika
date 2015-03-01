# encoding: utf-8
from py2py import async
from py2py import net


class Peer(net.Client):
    def __init__(self, id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id = id

    @classmethod
    def from_client(cls, id, client):
        return Peer(id, client.address, client.reader, client.writer, client.is_outgoing)


class Overlay(object):
    def __init__(self, id, port, network, peer_accepted_cb):
        self.network = network
        self.peers = {}
        self.port = port
        self.id = id
        self._peer_accepted_cb = peer_accepted_cb
        self.network.set_client_accepted_cb(self.client_accepted_cb)

    def client_accepted_cb(self, client):
        peer_fut = self._accept_client(client)
        peer_fut.add_done_callback(self._peer_accepted_cb)

    @async.task
    def _accept_client(self, client):
        if client.is_outgoing:
            client.sendmessage(bytes(self.id, 'utf-8'))
            message = yield from client.recvmessage()
            remote_id = str(message, encoding='utf-8')
        else:
            message = yield from client.recvmessage()
            remote_id = str(message, encoding='utf-8')
            client.sendmessage(bytes(self.id, 'utf-8'))

        peer = Peer.from_client(remote_id, client)

        if peer.id in self.peers:
            if not peer.is_outgoing:
                if peer.id > self.id:
                    self.network.disconnect_client(self.peers[peer.id])
                    self.peers[peer.id] = peer
                else:
                    self.network.disconnect_client(peer)
            else:
                if self.id > peer.id:
                    self.network.disconnect_client(self.peers[peer.id])
                    self.peers[peer.id] = peer
                else:
                    self.network.disconnect_client(peer)
        else:
            self.peers[peer.id] = peer
        return peer

    @async.task
    def add_peer(self, address):
        client = yield from self.network.connect(address)
        peer = yield from self._accept_client(client)
        return peer

    @async.task
    def listen(self):
        yield from self.network.listen(self.port)
        return

    def stop(self):
        self.network.stop()
