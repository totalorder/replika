# encoding: utf-8
import async


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
            client.id = message
        else:
            message = yield from client.recvmessage()
            client.id = message
            client.sendmessage(bytes(self.id, 'utf-8'))

        if client.id in self.peers:
            if not client.is_outgoing:
                if client.id > self.id:
                    self.network.disconnect_client(self.peers[client.id])
                    self.peers[client.id] = client
                else:
                    self.network.disconnect_client(client)
            else:
                if self.id > client.id:
                    self.network.disconnect_client(self.peers[client.id])
                    self.peers[client.id] = client
                else:
                    self.network.disconnect_client(client)
        else:
            self.peers[client.id] = client
        return client

    @async.task
    def add_peer(self, address):
        client = yield from self.network.connect(address)
        peer = yield from self._accept_client(client)
        return peer

    @async.task
    def listen(self):
        yield from self.network.listen(self.port)
        return

