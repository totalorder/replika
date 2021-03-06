# encoding: utf-8
import asyncio
from py2py import async
from py2py import net


class FileClient(net.Client):
    def __init__(self, id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id = id

    def __repr__(self):
        return "%s(%s, %s, %s)" % (self.__class__.__name__,
                                   self.id,
                                   self.address,
                                   "out" if self.is_outgoing else "in")

    @staticmethod
    def from_client(id, client):
        return FileClient(id, client.address, client.reader, client.writer,
                          client.is_outgoing)

    def sendfile(self, file, metadata=None):
        return super().sendfile(file, metadata)

    def recvfile(self):
        return super().recvfile()


class Peer(FileClient):
    def __init__(self, *args, **kwargs):
        self.file_client = None
        super().__init__(*args, **kwargs)

    @staticmethod
    def from_client(id, client):
        return Peer(id, client.address, client.reader, client.writer,
                    client.is_outgoing)

    def set_file_client(self, client):
        self.file_client = client

    def sendfile(self, file, metadata=None):
        return self.file_client.sendfile(file, metadata)

    def recvfile(self):
        return self.file_client.recvfile()


class Overlay(async.FlightControl):
    class ConnectionType(object):
        PEER = 0
        FILE_CLIENT = 1

    def __init__(self, id, port, network, peer_accepted_cb):
        super().__init__()
        self.network = network
        self.peers = {}
        self.file_clients = {}
        self.unbound_file_clients = {}
        self.port = port
        self.id = id
        self._peer_accepted_cb = peer_accepted_cb
        self.network.set_client_accepted_cb(self.client_accepted_cb)
        self.retry_wait = 5
        self.reverse_direction_client_futs = {}
        self.reverse_direction_file_client_futs = {}

    def client_accepted_cb(self, client):
        accept_client_fut = self._accept_client(client)
        print(self.id, "add_done_callback")
        accept_client_fut.add_done_callback(self._client_accepted_cb)

    def _client_accepted_cb(self, accepted_client_fut):
        peer, run_callback = accepted_client_fut.result()
        if run_callback:
            print(self.id, "run_callback")
            self._peer_accepted_cb(peer)
        else:
            print(self.id, "don't run callback")

    @async.task
    def _negotiate_client_info(self, client, file_client):
        if client.is_outgoing:
            connection_type = self.ConnectionType.FILE_CLIENT if file_client \
                else self.ConnectionType.PEER
            client.sendmessage(bytes(self.id, 'utf-8'))
            client.senddata('Hb', self.port, connection_type)
            message = yield from client.recvmessage()
            remote_id = str(message, encoding='utf-8')
        else:
            message = yield from client.recvmessage()
            remote_id = str(message, encoding='utf-8')
            remote_port, connection_type = (yield from client.recvdata('Hb'))
            client.address = (client.address[0], remote_port)
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

    def _unbind_file_client(self, peer, is_file_client):
        if is_file_client:
            del self.file_clients[peer.id]
            if peer.id in self.unbound_file_clients:
                del self.unbound_file_clients[peer.id]
        else:
            if peer.id in self.peers:
                self.unbound_file_clients[peer.id] = peer.file_client

    def _create_peer(self, remote_id, client, connection_type):
        if connection_type == self.ConnectionType.PEER:
            is_file_client = False
        elif connection_type == self.ConnectionType.FILE_CLIENT:
            is_file_client = True
        else:
            raise Exception("Invalid connection type: {}".format(
                connection_type))
        if is_file_client:
            peer = FileClient.from_client(remote_id, client)
            existing_peers = self.file_clients
            reverse_direction_futs = self.reverse_direction_client_futs
        else:
            peer = Peer.from_client(remote_id, client)
            existing_peers = self.peers
            reverse_direction_futs = self.reverse_direction_file_client_futs
        return peer, is_file_client, existing_peers, reverse_direction_futs

    def _bind_reversed_peer(self, existing_peers, fc, is_file_client, peer,
                            reverse_direction_futs):
        print(self.id, fc, "received reverse: ", peer.id, "(accept)",
              peer.reader._transport._sock_fd)
        existing_peers[peer.id] = peer
        self._bind_file_client(peer, is_file_client)
        reverse_direction_futs[peer.id].set_result(peer)
        del reverse_direction_futs[peer.id]

    def _handle_new_peer(self, peer, is_file_client, fc, reverse_direction_futs,
                         existing_peers, run_callback):
        if not peer.is_outgoing:
            if peer.id > self.id:
                print(self.id, fc, "received > incoming:", peer.id,
                      "(accept)", peer.reader._transport._sock_fd)
                existing_peers[peer.id] = peer
                self._bind_file_client(peer, is_file_client)
                resulting_peer = peer
            else:
                print(self.id, fc, "received < incoming:", peer.id,
                      "(close + connect)", peer.reader._transport._sock_fd)
                self.network.disconnect_client(peer)
                resulting_peer = yield from self.add_peer(peer.address)
        else:
            if self.id > peer.id:
                print(self.id, fc, "received > outgoing:", peer.id,
                      "(connect)", peer.reader._transport._sock_fd,
                      "callback:", run_callback)
                existing_peers[peer.id] = peer
                self._bind_file_client(peer, is_file_client)
                resulting_peer = peer
            else:
                print(self.id, fc, "received < outgoing:", peer.id,
                      "(nothing + yield reverse)",
                      peer.reader._transport._sock_fd)
                run_callback = False
                self.network.disconnect_client(peer)
                reverse_direction_fut = asyncio.Future()
                reverse_direction_futs[peer.id] = reverse_direction_fut
                resulting_peer = yield from reverse_direction_fut
        return resulting_peer, run_callback

    def _disconnect_existing_peer(self, existing_peers, fc, peer):
        print(self.id, fc, "existing",
              "outgoing" if peer.is_outgoing else "incoming",
              "(close)", peer.reader._transport._sock_fd)
        self.network.disconnect_client(peer)
        resulting_peer = existing_peers[peer.id]
        return resulting_peer

    @async.task
    def _accept_peer(self, remote_id, client, connection_type):
        peer, is_file_client, existing_peers, reverse_direction_futs = \
            self._create_peer(remote_id, client, connection_type)

        fc = "fileclient" if is_file_client else "peer"
        run_callback = not is_file_client
        if not peer.is_outgoing and peer.id in reverse_direction_futs and \
                peer.id not in existing_peers:
            self._bind_reversed_peer(existing_peers, fc, is_file_client, peer,
                                     reverse_direction_futs)
            resulting_peer = peer
            run_callback = False
        elif peer.id not in existing_peers:
            resulting_peer, run_callback = yield from self._handle_new_peer(
                peer, is_file_client, fc, reverse_direction_futs,
                existing_peers, run_callback)
        else:
            resulting_peer = self._disconnect_existing_peer(existing_peers, fc,
                                                            peer)
            run_callback = False
        if peer.address in self.flight:
            print(self.id, "connections_in_progress")
            run_callback = False
        print(self.id, fc, "run_callback", run_callback)
        return resulting_peer, run_callback

    @async.task
    def _accept_client(self, client, file_client=False):
        client, remote_id, connection_type = \
            yield from self._negotiate_client_info(client, file_client)
        return (
            yield from self._accept_peer(remote_id, client, connection_type))

    @async.task
    def _connect_with_retries(self, address, num_connections=2):
        num_retries = 3
        conns = [{} for _ in range(num_connections)]
        for conn in conns:
            conn['retries'] = 0
            conn['client'] = None

        while [1 for conn in conns if conn['client'] is None
                and conn['retries'] < num_retries]:
            for conn in conns:
                if conn['client'] is None:
                    client_fut = self.network.connect(address)
                    try:
                        conn['client'] = yield from client_fut
                    except ConnectionRefusedError:
                        conn['retries'] += 1
                        yield from asyncio.sleep(self.retry_wait)
        for conn in conns:
            if conn['client'] is None:
                raise ConnectionRefusedError(
                    'Could not connect to address {} after {} attempts!'.format(
                        address, num_retries))

        return [conn['client'] for conn in conns]

    @async.task
    def add_peer(self, address):
        if address not in self.flight:
            with self.flight_control(address) as plane:
                client, file_client = \
                    yield from self._connect_with_retries(address)

                yield from self._accept_client(file_client, file_client=True)

                peer, _ = yield from self._accept_client(client)
                print(self.id, "i don't care")
                plane.set_result(peer)
            return peer
        else:
            return (yield from self.landed_flight(address))

    @async.task
    def listen(self):
        yield from self.network.listen(self.port)
        return

    def stop(self):
        self.network.stop()

    def disconnect_peer(self, peer):
        if peer.file_client:
            self.network.disconnect_client(peer.file_client)
        self.network.disconnect_client(peer)
        del self.peers[peer.id]
