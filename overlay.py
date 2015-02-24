# encoding: utf-8
import queue
import async
from async import F, P
import net


class Overlay(async.EventThread):
    def __init__(self, id, port, processor, network, *args, **kwargs):
        super(Overlay, self).__init__(*args, **kwargs)
        self.processor = processor
        self.network = network
        self.peers = {}
        self.port = port
        self.accepted_clients = queue.Queue()
        self.failed_connects = queue.Queue()
        self.id = id

    def setup(self):
        self.processor.register(net.Network.CLIENT_ACCEPTED, self.accepted_clients)
        self.processor.register(net.Network.FAILED_DO_CONNECT, self.failed_connects)
        self.network.listen(self.port, async=True)

    def step(self):
        accepted_client = False
        try:
            print("Step: ", self.id)
            signal_code, client = self.accepted_clients.get_nowait()
            print("Step: signal_code", self.id, signal_code)
            if self.async:
                async_accept_client = self.accept_client(client)
                self.add_async(async_accept_client)
            else:
                self.accept_client(client).run()
            accepted_client = True
        except queue.Empty:
            pass

        if self.execute_asyncs() and not accepted_client:
            return True

    def accept_client(self, client):
        def async_accept_client():
            if client.is_outgoing:
                client.sendmessage(bytes(self.id, 'utf-8'))
                print(self.id, client.id, "out?")

                for async_result in client.recvmessage():
                    if async_result is not F.NOT_AVAILABLE:
                        client.id = async_result
                        print(self.id, client.id, "out!")
                    else:
                        yield

            else:
                for async_result in client.recvmessage():
                    if async_result is not None:
                        client.id = async_result
                        print(self.id, client.id, "in")
                    else:
                        yield
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

        return P(async_accept_client())

    def teardown(self):
        self.processor.unregister(net.Network.FAILED_DO_CONNECT, self.failed_connects)
        self.processor.unregister(net.Network.CLIENT_ACCEPTED, self.accepted_clients)

    def add_peer(self, address):
        self.processor.signal(net.Network.DO_CONNECT, address)
