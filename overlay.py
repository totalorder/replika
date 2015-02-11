# encoding: utf-8
import Queue
import async
import net


class Overlay(async.EventThread):
    def __init__(self, id, port, processor, network, *args, **kwargs):
        super(Overlay, self).__init__(*args, **kwargs)
        self.processor = processor
        self.network = network
        self.peers = {}
        self.port = port
        self.accepted_clients = Queue.Queue()
        self.failed_connects = Queue.Queue()
        self.id = id

    def setup(self):
        self.processor.register(net.Network.CLIENT_ACCEPTED, self.accepted_clients)
        self.processor.register(net.Network.FAILED_DO_CONNECT, self.failed_connects)
        self.network.listen(self.port, async=True)

    def step(self):
        accepted_client = False
        try:
            print "Step: ", self.id
            signal_code, client = self.accepted_clients.get_nowait()
            print "Step: signal_code", self.id, signal_code
            if self.async:
                async_accept_client = self.accept_client(client, async=True)
                self.add_async(async_accept_client)
            else:
                self.accept_client(client)
            accepted_client = True
        except Queue.Empty:
            pass

        if self.execute_asyncs() and not accepted_client:
            return True

    def accept_client(self, client, async=False):
        def async_accept_client():
            if client.is_outgoing:
                client.sendmessage(self.id)
                print self.id, client.id, "out?"

                if async:
                    for async_result in client.recvmessage(async=True):
                        if async_result is not None:
                            client.id = async_result
                            print self.id, client.id, "out!"
                        else:
                            yield
                else:
                    client.id = client.recvmessage()

            else:
                if async:
                    for async_result in client.recvmessage(async=True):
                        if async_result is not None:
                            client.id = async_result
                            print self.id, client.id, "in"
                        else:
                            yield
                else:
                    client.id = client.recvmessage()
                client.sendmessage(self.id)

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

        if async:
            return async_accept_client()
        else:
            list(async_accept_client())


    def teardown(self):
        self.processor.unregister(net.Network.FAILED_DO_CONNECT, self.failed_connects)
        self.processor.unregister(net.Network.CLIENT_ACCEPTED, self.accepted_clients)

    def add_peer(self, address):
        self.processor.signal(net.Network.DO_CONNECT, address)

