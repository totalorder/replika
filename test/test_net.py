# encoding: utf-8
import queue
import socket
from unittest.mock import Mock, ANY, patch, call
import async
import net
from test.testutils import get_spy_processor
import util
import errno
from socket import error as socket_error


class TestNetwork:
    def setup_method(self, method):
        self.select_patcher = patch('select.select', lambda x, y, z, *args: ([], [], []))
        self.select_mock = self.select_patcher.start()

        self.socket_patcher = patch('socket.socket')
        self.socket_mock = self.socket_patcher.start()

        self.processor = get_spy_processor()
        self.processor.run()
        self.network = net.Network(self.processor, async=True)
        self.sock = Mock(spec=socket.socket)()
        self.network.run()

    def teardown_method(self, method):
        self.network.stop()
        self.processor.stop()
        self.socket_patcher.stop()
        self.select_patcher.stop()

    def test_on_client_accepted_signal_sent(self):
        self.network.on_connection_accepted(net.Listener.CONNECTION_ACCEPTED, (self.sock, ("127.0.0.1", 8000)))
        self.processor.signal.assert_called_once_with(net.Network.CLIENT_ACCEPTED, ANY)

    def test_on_connection_accepted_signal_sent(self):
        self.processor.signal(net.Listener.CONNECTION_ACCEPTED, (self.sock, ("127.0.0.1", 8000)))
        self.network.step_until_done()
        self.processor.step_until_done()
        self.processor.signal.assert_called_with(net.Network.CLIENT_ACCEPTED, ANY)

    def test_recv_data(self):
        fake_socket = self.socket_mock()
        fake_socket.recv.return_value = b"Hello bytes!"
        client = self.network.connect(fake_socket)
        select_return_values = [([fake_socket], [], []), ([], [], [])]
        with patch('select.select', lambda x, y, z, *args: select_return_values.pop(0)):
            self.network.step_until_done()
        assert client.recv() == b"Hello bytes!"

    def test_send_data(self):
        fake_socket = self.socket_mock()
        fake_socket_send_return_values = [5, 5, 2, 0, 0]
        fake_socket.send.side_effect = lambda x: fake_socket_send_return_values.pop(0)

        client = self.network.connect(fake_socket)
        client.send(b"Hello bytes!")
        select_return_values = [([], [fake_socket], []), ([], [fake_socket], []),
                                ([], [fake_socket], []), ([], [fake_socket], []),
                                ([], [fake_socket], []), ([], [], [])]
        with patch('select.select', lambda x, y, z, *args: select_return_values.pop(0)):
            self.network.step_until_done()
        assert fake_socket.send.call_args_list == [call(b"Hello bytes!"), call(b" bytes!"), call(b"s!")]

    def test_listen(self):
        self.socket_mock.return_value = self.sock
        def raise_eagain():
            error = socket_error()
            error.errno = errno.EAGAIN
            raise error
        accept_side_effects = [lambda: ("127.0.0.1", 8001), raise_eagain]
        self.sock.accept.side_effect = lambda: accept_side_effects.pop(0)()
        self.network.listen(8000)
        self.network.step_until_done()
        assert self.sock.accept.call_count == 2
        self.processor.signal.assert_called_with(net.Listener.CONNECTION_ACCEPTED, ANY)

    def test_do_connect(self):
        loop = async.Loop()
        loop.add_runner(self.processor)
        loop.add_runner(self.network)

        self.processor.signal(net.Network.DO_CONNECT, ("127.0.0.1", 8000))
        loop.run_until_done()

        self.processor.signal.assert_called_with(net.Network.CLIENT_ACCEPTED, ANY)

    def test_failed_connect(self):
        fake_socket = self.socket_mock()
        def raise_refused(address):
            error = socket_error()
            error.errno = errno.ECONNREFUSED
            raise error
        fake_socket.connect.side_effect = raise_refused

        loop = async.Loop()
        loop.add_runner(self.processor)
        loop.add_runner(self.network)
        self.processor.signal(net.Network.DO_CONNECT, ("127.0.0.1", 8000))
        loop.run_until_done()
        self.processor.signal.assert_called_with(net.Network.FAILED_DO_CONNECT, ("127.0.0.1", 8000))


class TestListener:
    def setup_method(self, method):
        self.socket_patcher = patch('socket.socket')
        self.socket_mock = self.socket_patcher.start()

        self.processor = get_spy_processor()
        self.processor.run()
        self.sock = Mock(spec=socket.socket)

        self.remote_address = ("80.0.0.1", 1234)
        self.socket_mock().accept.return_value = (self.sock, self.remote_address)

        self.listener = net.Listener(8000, self.processor, util.HierarchyLogger(lambda: ""), async=True)
        self.listener.run()

    def teardown_method(self, method):
        self.processor.stop()
        self.listener.stop()
        self.socket_patcher.stop()

    def test_accept_sends_signal(self):
        self.listener.step()
        self.processor.signal.assert_called_once_with(net.Listener.CONNECTION_ACCEPTED,
                                                      (self.sock, self.remote_address))


class TestClient:
    def setup_method(self, method):
        self.sender = net.Client(("127.0.0.1", 8000), True)
        self.receiver = net.Client(("127.0.0.1", 8001), False)
        self.sender.outgoing = self.receiver.incoming

    def test_recvbytes(self):
        [self.receiver.incoming.put(bytes(c, 'utf-8')) for c in "Hello by"]
        self.receiver.incoming.put(b"tes!")
        assert next(self.receiver.recvbytes(5)) == b"Hello"
        assert next(self.receiver.recvbytes(1)) == b" "
        assert next(self.receiver.recvbytes(3)) == b"byt"
        assert next(self.receiver.recvbytes(3)) == b"es!"

    def test_transfer_data(self):
        self.sender.senddata("I?", 123456, True)
        assert self.receiver.recvdata("I?") == (123456, True)

    def test_transfer_message(self):
        self.sender.sendmessage(b"Hello bytes!")
        assert next(self.receiver.recvmessage()) == b"Hello bytes!"

    def test_recvbytes_async(self):
        [self.receiver.incoming.put(bytes(c, 'utf-8')) for c in "Hello by"]
        self.receiver.incoming.put(b"tes!")
        async_bytes = self.receiver.recvbytes(12, async=True)
        assert list(async_bytes).pop() == b"Hello bytes!"

    def test_transfer_message_async(self):
        async_message = self.receiver.recvmessage(async=True)
        assert next(async_message) == None
        self.sender.sendmessage(b"Hello bytes!")
        assert next(async_message) == b"Hello bytes!"

class TestIntegration:
    def setup_method(self, method):
        self.loop = async.Loop()
        self.sending_processor = get_spy_processor()
        self.sending_processor.run()
        self.loop.add_runner(self.sending_processor)

        self.sending_network = net.Network(self.sending_processor, async=True)
        self.sending_network.run()
        self.loop.add_runner(self.sending_network)

        self.receiving_processor = get_spy_processor()
        self.receiving_processor.run()
        self.loop.add_runner(self.receiving_processor)

        self.receiving_network = net.Network(self.receiving_processor, async=True)
        self.receiving_network.run()
        self.loop.add_runner(self.receiving_network)

    def teardown_method(self, method):
        self.receiving_network.stop()
        self.receiving_processor.stop()

        self.sending_network.stop()
        self.sending_processor.stop()

    def test_establish_connection(self):
        self.receiving_network.listen(8000, async=True)
        self.sending_network.connect(("127.0.0.1", 8000))
        self.loop.run_until_done()
        self.receiving_processor.signal.assert_called_with(net.Network.CLIENT_ACCEPTED, ANY)

    def test_send_and_receive_data(self):
        receiving_accepted_clients = queue.Queue()
        self.receiving_processor.register(net.Network.CLIENT_ACCEPTED, receiving_accepted_clients)

        self.receiving_network.listen(8000, async=True)
        sending_client = self.sending_network.connect(("127.0.0.1", 8000))
        sending_client.send(b"Hello bytes!")

        self.loop.run_until_done()
        self.receiving_processor.signal.assert_called_with(net.Network.CLIENT_ACCEPTED, ANY)

        signal_code, receiving_client = receiving_accepted_clients.get_nowait()
        assert signal_code == net.Network.CLIENT_ACCEPTED
        assert receiving_client.recv() == b"Hello bytes!"

        receiving_client.send(b"Hello! I got your message!")
        self.loop.run_until_done()
        assert sending_client.recv() == b"Hello! I got your message!"