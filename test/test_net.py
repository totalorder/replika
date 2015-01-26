# encoding: utf-8
import Queue
import socket
from mock import Mock, ANY, patch, call
import async
import net
import signals
import util
import errno
from socket import error as socket_error


def get_spy_processor():
    processor = signals.Processor(async=True)
    return Mock(spec=signals.Processor, wraps=processor)


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
        fake_socket.recv.return_value = "Hello bytes!"
        client = self.network.connect(fake_socket)
        select_return_values = [([fake_socket], [], []), ([], [], [])]
        with patch('select.select', lambda x, y, z, *args: select_return_values.pop(0)):
            self.network.step_until_done()
        assert client.recv() == "Hello bytes!"

    def test_send_data(self):
        fake_socket = self.socket_mock()
        fake_socket_send_return_values = [5, 5, 2, 0, 0]
        fake_socket.send.side_effect = lambda x: fake_socket_send_return_values.pop(0)

        client = self.network.connect(fake_socket)
        client.send("Hello bytes!")
        select_return_values = [([], [fake_socket], []), ([], [fake_socket], []),
                                ([], [fake_socket], []), ([], [fake_socket], []),
                                ([], [fake_socket], []), ([], [], [])]
        with patch('select.select', lambda x, y, z, *args: select_return_values.pop(0)):
            self.network.step_until_done()
        assert fake_socket.send.call_args_list == [call("Hello bytes!"), call(" bytes!"), call("s!")]

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
        receiving_accepted_clients = Queue.Queue()
        self.receiving_processor.register(net.Network.CLIENT_ACCEPTED, receiving_accepted_clients)

        self.receiving_network.listen(8000, async=True)
        sending_client = self.sending_network.connect(("127.0.0.1", 8000))
        sending_client.send("Hello bytes!")

        self.loop.run_until_done()
        self.receiving_processor.signal.assert_called_with(net.Network.CLIENT_ACCEPTED, ANY)

        signal_code, receiving_client = receiving_accepted_clients.get_nowait()
        assert signal_code == net.Network.CLIENT_ACCEPTED
        assert receiving_client.recv() == "Hello bytes!"

        receiving_client.send("Hello! I got your message!")
        self.loop.run_until_done()
        assert sending_client.recv() == "Hello! I got your message!"