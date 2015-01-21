# encoding: utf-8
import socket
from mock import Mock, ANY, patch, call
import net
import signals


class TestNetwork:
    def setup_method(self, method):
        self.select_patcher = patch('select.select', lambda x, y, z, *args: ([], [], []))
        self.select_mock = self.select_patcher.start()

        self.socket_patcher = patch('socket.socket')
        self.socket_mock = self.socket_patcher.start()

        processor = signals.Processor(async=True)
        processor.setup()
        self.processor = Mock(spec=signals.Processor, wraps=processor)
        self.network = net.Network(self.processor, async=True)
        self.sock = Mock(spec=socket.socket)
        self.network.setup()

    def teardown_method(self, method):
        self.network.teardown()
        self.processor.teardown()
        self.socket_patcher.stop()
        self.select_patcher.stop()

    def test_on_client_accepted_signal_sent(self):
        self.network.on_connection_accepted(net.Listener.CONNECTION_ACCEPTED, self.sock)
        self.processor.signal.assert_called_once_with(net.Network.CLIENT_ACCEPTED, ANY)

    def test_on_connection_accepted_signal_sent(self):
        self.processor.signal(net.Listener.CONNECTION_ACCEPTED, self.sock)
        self.network.step_until_done()
        self.processor.step_until_done()
        self.processor.signal.assert_called_with(net.Network.CLIENT_ACCEPTED, ANY)

    def test_recv_data(self):
        fake_socket = self.socket_mock()
        fake_socket.read.return_value = "Hello bytes!"
        client = self.network.connect(fake_socket)
        select_return_values = [([fake_socket], [], []), ([], [], [])]
        with patch('select.select', lambda x, y, z, *args: select_return_values.pop(0)):
            self.network.step_until_done()
        assert client.incoming.get_nowait() == "Hello bytes!"

    def test_send_data(self):
        fake_socket = self.socket_mock()
        fake_socket_send_return_values = [5, 5, 2, 0, 0]
        fake_socket.send.side_effect = lambda x: fake_socket_send_return_values.pop(0)

        client = self.network.connect(fake_socket)
        client.outgoing.put("Hello bytes!")
        select_return_values = [([], [fake_socket], []), ([], [fake_socket], []),
                                ([], [fake_socket], []), ([], [fake_socket], []),
                                ([], [fake_socket], []), ([], [], [])]
        with patch('select.select', lambda x, y, z, *args: select_return_values.pop(0)):
            self.network.step_until_done()
        assert fake_socket.send.call_args_list == [call("Hello bytes!"), call(" bytes!"), call("s!")]
