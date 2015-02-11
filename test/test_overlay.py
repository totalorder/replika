# encoding: utf-8
import async
import net
import overlay
from mock import Mock, ANY, patch, call
from test.testutils import get_spy_processor


class TestOverlay:
    def setup_method(self, method):
        self.network = Mock(spec=net.Network)
        self.processor = get_spy_processor()

        self.overlay_1 = overlay.Overlay("1", 8001, self.processor, self.network)

        self.outgoing_client_1 = Mock(spec=net.Client)()
        self.outgoing_client_1.is_outgoing = True
        self.outgoing_client_1.recvmessage.return_value = "2"

        self.incoming_client_1 = Mock(spec=net.Client)()
        self.incoming_client_1.is_outgoing = False
        self.incoming_client_1.recvmessage.return_value = "2"

        self.overlay_2 = overlay.Overlay("2", 8002, self.processor, self.network)
        self.outgoing_client_2 = Mock(spec=net.Client)()
        self.outgoing_client_2.is_outgoing = True
        self.outgoing_client_2.recvmessage.return_value = "1"

        self.incoming_client_2 = Mock(spec=net.Client)()
        self.incoming_client_2.is_outgoing = False
        self.incoming_client_2.recvmessage.return_value = "1"

    def test_accept_client(self):
        self.overlay_1.accept_client(self.incoming_client_1)
        assert self.overlay_1.peers["2"] == self.incoming_client_1

    def test_accept_lower_incoming_then_outgoing(self):
        self.overlay_1.accept_client(self.incoming_client_1)
        self.overlay_1.accept_client(self.outgoing_client_1)
        assert self.overlay_1.peers["2"] == self.incoming_client_1

    def test_accept_lower_outgoing_then_incoming(self):
        self.overlay_1.accept_client(self.outgoing_client_1)
        self.overlay_1.accept_client(self.incoming_client_1)
        assert self.overlay_1.peers["2"] == self.incoming_client_1

    def test_accept_higher_incoming_then_outgoing(self):
        self.overlay_2.accept_client(self.incoming_client_2)
        self.overlay_2.accept_client(self.outgoing_client_2)
        assert self.overlay_2.peers["1"] == self.outgoing_client_2

    def test_accept_higher_outgoing_then_incoming(self):
        self.overlay_2.accept_client(self.outgoing_client_2)
        self.overlay_2.accept_client(self.incoming_client_2)
        assert self.overlay_2.peers["1"] == self.outgoing_client_2


class TestIntegration:
    def setup_method(self, method):
        self.loop = async.Loop()
        self.processor_1 = get_spy_processor()
        self.processor_1.run()
        self.loop.add_runner(self.processor_1)

        self.network_1 = net.Network(self.processor_1, async=True)
        self.network_1.run()
        self.loop.add_runner(self.network_1)

        self.overlay_1 = overlay.Overlay("1", 8001, self.processor_1, self.network_1, async=True)
        self.overlay_1.run()
        self.loop.add_runner(self.overlay_1)

        self.processor_2 = get_spy_processor()
        self.processor_2.run()
        self.loop.add_runner(self.processor_2)

        self.network_2 = net.Network(self.processor_2, async=True)
        self.network_2.run()
        self.loop.add_runner(self.network_2)

        self.overlay_2 = overlay.Overlay("2", 8002, self.processor_2, self.network_2, async=True)
        self.overlay_2.run()
        self.loop.add_runner(self.overlay_2)

    def teardown_method(self, method):
        self.overlay_2.stop()
        self.network_2.stop()
        self.processor_2.stop()

        self.overlay_1.stop()
        self.network_1.stop()
        self.processor_1.stop()

    def test_connect(self):
        self.overlay_1.add_peer(("127.0.0.1", 8002))
        self.loop.run_until_done()
        assert self.overlay_2.peers.keys() == ["1"]
        assert self.overlay_1.peers.keys() == ["2"]