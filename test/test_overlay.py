# encoding: utf-8
import asyncio
import async
import net
import overlay
from unittest.mock import Mock, ANY, patch, call
from test import testutils
from test.testutils import get_spy_processor
from test.testutils import create_future_result as cfr


class TestOverlay:
    def setup_method(self, method):
        self.network = Mock(spec=net.Network)
        self.processor = get_spy_processor()

        self.overlay_1 = overlay.Overlay("1", 8001, self.processor, self.network)

        self.outgoing_client_1 = Mock(spec=net.Client)()
        self.outgoing_client_1.is_outgoing = True
        self.outgoing_client_1.recvmessage.return_value = cfr("2")

        self.incoming_client_1 = Mock(spec=net.Client)()
        self.incoming_client_1.is_outgoing = False
        self.incoming_client_1.recvmessage.return_value = cfr("2")

        self.overlay_2 = overlay.Overlay("2", 8002, self.processor, self.network)
        self.outgoing_client_2 = Mock(spec=net.Client)()
        self.outgoing_client_2.is_outgoing = True
        self.outgoing_client_2.recvmessage.return_value = cfr("1")

        self.incoming_client_2 = Mock(spec=net.Client)()
        self.incoming_client_2.is_outgoing = False
        self.incoming_client_2.recvmessage.return_value = cfr("1")

    def test_accept_client(self):
        self.overlay_1.accept_client(self.incoming_client_1).run()
        assert self.overlay_1.peers["2"] == self.incoming_client_1

    def test_accept_lower_incoming_then_outgoing(self):
        self.overlay_1.accept_client(self.incoming_client_1).run()
        self.overlay_1.accept_client(self.outgoing_client_1).run()
        assert self.overlay_1.peers["2"] == self.incoming_client_1

    def test_accept_lower_outgoing_then_incoming(self):
        self.overlay_1.accept_client(self.outgoing_client_1).run()
        self.overlay_1.accept_client(self.incoming_client_1).run()
        assert self.overlay_1.peers["2"] == self.incoming_client_1

    def test_accept_higher_incoming_then_outgoing(self):
        self.overlay_2.accept_client(self.incoming_client_2).run()
        self.overlay_2.accept_client(self.outgoing_client_2).run()
        assert self.overlay_2.peers["1"] == self.outgoing_client_2

    def test_accept_higher_outgoing_then_incoming(self):
        self.overlay_2.accept_client(self.outgoing_client_2).run()
        self.overlay_2.accept_client(self.incoming_client_2).run()
        assert self.overlay_2.peers["1"] == self.outgoing_client_2


class TestIntegration:
    def setup_method(self, method):
        self.loopio = testutils.TestLoop()
        asyncio.set_event_loop(self.loopio)

        self.loop = async.Loop()
        self.processor_1 = get_spy_processor()
        self.loop.add_runner(self.processor_1)

        self.network_1 = net.Network(self.processor_1, async=True)
        self.network_1.step_until_done()

        self.overlay_1 = overlay.Overlay("1", 8001, self.processor_1, self.network_1, async=True)
        self.overlay_1.setup()
        self.loop.add_runner(self.overlay_1)

        self.processor_2 = get_spy_processor()
        self.loop.add_runner(self.processor_2)

        self.network_2 = net.Network(self.processor_2, async=True)
        self.network_2.step_until_done()

        self.overlay_2 = overlay.Overlay("2", 8002, self.processor_2, self.network_2, async=True)
        self.overlay_2.setup()
        self.loop.add_runner(self.overlay_2)

    def teardown_method(self, method):
        self.overlay_2.stop()
        self.network_2.stop()
        self.processor_2.stop()

        self.overlay_1.stop()
        self.network_1.stop()
        self.processor_1.stop()
        self.loopio.close()

    def test_connect(self):
        self.overlay_1.add_peer(("127.0.0.1", 8002))
        self.loopio._run_once()

        run_asyncio_until_fut = self.loop.run_asyncio_until(
            lambda: self.overlay_1.peers.keys() and self.overlay_2.peers.keys()
        )
        self.loopio.run_until_complete(run_asyncio_until_fut)

        assert list(self.overlay_2.peers.keys()) == [b"1"]
        assert list(self.overlay_1.peers.keys()) == [b"2"]