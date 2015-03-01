# encoding: utf-8
import asyncio
from py2py import net
from py2py import overlay
from unittest.mock import Mock
from test import testutils
from test.testutils import create_future_result as cfr


class TestOverlay:
    def setup_method(self, method):
        self.loop = testutils.TestLoop()
        asyncio.set_event_loop(self.loop)

        self.network = Mock(spec=net.Network)
        self.overlay_1 = overlay.Overlay("1", 8001, self.network, self.peer_accepted_cb)

        self.outgoing_client_1 = Mock(spec=net.Client)()
        self.outgoing_client_1.is_outgoing = True
        self.outgoing_client_1.recvmessage.return_value = cfr(b"2")
        self.outgoing_client_1.address.return_value = ("127.0.0.1", 8002)

        self.incoming_client_1 = Mock(spec=net.Client)()
        self.incoming_client_1.is_outgoing = False
        self.incoming_client_1.recvmessage.return_value = cfr(b"2")
        self.incoming_client_1.address.return_value = ("127.0.0.1", 8002)

        self.overlay_2 = overlay.Overlay("2", 8002, self.network, self.peer_accepted_cb)
        self.outgoing_client_2 = Mock(spec=net.Client)()
        self.outgoing_client_2.is_outgoing = True
        self.outgoing_client_2.recvmessage.return_value = cfr(b"1")
        self.outgoing_client_2.address.return_value = ("127.0.0.1", 8001)

        self.incoming_client_2 = Mock(spec=net.Client)()
        self.incoming_client_2.is_outgoing = False
        self.incoming_client_2.recvmessage.return_value = cfr(b"1")
        self.incoming_client_2.address.return_value = ("127.0.0.1", 8001)

    def teardown_method(self, method):
        self.loop.close()

    def peer_accepted_cb(self, peer):
        self.accepted_peer = peer

    def test_accept_client(self):
        peer_fut = self.overlay_1._accept_client(self.incoming_client_1)
        self.loop.run_until_complete(peer_fut)
        assert peer_fut.result().id == "2"
        assert self.overlay_1.peers["2"].address == self.incoming_client_1.address

    def test_accept_lower_incoming_then_outgoing(self):
        incoming_peer_fut = self.overlay_1._accept_client(self.incoming_client_1)
        outgoing_peer_fut = self.overlay_1._accept_client(self.outgoing_client_1)
        self.loop.run_until_no_events()
        assert incoming_peer_fut.result().id == "2"
        assert outgoing_peer_fut.result().id == "2"
        assert self.overlay_1.peers["2"].address == self.incoming_client_1.address

    def test_accept_lower_outgoing_then_incoming(self):
        outgoing_peer_fut = self.overlay_1._accept_client(self.outgoing_client_1)
        incoming_peer_fut = self.overlay_1._accept_client(self.incoming_client_1)
        self.loop.run_until_no_events()
        assert incoming_peer_fut.result().id == "2"
        assert outgoing_peer_fut.result().id == "2"
        assert self.overlay_1.peers["2"].address == self.incoming_client_1.address

    def test_accept_higher_incoming_then_outgoing(self):
        incoming_peer_fut = self.overlay_2._accept_client(self.incoming_client_2)
        outgoing_peer_fut = self.overlay_2._accept_client(self.outgoing_client_2)
        self.loop.run_until_no_events()
        assert incoming_peer_fut.result().id == "1"
        assert outgoing_peer_fut.result().id == "1"
        assert self.overlay_2.peers["1"].address == self.outgoing_client_2.address

    def test_accept_higher_outgoing_then_incoming(self):
        outgoing_peer_fut = self.overlay_2._accept_client(self.outgoing_client_2)
        incoming_peer_fut = self.overlay_2._accept_client(self.incoming_client_2)
        self.loop.run_until_no_events()
        assert incoming_peer_fut.result().id == "1"
        assert outgoing_peer_fut.result().id == "1"
        assert self.overlay_2.peers["1"].address == self.outgoing_client_2.address


class TestIntegration:
    def setup_method(self, method):
        self.loop = testutils.TestLoop()
        asyncio.set_event_loop(self.loop)

        self.network_1 = net.Network()
        self.overlay_1 = overlay.Overlay("1", 8001, self.network_1, self.overlay1_peer_accepted_cb)

        self.network_2 = net.Network()
        self.overlay_2 = overlay.Overlay("2", 8002, self.network_2, self.overlay2_peer_accepted_cb)

        self.overlay_1_accepted_peer = None
        self.overlay_2_accepted_peer = None

    def overlay1_peer_accepted_cb(self, peer):
        self.overlay_1_accepted_peer = peer

    def overlay2_peer_accepted_cb(self, peer):
        self.overlay_2_accepted_peer = peer

    def teardown_method(self, method):
        self.overlay_1.stop()
        self.overlay_2.stop()
        self.loop.close()

    def test_connect(self):
        listen_fut = self.overlay_2.listen()
        self.loop.run_until_complete(listen_fut, raise_exceptions=True)
        assert listen_fut.done()

        peer_fut = self.overlay_1.add_peer(("127.0.0.1", 8002))
        self.loop.run_until_complete(peer_fut)
        assert peer_fut.result().id == "2"
        assert self.overlay_2_accepted_peer.result().id == "1"

        assert list(self.overlay_2.peers.keys()) == ["1"]
        assert list(self.overlay_1.peers.keys()) == ["2"]

    def test_connect_both_ways(self):
        overlay_1_peer_fut = self.overlay_1.add_peer(("127.0.0.1", 8002))
        overlay_2_peer_fut = self.overlay_2.add_peer(("127.0.0.1", 8001))

        overlay_2_listen_fut = self.overlay_2.listen()
        overlay_1_listen_fut = self.overlay_1.listen()

        self.loop.run_until_complete(overlay_1_peer_fut)
        self.loop.run_until_complete(overlay_2_peer_fut)

        self.loop.run_until_complete(overlay_2_listen_fut)
        self.loop.run_until_complete(overlay_1_listen_fut)

        assert overlay_1_peer_fut.result().id == "2"
        assert self.overlay_2_accepted_peer.result().id == "1"

        assert list(self.overlay_2.peers.keys()) == ["1"]
        assert list(self.overlay_1.peers.keys()) == ["2"]