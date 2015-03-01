# encoding: utf-8
import asyncio
from py2py import net
from py2py import overlay
from unittest.mock import Mock, ANY, patch
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
        self.outgoing_client_1.recvdata.return_value = cfr((overlay.Overlay.ConnectionType.PEER,))
        self.outgoing_client_1.address.return_value = ("127.0.0.1", 8002)

        self.incoming_client_1 = Mock(spec=net.Client)()
        self.incoming_client_1.is_outgoing = False
        self.incoming_client_1.recvmessage.return_value = cfr(b"2")
        self.incoming_client_1.recvdata.return_value = cfr((overlay.Overlay.ConnectionType.PEER,))
        self.incoming_client_1.address.return_value = ("127.0.0.1", 8002)

        self.overlay_2 = overlay.Overlay("2", 8002, self.network, self.peer_accepted_cb)
        self.outgoing_client_2 = Mock(spec=net.Client)()
        self.outgoing_client_2.is_outgoing = True
        self.outgoing_client_2.recvmessage.return_value = cfr(b"1")
        self.outgoing_client_2.recvdata.return_value = cfr((overlay.Overlay.ConnectionType.PEER,))
        self.outgoing_client_2.address.return_value = ("127.0.0.1", 8001)

        self.incoming_client_2 = Mock(spec=net.Client)()
        self.incoming_client_2.is_outgoing = False
        self.incoming_client_2.recvmessage.return_value = cfr(b"1")
        self.incoming_client_2.recvdata.return_value = cfr((overlay.Overlay.ConnectionType.PEER,))
        self.incoming_client_2.address.return_value = ("127.0.0.1", 8001)

    def teardown_method(self, method):
        self.loop.close()

    def peer_accepted_cb(self, peer):
        self.accepted_peer = peer

    def test_accept_peer(self):
        peer_fut = self.overlay_1._accept_client(self.incoming_client_1)
        self.loop.run_until_complete(peer_fut)
        assert peer_fut.result().id == "2"
        assert list(self.overlay_1.peers.keys()) == ["2"]
        assert self.overlay_1.peers["2"].address == self.incoming_client_1.address

    def test_accept_file_client(self):
        self.incoming_client_1.recvdata.return_value = cfr((overlay.Overlay.ConnectionType.FILE_CLIENT,))

        file_client_fut = self.overlay_1._accept_client(self.incoming_client_1)
        self.loop.run_until_complete(file_client_fut)
        assert file_client_fut.result().id == "2"
        self.overlay_1.file_clients.keys() == ["2"]
        assert self.overlay_1.file_clients["2"].address == self.incoming_client_1.address
        assert list(self.overlay_1.unbound_file_clients.keys()) == ["2"]

    def test_file_client_bound(self):
        peer_fut = self.overlay_1._accept_client(self.incoming_client_1)
        self.loop.run_until_complete(peer_fut)
        assert peer_fut.result().id == "2"
        assert list(self.overlay_1.peers.keys()) == ["2"]
        assert self.overlay_1.peers["2"].address == self.incoming_client_1.address

        self.incoming_client_1.recvdata.return_value = cfr((overlay.Overlay.ConnectionType.FILE_CLIENT,))

        file_client_fut = self.overlay_1._accept_client(self.incoming_client_1)
        self.loop.run_until_complete(file_client_fut)
        assert file_client_fut.result().id == "2"
        self.overlay_1.file_clients.keys() == ["2"]
        assert self.overlay_1.file_clients["2"].address == self.incoming_client_1.address

        assert self.overlay_1.peers["2"].file_client == self.overlay_1.file_clients["2"]
        assert list(self.overlay_1.unbound_file_clients.keys()) == []


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

    def test_connect_with_retries(self):
        self.overlay_1.retry_wait = 0
        mock_client = Mock(spec=overlay.Peer)()
        mock_client.id = "2"
        client_fut = cfr(mock_client)
        connection_refused_fut = asyncio.futures.Future()
        connection_refused_fut.set_exception(ConnectionRefusedError)
        connect_side_effects = [connection_refused_fut, connection_refused_fut,
                                client_fut,             connection_refused_fut,
                                                        client_fut]
        self.overlay_1.network.connect.side_effect = lambda address: connect_side_effects.pop(0)

        connect_fut = self.overlay_1._connect_with_retries(("127.0.0.1", 8002))
        self.loop.run_until_complete(connect_fut)
        client, file_client = connect_fut.result()
        assert client.id == "2"
        assert file_client.id == "2"


class TestIntegration:
    def setup_method(self, method):
        self.loop = testutils.TestLoop()
        asyncio.set_event_loop(self.loop)

        self.network_1 = net.Network()
        self.overlay_1 = overlay.Overlay("1", 8001, self.network_1, self.overlay1_peer_accepted_cb)
        self.overlay_1.retry_wait = 0.1

        self.network_2 = net.Network()
        self.overlay_2 = overlay.Overlay("2", 8002, self.network_2, self.overlay2_peer_accepted_cb)
        self.overlay_2.retry_wait = 0.1

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
        assert peer_fut.result().address == ("127.0.0.1", 8002)
        assert peer_fut.result().file_client.id == "2"
        assert list(self.overlay_1.peers.keys()) == ["2"]

        assert list(self.overlay_2.peers.keys()) == ["1"]
        assert self.overlay_2.peers["1"].id == "1"
        assert self.overlay_2.peers["1"].address == ("127.0.0.1", ANY)
        assert self.overlay_2.peers["1"].file_client.id == "1"


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

    def test_transfer_data_and_file(self, tmpdir):
        listen_fut = self.overlay_2.listen()
        self.loop.run_until_complete(listen_fut, raise_exceptions=True)
        assert listen_fut.done()

        peer_fut = self.overlay_1.add_peer(("127.0.0.1", 8002))
        self.loop.run_until_complete(peer_fut)
        assert peer_fut.result().id == "2"
        assert peer_fut.result().address == ("127.0.0.1", 8002)
        assert peer_fut.result().file_client.id == "2"
        assert list(self.overlay_1.peers.keys()) == ["2"]

        assert list(self.overlay_2.peers.keys()) == ["1"]
        assert self.overlay_2.peers["1"].id == "1"
        assert self.overlay_2.peers["1"].address == ("127.0.0.1", ANY)
        assert self.overlay_2.peers["1"].file_client.id == "1"

        tmp_file_path = tmpdir.join("test_file.txt")
        tmp_file = tmp_file_path.open(mode='wb+', ensure=True)
        tmp_file.write(b"File content!")
        tmp_file.seek(0)

        self.overlay_1.peers["2"].sendfile(tmp_file, b"Some metadata")

        with patch('tempfile.TemporaryFile', new_callable=testutils.FakeFile):
            recvfile_fut = self.overlay_2.peers["1"].recvfile()
            self.loop.run_until_complete(recvfile_fut)
            received_file, recevied_metadata = recvfile_fut.result()
            assert received_file.read() == b"File content!"
            assert recevied_metadata == b"Some metadata"

        self.overlay_1.peers["2"].sendmessage(b"Hello!")
        recvmessage_fut = self.overlay_2.peers["1"].recvmessage()
        self.loop.run_until_complete(recvmessage_fut)
        assert recvmessage_fut.result() == b"Hello!"