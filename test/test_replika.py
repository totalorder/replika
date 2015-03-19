# encoding: utf-8
import asyncio
from io import BytesIO
import queue
from unittest import mock
import time
from event import EventType
from py2py import overlay
import replika
from test import testutils
from test.testutils import create_future_result as cfr, get_future_result as gfr


class TestPeer:
    def setup_method(self, method):
        self.loop = testutils.TestLoop()
        asyncio.set_event_loop(self.loop)

        self.incoming_messages = queue.Queue()
        self.overlay_peer_mock = mock.Mock(spec=overlay.Peer)
        self.overlay_peer_mock.address = ("127.0.0.1", 5000)
        self.peer = replika.Peer("ring0", self.overlay_peer_mock,
                                 self.incoming_messages)
        self.receieved_files = []
        self.receieved_messages = []
        self.overlay_peer_mock.sendfile.side_effect = \
            lambda file, metadata: self.receieved_files.append((file, metadata))
        self.overlay_peer_mock.recvfile.side_effect = \
            lambda: cfr(self.receieved_files.pop(-1))

        self.overlay_peer_mock.sendmessage.side_effect = \
            lambda message: self.receieved_messages.append(message)
        self.overlay_peer_mock.recvmessage.side_effect = \
            lambda: cfr(self.receieved_messages.pop(-1))

        self.overlay_peer_mock.id = "2"
        self.open_patcher = mock.patch('replika.open',
                                       mock.Mock(spec=open), create=True)
        self.open_mock = self.open_patcher.start()
        fake_file = BytesIO()
        fake_file.write(b"file_content")
        fake_file.seek(0)
        self.open_mock.return_value = fake_file
        self.file_time = time.time()
        self.getmtime_patcher = mock.patch('os.path.getmtime',
                                           lambda x: self.file_time)
        self.getmtime_patcher.start()

    def teardown_method(self, method):
        self.open_patcher.stop()
        self.getmtime_patcher.stop()

    def test_transfer_file(self):
        self.peer.sendfile("sync_point", "mount_path", "path")
        file_fut = self.peer._recvfile()
        self.loop.run_until_complete(file_fut)
        message_type, remote_peer_id, data = self.incoming_messages.get_nowait()
        assert message_type == replika.Peer.MessageReceivedType.FILE
        assert remote_peer_id == "2"
        file, sync_point, path, file_time = data
        assert file.read() == b"file_content"
        assert sync_point == "sync_point"
        assert path == "path"
        assert file_time == self.file_time

    def test_transfer_event(self):
        evt = EventType.create(EventType.FETCH, "sync_point", "source_path")
        self.peer.sendevent(evt)
        self.loop.run_until_complete(self.peer._recvevent())

        message_type, remote_peer_id, deserialized_evt = \
            self.incoming_messages.get_nowait()
        assert message_type == replika.Peer.MessageReceivedType.EVENT
        assert evt.type == deserialized_evt.type
        assert evt.sync_point == deserialized_evt.sync_point
        assert evt.source_path == deserialized_evt.source_path


class TestClient:
    def setup_method(self, method):
        self.loop = testutils.TestLoop()
        asyncio.set_event_loop(self.loop)

        self.observer_factory_mock = mock.Mock()
        self.overlay_factory_mock = mock.Mock()
        self.client = replika.Client("1", "ring-0", loop=self.loop,
            observer_factory=self.observer_factory_mock,
            overlay_factory=self.overlay_factory_mock)
        self.overlay_peer_mock = mock.Mock(spec=overlay.Peer)
        self.overlay_peer_mock.id = "1"
        self.overlay_peer_mock.address = ("127.0.0.1", 5000)
        self.overlay_peer_mock.recvstring.return_value = cfr("ring-1")

    def teardown_method(self, method):
        self.loop.close()

    def test_accept_peer_ignores_existing(self):
        existing_peer_mock = mock.Mock()
        existing_peer_mock.overlay_peer = self.overlay_peer_mock
        self.client.peers["1-ring-1"] = existing_peer_mock

        peer_fut = self.client.accept_peer(self.overlay_peer_mock)
        assert gfr(peer_fut) == existing_peer_mock

    def test_accept_peer_outgoing_is_accepted(self):
        self.overlay_peer_mock.is_outgoing = True

        peer_fut = self.client.accept_peer(self.overlay_peer_mock)
        self.loop.run_until_complete(peer_fut)
        peer = peer_fut.result()
        assert peer.address == self.overlay_peer_mock.address

    def test_accept_peer_incoming_is_accepted(self):
        self.overlay_peer_mock.is_outgoing = False

        peer_fut = self.client.accept_peer(self.overlay_peer_mock)
        self.loop.run_until_complete(peer_fut)
        peer = peer_fut.result()
        assert peer.address == self.overlay_peer_mock.address

    def test_accept_peer_in_flight(self):
        self.overlay_peer_mock.is_outgoing = True
        flight_future = asyncio.futures.Future()
        self.client.flight[self.overlay_peer_mock] = flight_future
        peer_mock = mock.Mock()
        flight_future.set_result(peer_mock)

        peer_fut = self.client.accept_peer(self.overlay_peer_mock)
        self.loop.run_until_complete(peer_fut)
        peer = peer_fut.result()
        assert peer == peer_mock

    def test_add_peer_ignores_existing(self):
        existing_peer_mock = mock.Mock()
        existing_peer_mock.address = self.overlay_peer_mock.address
        self.client.peers["1-ring-1"] = existing_peer_mock

        peer_fut = self.client._add_peer(self.overlay_peer_mock.address)

        self.loop.run_until_complete(peer_fut)
        assert peer_fut.result() == existing_peer_mock

    def test_add_peer(self):
        self.client.overlay.add_peer = lambda _: cfr(self.overlay_peer_mock)
        peer_mock = mock.Mock()
        self.client.accept_peer = lambda _: cfr(peer_mock)
        peer_fut = self.client._add_peer(("127.0.0.1", 5000))
        assert gfr(peer_fut) == peer_mock
