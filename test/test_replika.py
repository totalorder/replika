# encoding: utf-8
import asyncio
from io import BytesIO
import queue
from unittest import mock
import time
from py2py import overlay
import replika
from test import testutils
from test.testutils import create_future_result as cfr


class TestPeer:
    def setup_method(self, method):
        self.loop = testutils.TestLoop()
        asyncio.set_event_loop(self.loop)

        self.incoming_messages = queue.Queue()
        self.overlay_peer_mock = mock.Mock(spec=overlay.Peer)
        self.peer = replika.Peer("ring0", self.overlay_peer_mock, self.incoming_messages)
        self.receieved_files = []
        self.overlay_peer_mock.sendfile.side_effect = \
            lambda file, metadata: self.receieved_files.append((file, metadata))
        self.overlay_peer_mock.recvfile.side_effect = lambda: cfr(self.receieved_files.pop(-1))
        self.overlay_peer_mock.id = "2"
        self.open_patcher = mock.patch('replika.open', mock.Mock(spec=open), create=True)
        self.open_mock = self.open_patcher.start()
        fake_file = BytesIO()
        fake_file.write(b"file_content")
        fake_file.seek(0)
        self.open_mock.return_value = fake_file
        self.file_time = time.time()
        self.getmtime_patcher = mock.patch('os.path.getmtime', lambda x: self.file_time)
        self.getmtime_patcher.start()


    def teardown_method(self, method):
        self.open_patcher.stop()
        self.getmtime_patcher.stop()

    def test_transfer_file(self):
        self.peer.sendfile("sync_point", "mount_path", "path")
        file_fut = self.peer._recvfile()
        self.loop.run_until_complete(file_fut)
        # self.overlay_peer.id, (file, sync_point, path, file_time))
        message_type, remote_peer_id, data = self.incoming_messages.get_nowait()
        assert message_type == replika.Peer.MessageReceivedType.FILE
        assert remote_peer_id == "2"
        file, sync_point, path, file_time = data
        assert file.read() == b"file_content"
        assert sync_point == "sync_point"
        assert path == "path"
        assert file_time == self.file_time
