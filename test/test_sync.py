# encoding: utf-8
from unittest import mock
import time
from event import EventType
import sync
from os.path import join as jn


class TestSyncPoint:
    def setup_method(self, method):
        self.send_file_mock = mock.Mock()
        self.scanner_mock = mock.Mock()
        self.mount_path = jn("some", "path")
        self.sync_point = sync.SyncPoint("1", self.mount_path,
                                         self.send_event,
                                         self.send_file_mock,
                                         self.scanner_mock, mock.Mock())
        self.events_sent = []

    def teardown_method(self, method):
        pass

    def send_event(self, evt):
        self.events_sent.append(evt)

    def test_file_scanned_deleted(self):
        file_name = jn(self.mount_path, "a", "file.txt")
        mtime = time.time()
        self.sync_point.file_scanned(file_name, False, mtime, 100, True)
        assert len(self.events_sent) == 1
        evt = self.events_sent[0]
        assert evt.type == EventType.DELETE
        assert evt.sync_point == self.sync_point.id
        assert evt.source_path == jn("a", "file.txt")
        assert not evt.is_directory

    def test_file_scanned_directory(self):
        file_name = jn(self.mount_path, "a", "directory")
        mtime = time.time()
        self.sync_point.file_scanned(file_name, True, mtime, 100, False)
        assert len(self.events_sent) == 1
        evt = self.events_sent[0]
        assert evt.type == EventType.CREATE
        assert evt.sync_point == self.sync_point.id
        assert evt.source_path == jn("a", "directory")
        assert evt.is_directory

    def test_file_scanned(self):
        file_name = jn(self.mount_path, "a", "file.txt")
        mtime = time.time()
        with mock.patch('sync.get_file_hash') as get_file_hash_mock:
            get_file_hash_mock.return_value = "AABBCC"
            self.sync_point.file_scanned(file_name, False, mtime, 100, False)
            assert len(self.events_sent) == 1
            evt = self.events_sent[0]
            assert evt.type == EventType.MODIFY
            assert evt.sync_point == self.sync_point.id
            assert evt.size == 100
            assert evt.source_path == jn("a", "file.txt")
            assert evt.hash == "AABBCC"


class TestRemoteEventHandler:
    def setup_method(self, method):
        self.mount_path = jn("some", "path")
        self.signal_mock = mock.Mock()
        self.event_handler = sync.RemoteEventHandler(self.mount_path,
                                                     self.send_event,
                                                     self.send_file,
                                                     self.signal_mock,
                                                     mock.Mock())
        self.events_sent = []
        self.files_sent = []

    def teardown_method(self, method):
        pass

    def send_event(self, path, sender):
        self.events_sent.append((path, sender))

    def send_file(self, path, sender):
        self.files_sent.append((path, sender))

    def test_on_fetch(self):
        evt = EventType.create(EventType.FETCH,
                               "sync_point",
                               source_path=jn("some", "file.txt"))
        self.event_handler.on_fetch(evt, "sender")
        assert len(self.files_sent) == 1
        path, sender = self.files_sent[0]
        assert path == jn("some", "file.txt")
        assert sender == "sender"

    # def test_on_create(self):
    #     evt = EventType.create(EventType.CREATE,
    #                            "sync_point",
    #                            source_path=jn("some", "file.txt"),
    #                            is_directory=False)
    #     self.event_handler.on_create(evt, "sender")
    #     assert len(self.files_sent) == 1
    #     path, sender = self.files_sent[0]
    #     assert path == jn("some", "file.txt")
    #     assert sender == "sender"

    def test_on_create_directory(self):
        evt = EventType.create(EventType.CREATE,
                               "sync_point",
                               source_path=jn("some", "file.txt"),
                               is_directory=True)

        with mock.patch('sync.exists') as exists_mock:
            exists_mock.return_value = False
            with mock.patch('sync.makedirs') as makedirs_mock:
                self.event_handler.on_create(evt, "sender")
        assert len(self.files_sent) == 0
        makedirs_mock.assert_called_once_with(
            jn(self.mount_path, evt.source_path))
        self.signal_mock.assert_called_once_with(evt.source_path,
                                                 EventType.CREATE)
