# encoding: utf-8
import os
import queue
from unittest import mock
import time
import scan
from os.path import join as jn


class TestScan:
    def setup_method(self, method):
        self.output_queue = queue.Queue()
        self.scanner = scan.Scanner(mock.Mock(), self.output_queue,
                                    db_factory=mock.Mock())

    def teardown_method(self, method):
        pass

    def test_add_path(self):
        self.scanner.pool = mock.Mock()
        path = os.path.join("some", "file.txt")
        sync_point = "sync_point"
        self.scanner.add_path(sync_point, path)
        assert self.scanner.pool.apply_async.called_once_with(
            mock.ANY, sync_point, path)
        assert len(self.scanner.scans_in_progress) == 1

    def test_scan_path(self):
        with mock.patch('os.walk') as walk_mock:
            walk_mock.return_value = \
                (("folder", ("subfolder", "subfolder2"), ("file1.txt",)),
                 (jn("folder", "subfolder"), tuple(), ("subfile1.txt",)),
                 (jn("folder", "subfolder2"), tuple(), tuple()))
            sentinel = object()
            root = os.path.join("some", "path")
            sync_point = "sync_point"
            self.scanner.scans_in_progress[sentinel] = mock.Mock()
            self.scanner.scan_path(sentinel, sync_point, root)
            assert self.scanner.paths_to_scan == \
                [(sync_point, False, jn("folder", "file1.txt")),
                 (sync_point, False, jn("folder", "subfolder", "subfile1.txt")),
                 (sync_point, True, jn("folder", "subfolder2"))]

    def test_step_sleeps_on_full_queue(self):
        with mock.patch('time.sleep') as sleep_mock:
            self.scanner.output_queue = mock.Mock()
            self.scanner.output_queue.qsize.return_value = 21
            self.scanner.step()
            assert sleep_mock.called_once_with(0.5)

    def test_step_scans_file(self):
        self.scanner.db.get.return_value = None
        self.scanner.db.__setitem__ = mock.Mock()
        sync_point = "sync_point"
        self.scanner.paths_to_scan = \
            [(sync_point, False, jn("some", "file.txt")),
             (sync_point, True, jn("other", "path"))]
        with mock.patch('scan.getmtime') as getmtime_mock:
            mtime = time.time()
            getmtime_mock.return_value = mtime
            with mock.patch('scan.getsize') as getsize_mock:
                with mock.patch('random.randint') as randint_mock:
                    getsize_mock.return_value = 100
                    randint_mock.return_value = 0
                    self.scanner.step()
                    assert self.output_queue.qsize() == 1
                    assert self.output_queue.get_nowait() == \
                        ([sync_point], jn("some", "file.txt"), False, mtime,
                         100, False)
                    self.scanner.db.__setitem__.assert_called_once_with(
                        jn("some", "file.txt"),
                        {"sync_points": [sync_point],
                         "size": 100,
                         "mtime": mtime,
                         "is_dir": False}
                    )
                    self.scanner.db.__setitem__.reset_mock()

                    self.scanner.step()
                    assert self.output_queue.qsize() == 1
                    assert self.output_queue.get_nowait() == \
                        ([sync_point], jn("other", "path"), True, mtime, 100,
                         False)
                    self.scanner.db.__setitem__.assert_called_once_with(
                        jn("other", "path"),
                        {"sync_points": [sync_point],
                         "size": 100,
                         "mtime": mtime,
                         "is_dir": True}
                    )

    def test_step_queues_deletes(self):
        self.scanner.scans_finished = [mock.Mock()]
        self.scanner.db.keys.return_value = [jn("some", "file.txt")]
        self.scanner.step()
        assert self.scanner.scans_finished == []
        assert self.scanner.deletes_to_check == [jn("some", "file.txt")]

    def test_step_deletes_queued_deletes(self):
        self.scanner.deletes_to_check = [jn("some", "file.txt")]
        sync_point = "sync_point"
        mtime = time.time()
        self.scanner.db.get.return_value = {"sync_points": [sync_point],
                                            "size": 100,
                                            "mtime": mtime,
                                            "is_dir": False}
        self.scanner.step()
        assert self.output_queue.qsize() == 1
        assert self.output_queue.get_nowait() == \
            ([sync_point], jn("some", "file.txt"), False, mtime, 100, True)