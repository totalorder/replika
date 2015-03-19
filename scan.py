# encoding: utf-8
from multiprocessing.dummy import Pool
import os
import random
import threading
from os.path import getsize, getmtime, exists
from sqlitedict import SqliteDict
import time


class Scanner(threading.Thread):
    def __init__(self, db_name, output_queue):
        super().__init__()
        self.setDaemon(True)
        self.output_queue = output_queue
        self.paths_to_scan = []
        self.pool = Pool(3)
        self.db = SqliteDict('{}.sqlite'.format(db_name), autocommit=True)
        self.scans_in_progress = {}
        self.scans_finished = []
        self.deletes_to_check = []

    def add_path(self, sync_point, path):
        sentinel = object()
        self.pool.apply_async(self.scan_path, (sentinel, sync_point, path))

    def scan_path(self, sentinel, sync_point, root):
        for path, subdirs, files in os.walk(root):
            for name in files:
                self.paths_to_scan.append((sync_point, False, os.path.join(path, name)))
            if not files:
                self.paths_to_scan.append((sync_point, True, path))
        self.scans_finished.append((sync_point, root))
        del self.scans_in_progress[sentinel]

    def run(self):
        while 1:
            if self.output_queue.qsize() > 20:
                time.sleep(0.5)
                continue

            if self.paths_to_scan:

                sync_point, is_dir, path = self.paths_to_scan.pop(random.randint(0, len(self.paths_to_scan) - 1))
                mtime = getmtime(path)
                record = self.db.get(path, None)
                record_is_new = False
                if record is not None and record['mtime'] == mtime:
                    continue
                elif record is None:
                    record_is_new = True
                    record = {'sync_points': []}
                size = getsize(path)

                record['size'] = size
                record['mtime'] = mtime
                record['is_dir'] = is_dir
                record['sync_points'].append(sync_point)

                if record_is_new or record['size'] != size:
                    self.output_queue.put((record['sync_points'], path, is_dir, mtime, size, False))
                self.db[path] = record
            elif not self.scans_in_progress and self.scans_finished:
                self.scans_finished.clear()
                self.deletes_to_check = self.db.keys()
            elif self.deletes_to_check:
                path = self.deletes_to_check.pop(random.randint(0, len(self.deletes_to_check) - 1))
                record = self.db.get(path, None)
                if record is not None:
                    if not exists(path):
                        self.output_queue.put((record['sync_points'],
                                               path,
                                               record['is_dir'],
                                               record['mtime'],
                                               record['size'],
                                               True))
            else:
                time.sleep(1)
