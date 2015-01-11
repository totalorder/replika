# encoding: utf-8
from watchdog.events import FileSystemEventHandler
from network import EventType
from os.path import getsize, getmtime
from os import sep
import hashlib

BLOCK_SIZE = 65536


def get_file_hash(path):
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        buffer = f.read(BLOCK_SIZE)
        while len(buffer) > 0:
            hasher.update(buffer)
            buffer = f.read(BLOCK_SIZE)
    return hasher.hexdigest()


class ReplikaFileSystemEventHandler(FileSystemEventHandler):

    def __init__(self, id, mount_path, send_event):
        self.id = id
        self.send_event = send_event
        self.mount_path = mount_path
        self.mount_path_len = len(self.mount_path)
        super(ReplikaFileSystemEventHandler, self).__init__()

    def relpath(self, path):
        return path[self.mount_path_len:].strip(sep)

    def on_moved(self, event):
        super(ReplikaFileSystemEventHandler, self).on_moved(event)
        self.send_event(EventType.create(EventType.MOVE, self.id, source_path=self.relpath(event.src_path),
                                   destination_path=self.relpath(event.dest_path), is_directory=event.is_directory))

    def on_created(self, event):
        super(ReplikaFileSystemEventHandler, self).on_created(event)
        if event.is_directory:
            self.send_event(EventType.create(EventType.CREATE, self.id, source_path=self.relpath(event.src_path),
                                             is_directory=event.is_directory))

    def on_deleted(self, event):
        super(ReplikaFileSystemEventHandler, self).on_deleted(event)
        self.send_event(EventType.create(EventType.DELETE, self.id, source_path=self.relpath(event.src_path),
                                         is_directory=event.is_directory))

    def on_modified(self, event):
        super(ReplikaFileSystemEventHandler, self).on_modified(event)
        if not event.is_directory:
            self.send_event(EventType.create(EventType.MODIFY, self.id, source_path=self.relpath(event.src_path),
                                       size=getsize(event.src_path), hash=get_file_hash(event.src_path),
                                       modified_date=getmtime(event.src_path)))


