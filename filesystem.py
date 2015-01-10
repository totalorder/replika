# encoding: utf-8
from watchdog.events import FileSystemEventHandler
from network import EventType
from os.path import getsize


class ReplikaFileSystemEventHandler(FileSystemEventHandler):
    def __init__(self, id, send):
        self.id = id
        self.send = send
        super(ReplikaFileSystemEventHandler, self).__init__()

    def on_moved(self, event):
        super(ReplikaFileSystemEventHandler, self).on_moved(event)
        # what = u'directory' if event.is_directory else u'file'
        self.send(EventType.create(EventType.MOVE, self.id, source_path=event.src_path,
                                   destination_path=event.dest_path))

    def on_created(self, event):
        super(ReplikaFileSystemEventHandler, self).on_created(event)
        self.send(EventType.create(EventType.CREATE, self.id, source_path=event.src_path))

    def on_deleted(self, event):
        super(ReplikaFileSystemEventHandler, self).on_deleted(event)
        self.send(EventType.create(EventType.DELETE, self.id, source_path=event.src_path))

    def on_modified(self, event):
        super(ReplikaFileSystemEventHandler, self).on_modified(event)
        self.send(EventType.create(EventType.MODIFY, self.id, source_path=event.src_path,
                                   size=getsize(event.src_path)))
