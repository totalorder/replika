# encoding: utf-8
from collections import defaultdict
from os import remove, rmdir, makedirs
from os.path import exists, join as jn, isdir
import shutil
from filesystem import ReplikaFileSystemEventHandler, get_file_hash
from event import EventType
from util import HierarchyLogger


class SyncPoint(object):
    def __init__(self, id, mount_path, send_event, send_file, logger):
        self.id = id
        self.mount_path = mount_path
        self.started = False
        self.watch = None
        self.send_event = send_event
        self.send_file = send_file
        self.logger = HierarchyLogger(lambda: "SyncPoint %s" % self.id, logger)
        self.remote_handler = RemoteEventHandler(mount_path, self.send_event, self._send_file, self.signal, logger)
        self.outstanding_events = defaultdict(list)

    def signal(self, source_path, event_type):
        self.logger.info("Received signal: %s - %s", source_path, event_type)
        self.outstanding_events[source_path].append(event_type)

    def _send_file(self, path, peer_id):
        self.send_file(self.id, path, peer_id)

    def _send_event(self, evt):
        if evt.type in self.outstanding_events[evt.source_path]:
            self.logger.info("Filtered out evt %s because %s", evt, self.outstanding_events[evt.source_path])
            self.outstanding_events[evt.source_path].remove(evt.type)
        else:
            self.send_event(evt)

    def start(self, observer):
        if self.started:
            return False
        else:
            event_handler = ReplikaFileSystemEventHandler(self.id, self.mount_path, self._send_event)
            self.watch = observer.schedule(event_handler, self.mount_path, recursive=True)
            self.logger.info("Observing directory: %s", self.mount_path)
            return True

    def stop(self, observer):
        if self.started:
            observer.unschedule(self.watch)
            self.started = False
            self.watch = None
            return True
        else:
            return False

    def on_event(self, evt, sender):
        self.remote_handler.on_event(evt, sender)


class RemoteEventHandler(object):
    def __init__(self, mount_point, send_event, send_file, signal, logger):
        self.logger = logger
        self.mount_point = mount_point
        self.send_event = send_event
        self.send_file = send_file
        self.signal = signal
        self.handlers = {EventType.FETCH: self.on_fetch,
                         EventType.CREATE: self.on_create,
                         EventType.DELETE: self.on_delete,
                         EventType.MODIFY: self.on_modify,
                         EventType.MOVE: self.on_move}

    def on_fetch(self, evt, sender):
        self.logger.info("FETCH %s from %s", evt, sender)
        self.send_file(evt.source_path, sender)

    def on_create(self, evt, sender):
        self.logger.info("CREATE %s from %s", evt, sender)
        source_path = jn(self.mount_point, evt.source_path)
        if evt.is_directory:
            if not exists(source_path):
                self.signal(evt.source_path, EventType.CREATE)
                makedirs(source_path)
        else:
            self.send_event(EventType.create(EventType.FETCH, evt.sync_point, evt.source_path), sender)

    def on_delete(self, evt, sender):
        self.logger.info("DELETE %s from %s", evt, sender)
        source_path = jn(self.mount_point, evt.source_path)
        if exists(source_path):
            if isdir(source_path) == evt.is_directory:
                self.signal(evt.source_path, EventType.DELETE)
                if evt.is_directory:
                    rmdir(source_path)
                else:
                    remove(source_path)

    def on_modify(self, evt, sender):
        self.logger.info("MODIFY %s from %s", evt, sender)
        source_path = jn(self.mount_point, evt.source_path)
        source_exists = exists(source_path)
        if not source_exists or get_file_hash(source_path) != evt.hash:
            self.send_event(EventType.create(EventType.FETCH, evt.sync_point, evt.source_path), sender)
        else:
            self.logger.info("Skipping equal file %s - %s", evt.sync_point, evt.source_path)

    def on_move(self, evt, sender):
        self.logger.info("MOVE %s from %s", evt, sender)
        source_path = jn(self.mount_point, evt.source_path)
        destination_path = jn(self.mount_point, evt.destination_path)
        if exists(source_path):
            self.signal(evt.source_path, EventType.MOVE)
            if exists(destination_path):
                shutil.copy2(source_path, destination_path)
                remove(source_path)
            else:
                shutil.move(source_path, destination_path)

    def on_event(self, evt, sender):
        return self.handlers[evt.type](evt, sender)