# encoding: utf-8
from os.path import exists, getsize, join as jn
from filesystem import ReplikaFileSystemEventHandler
from network import EventType
from util import HierarchyLogger


class SyncPoint(object):
    def __init__(self, id, mount_path, send, logger):
        self.id = id
        self.mount_path = mount_path
        self.started = False
        self.watch = None
        self.send = send
        self.logger = HierarchyLogger(lambda: u"SyncPoint %s" % self.id, logger)
        self.remote_handler = RemoteEventHandler(mount_path, send, logger)

    def start(self, observer):
        if self.started:
            return False
        else:
            event_handler = ReplikaFileSystemEventHandler(self.id, self.send)
            self.watch = observer.schedule(event_handler, self.mount_path, recursive=True)
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
    def __init__(self, mount_point, send, logger):
        self.logger = logger
        self.mount_point = mount_point
        self.send = send
        self.handlers = {EventType.FETCH: self.on_fetch,
                         EventType.CREATE: self.on_create,
                         EventType.DELETE: self.on_delete,
                         EventType.MODIFY: self.on_modify,
                         EventType.MOVE: self.on_move}

    def on_fetch(self, evt, sender):
        self.logger.info(u"FETCH %s from %s", evt, sender)

    def on_create(self, evt, sender):
        self.logger.info(u"CREATE %s from %s", evt, sender)

    def on_delete(self, evt, sender):
        self.logger.info(u"DELETE %s from %s", evt, sender)

    def on_modify(self, evt, sender):
        self.logger.info(u"MODIFY %s from %s", evt, sender)
        source_path = jn(self.mount_point, evt.source_path)
        source_exists = exists(source_path)
        if not source_exists or (source_exists and getsize(source_path) != evt.size):
            self.send(EventType.create(EventType.FETCH, evt.sync_point, evt.source_path), sender)
        else:
            print "Skip"

    def on_move(self, evt, sender):
        self.logger.info(u"MOVE %s from %s", evt, sender)

    def on_event(self, evt, sender):
        return self.handlers[evt.type](evt, sender)