# encoding: utf-8
import Queue
from collections import defaultdict
import async


class Signal(object):
    def __init__(self, id):
        self.id = id

    def __repr__(self):
        return self.id


class Processor(async.EventThread):
    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)
        self.registry = defaultdict(list)
        self.signals = Queue.Queue()
        self.signals_get = self.signals.get_nowait if self.async else self.signals.get

    def signal(self, signal_code, data):
        self.signals.put((signal_code, data))

    def step(self):
        try:
            signal_code, data = self.signals_get()
            for recipient in self.registry[signal_code]:
                if isinstance(recipient, Queue.Queue):
                    recipient.put((signal_code, data))
                else:
                    recipient(signal_code, data)
        except Queue.Empty:
            return True

    def register(self, signal_code, recipient):
        self.registry[signal_code].append(recipient)

    def unregister(self, signal_code, recipient):
        self.registry[signal_code].remove(recipient)