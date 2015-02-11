# encoding: utf-8
import Queue
from collections import defaultdict
import time
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
        self.delayed_signals = Queue.Queue()

    def signal(self, signal_code, data):
        print signal_code, data
        self.signals.put((signal_code, data))

    def delayed_signal(self, signal_code, data, delay):
        self.delayed_signals.put((time.time() + delay, signal_code, data))

    def step(self):
        now = time.time()
        while 1:
            try:
                send_time, signal_code, data = self.delayed_signals.get_nowait()
                if now >= send_time:
                    self.signal(signal_code, data)
                else:
                    self.delayed_signals.put((send_time, signal_code, data))
            except Queue.Empty:
                break

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