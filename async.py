# encoding: utf-8
import threading


class EventThread(threading.Thread):
    def __init__(self, async=False, *args, **kwargs):
        super(EventThread, self).__init__(*args, **kwargs)
        self.running = False
        self.async = async

    def setup(self):
        pass

    def step(self):
        raise NotImplementedError

    def teardown(self):
        pass

    def stop(self):
        self.running = False
        if self.is_alive:
            self.join()

    def run(self):
        self.setup()
        while self.running:
            self.step()
        self.teardown()

    def step_until_done(self):
        while 1:
            if self.step():
                return
