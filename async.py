# encoding: utf-8
import Queue
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
        if self.async:
            self.teardown()
        else:
            self.running = False
            if self.is_alive:
                self.join()

    def run(self):
        if self.async:
            self.setup()
        else:
            self.setup()
            while self.running:
                self.step()
            self.teardown()

    def step_until_done(self):
        while 1:
            if self.step():
                return


class Loop:
    def __init__(self):
        self.runners = Queue.Queue()

    def add_runner(self, runner):
        self.runners.put(runner)

    def run_until_done(self):
        if self.runners.empty():
            return
        last_done = None
        while 1:
            runner = self.runners.get()
            if runner.step():
                if last_done is runner:
                    self.runners.put(runner)
                    return
                elif last_done is None:
                    last_done = runner
            else:
                last_done = None
            self.runners.put(runner)