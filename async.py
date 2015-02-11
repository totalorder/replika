# encoding: utf-8
import queue
import threading
from util import Sentinel


class EventThread(threading.Thread):
    def __init__(self, async=False, *args, **kwargs):
        super(EventThread, self).__init__(*args, **kwargs)
        self.running = False
        self.async = async
        self.async_actions = []

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

    def execute_asyncs(self):
        actions_completed = False
        for action in self.async_actions[:]:
            try:
                action.get()
            except action.Exhausted:
                self.async_actions.remove(action)
                actions_completed = True
        return not actions_completed

    def add_async(self, action):
        self.async_actions.append(action)

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
        self.runners = queue.Queue()

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


class P:
    class NotAvailable(Exception):
        pass

    class Exhausted(Exception):
        pass

    def __init__(self, generator):
        self.generator = generator

    def get(self, blocking=False):
        if not blocking:
            try:
                return next(self.generator)
            except StopIteration:
                raise self.Exhausted()
        else:
            try:
                return list(self.generator).pop()
            except IndexError:
                raise self.Exhausted()

    def run(self):
        list(self.generator)


class F(P):
    NOT_AVAILABLE = Sentinel('F.NOT_AVAILABLE')

    def __init__(self, generator):
        super().__init__(generator)
        self.no_result = object()
        self.result = self.no_result

    def get(self, blocking=False):
        if self.result is not self.no_result:
            return self.result

        if not blocking:
            try:
                print("Generator: ", self.generator)
                result = next(self.generator)
                if result is self.NOT_AVAILABLE:
                    raise self.NotAvailable()
                else:
                    self.result = result
                    return result
            except StopIteration:
                raise F.Exhausted()
        else:
            try:
                return list(self.generator).pop()
            except IndexError:
                raise F.Exhausted()

    def run(self):
        list(self.generator)


class AsyncExecution:
    def __init__(self):
        self.runner = self.run()

    def step(self):
        try:
            next(self.runner)
            return False
        except StopIteration:
            return True

    def run(self):
        raise NotImplementedError