# encoding: utf-8
import queue
import threading
import functools
import asyncio
from util import Sentinel


def task(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.async(asyncio.coroutine(func)(*args, **kwargs))
    return wrapper


class EventThread(threading.Thread):
    def __init__(self, async=False, *args, **kwargs):
        super(EventThread, self).__init__(*args, **kwargs)
        self.running = False
        self.async = async
        self.async_actions = []

    @task
    def setup(self):
        pass

    @task
    def step(self):
        raise NotImplementedError

    @task
    def teardown(self):
        pass

    def stop(self):
        pass

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

    @asyncio.coroutine
    def run(self):
        yield from self._loop_until_done(self.setup())
        yield from self._loop_until_done(self.step())
        yield from self._loop_until_done(self.teardown())

    @asyncio.coroutine
    def _loop_until_done(self, fut):
        while not fut.done():
            yield from fut

    @asyncio.coroutine
    def step_until_done(self):
        yield from self.run()


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
            finished, last_done = self.step(last_done)
            if finished:
                break

    def step(self, last_done=None):
        runner = self.runners.get()
        if runner.step():
            if last_done is runner:
                self.runners.put(runner)
                return True, last_done
            elif last_done is None:
                last_done = runner
        else:
            last_done = None
        self.runners.put(runner)
        return False, last_done

    def run_asyncio_until(self, predicate):
        @asyncio.coroutine
        def run_loop():
            while 1:
                self.step()
                yield
                if predicate():
                    return
        asyncio.async(run_loop())


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