# encoding: utf-8
import asyncio
import asyncio.base_events


def create_future_result(result):
    fut = asyncio.Future()
    fut.set_result(result)
    return fut


def get_future_result(future, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    loop.run_until_no_events()
    return future.result()


class TestLoop(asyncio.get_event_loop().__class__):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_if_no_events = False
        self._raise_fut_exceptions = False

    def run_until_no_events(self, raise_exceptions=True):
        self._raise_fut_exceptions = raise_exceptions
        self._stop_if_no_events = True
        try:
            if not self._no_events():
                self.run_forever()
        finally:
            self._stop_if_no_events = False
            self._raise_fut_exceptions = False

    def run_until_complete(self, future, raise_exceptions=True):
        self._raise_fut_exceptions = raise_exceptions
        try:
            super().run_until_complete(future)
        finally:
            self._raise_fut_exceptions = False

    def _no_events(self):
        listening_fds = len(self._selector.get_map()) - self._internal_fds

        tasks_not_done = [task for task in asyncio.tasks.Task.all_tasks(self) if not task.done()]

        if self._raise_fut_exceptions and not tasks_not_done:  # Trigger exceptions
            [task.result() for task in asyncio.tasks.Task.all_tasks(self) if task.done()]

        if self._stop_if_no_events and not self._scheduled and not self._ready and not tasks_not_done \
                and listening_fds == 0:
            return True

    def _run_once(self):
        print("==>")
        super()._run_once()

        if self._no_events():
            raise asyncio.base_events._StopError
