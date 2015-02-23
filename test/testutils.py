# encoding: utf-8
import asyncio
import asyncio.base_events
from unittest.mock import Mock
import signals


def get_spy_processor():
    processor = signals.Processor(async=True)
    return Mock(spec=signals.Processor, wraps=processor)


class TestLoop(asyncio.get_event_loop().__class__):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_if_no_events = False

    def run_until_no_events(self):
        self._stop_if_no_events = True
        try:
            if not self._no_events():
                self.run_forever()
        finally:
            self._stop_if_no_events = False

    def _no_events(self):
        listening_fds = len(self._selector.get_map()) - self._internal_fds

        tasks_not_done = [task for task in asyncio.tasks.Task.all_tasks(self) if not task.done()]

        if not tasks_not_done:  # Trigger exceptions
            [task.result() for task in asyncio.tasks.Task.all_tasks(self) if task.done()]

        if self._stop_if_no_events and not self._scheduled and not self._ready and not tasks_not_done \
                and listening_fds == 0:
            return True

    def _run_once(self):
        print("==>")
        super()._run_once()

        if self._no_events():
            raise asyncio.base_events._StopError
