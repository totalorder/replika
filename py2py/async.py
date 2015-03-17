# encoding: utf-8
from contextlib import contextmanager
import functools
import asyncio


def task(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.async(asyncio.coroutine(func)(*args, **kwargs))
    return wrapper


class FlightControl:
    def __init__(self, *args, **kwargs):
        self.flight = {}
        super().__init__(*args, **kwargs)

    @contextmanager
    def flight_control(self, id):
        self.flight[id] = asyncio.futures.Future()

        yield self.flight[id]

        if id in self.flight:
            del self.flight[id]

    @asyncio.coroutine
    def landed_flight(self, id):
        plane = yield from self.flight[id]
        if id in self.flight:
            del self.flight[id]
        return plane