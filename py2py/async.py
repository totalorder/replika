# encoding: utf-8
import functools
import asyncio


def task(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.async(asyncio.coroutine(func)(*args, **kwargs))
    return wrapper