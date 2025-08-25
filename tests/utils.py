import asyncio
import functools
import traceback
from datetime import timedelta

from pydantic import BaseModel


async def reraise_gather(*coroutines):
    async def wrapped(coro):
        try:
            return await coro
        except:
            traceback.print_exc()
            raise

    res = await asyncio.gather(*(wrapped(i) for i in coroutines), return_exceptions=True)
    errors = list(filter(lambda x: isinstance(x, Exception), res))
    if errors:
        raise ExceptionGroup('', errors)
    return res


class RateLimit(BaseModel):
    rate: int
    window: timedelta


def with_timeout(func=None, *, timeout: float | timedelta = timedelta(minutes=1)):
    if isinstance(timeout, timedelta):
        timeout = timeout.total_seconds()

    def timeout_decorator(inner_func):
        @functools.wraps(inner_func)
        async def timeout_wrapper(*args, **kwargs):
            async with asyncio.timeout(timeout):
                return await inner_func(*args, **kwargs)
        return timeout_wrapper

    if func is None:
        return timeout_decorator
    else:
        return timeout_decorator(func)