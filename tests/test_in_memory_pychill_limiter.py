from datetime import timedelta
import pytest

from pychill import InMemoryPyChillLimiter
from tests.limited_server import LimitedServer, ServerRateLimit
from tests.utils import reraise_gather


async def test_normal():
    server = LimitedServer(10, timedelta(seconds=1))
    limiter = InMemoryPyChillLimiter(rate=10, window=timedelta(seconds=1))

    @limiter.decorator
    async def should_be_limited(*args, **kwargs):
        return await server.run(*args, **kwargs)

    res = []
    for _ in range(2):
        r = await reraise_gather(*(
            should_be_limited(i)
            for i in range(30)
        ))
        res.extend(r)

    assert all(map(lambda x: isinstance(x, str), res))


async def test_brake_the_limit():
    server = LimitedServer(10, timedelta(seconds=1))
    limiter = InMemoryPyChillLimiter(rate=100, window=timedelta(seconds=1))

    @limiter.decorator
    async def should_be_limited(*args, **kwargs):
        return await server.run(*args, **kwargs)

    with pytest.raises(ExceptionGroup) as e:
        await reraise_gather(*(
            should_be_limited(i)
            for i in range(30)
        ))

    assert all(map(lambda x: isinstance(x, ServerRateLimit), e.value.exceptions))


async def test_gather():
    server = LimitedServer(10, timedelta(seconds=1))
    limiter = InMemoryPyChillLimiter(rate=10, window=timedelta(seconds=1))

    async def should_be_limited(*args, **kwargs):
        return await server.run(*args, **kwargs)

    res = []
    for _ in range(2):
        r = await limiter.gather(*(
            should_be_limited(i)
            for i in range(30)
        ))
        res.extend(r)

    assert all(map(lambda x: isinstance(x, str), res))


async def test_gather_brake_the_limit():
    server = LimitedServer(10, timedelta(seconds=1))
    limiter = InMemoryPyChillLimiter(rate=100, window=timedelta(seconds=1))

    async def should_be_limited(*args, **kwargs):
        return await server.run(*args, **kwargs)

    with pytest.raises(ServerRateLimit):
        await limiter.gather(*(
            should_be_limited(i)
            for i in range(30)
        ))


async def test_gather_brake_the_limit_return_exceptions():
    server = LimitedServer(10, timedelta(seconds=1))
    limiter = InMemoryPyChillLimiter(rate=100, window=timedelta(seconds=1))

    async def should_be_limited(*args, **kwargs):
        return await server.run(*args, **kwargs)

    res = []
    for _ in range(2):
        r = await limiter.gather(
            *(
                should_be_limited(i)
                for i in range(30)
            ),
            return_exceptions=True,
        )
        res.extend(r)

    errors = list(filter(lambda x: isinstance(x, Exception), res))
    normal_results = list(filter(lambda x: not isinstance(x, Exception), res))

    assert all(map(lambda x: isinstance(x, ServerRateLimit), errors))
    assert all(map(lambda x: isinstance(x, str), normal_results))
