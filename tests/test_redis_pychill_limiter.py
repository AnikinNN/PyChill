import asyncio
from datetime import timedelta
from logging import getLogger

import pytest
from redis.asyncio import Redis

from pychill import RedisPyChillLimiter
from tests.limited_server import LimitedServer, ServerRateLimit
from tests.utils import reraise_gather


class LoggerRedis(Redis):
    logger = getLogger('pychill.redis_logger_')
    async def execute_command(self, *args, **options):
        str_args = []
        for i in args:
            if isinstance(i, bytes):
                str_args.append(i.decode())
            else:
                str_args.append(str(i))
        res = await super().execute_command(*args, **options)
        # self.logger.debug(f'redis execute_command\n{" ".join(str_args)}\n{res}\n{options=}')
        return res


@pytest.fixture
async def redis_client():
    async with LoggerRedis(host='localhost', port=6379) as client:
        yield client


@pytest.fixture
async def limiter(redis_client):
    async with RedisPyChillLimiter(
            rate=10,
            window=timedelta(seconds=1),
            redis=redis_client,
    ) as limiter:
        yield limiter


@pytest.fixture(autouse=True)
async def clean_redis(redis_client):
    await redis_client.flushdb()


@pytest.fixture
def limited_server():
    return LimitedServer(10, timedelta(seconds=1))


async def test_redis_empty(limiter, clean_redis):
    await asyncio.sleep(5)


async def test_redis_normal(limited_server, limiter):
    @limiter.decorator
    async def should_be_limited(*args, **kwargs):
        return await limited_server.run(*args, **kwargs)

    res = []
    for _ in range(2):
        r = await reraise_gather(*(
            should_be_limited(i)
            for i in range(30)
        ))
        res.extend(r)

    assert all(map(lambda x: isinstance(x, str), res))


async def test_redis_one_item(limited_server, limiter):
    @limiter.decorator
    async def should_be_limited(*args, **kwargs):
        return await limited_server.run(*args, **kwargs)

    res = await should_be_limited(1)
    assert isinstance(res, str)


async def test_redis_brake_the_limit(limited_server, limiter):
    limiter.rate = 100

    @limiter.decorator
    async def should_be_limited(*args, **kwargs):
        return await  limited_server.run(*args, **kwargs)

    with pytest.raises(ExceptionGroup) as e:
        await reraise_gather(*(
            should_be_limited(i)
            for i in range(30)
        ))

    assert all(map(lambda x: isinstance(x, ServerRateLimit), e.value.exceptions))


async def test_redis_gather(limited_server, limiter):
    async def should_be_limited(*args, **kwargs):
        return await limited_server.run(*args, **kwargs)

    res = []
    for _ in range(2):
        r = await limiter.gather(*(
            should_be_limited(i)
            for i in range(30)
        ))
        res.extend(r)

    assert all(map(lambda x: isinstance(x, str), res))


async def test_redis_gather_brake_the_limit(limited_server, limiter):
    limiter.rate = 100

    async def should_be_limited(*args, **kwargs):
        return await limited_server.run(*args, **kwargs)

    with pytest.raises(ServerRateLimit):
        await limiter.gather(*(
            should_be_limited(i)
            for i in range(30)
        ))


async def test_redis_gather_brake_the_limit_return_exceptions(limited_server, limiter):
    limiter.rate = 100

    async def should_be_limited(*args, **kwargs):
        return await limited_server.run(*args, **kwargs)

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


async def test_redis_itself(redis_client):
    res_1 = await redis_client.xread(
        streams={"foo": '1000-0'}
    )
