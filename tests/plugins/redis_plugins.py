from logging import getLogger

import pytest
from redis.asyncio import Redis

from pychill import RedisPyChillLimiter


class LoggerRedis(Redis):
    """
    wrapping original redis to get logs on executed commands
    """
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
async def redis_limiter(redis_client, good_rl):
    async with RedisPyChillLimiter(
            rate=good_rl.rate,
            window=good_rl.window,
            redis=redis_client,
    ) as limiter:
        yield limiter


@pytest.fixture
async def bad_redis_limiter(redis_client, bad_rl):
    async with RedisPyChillLimiter(
            rate=bad_rl.rate,
            window=bad_rl.window,
            redis=redis_client,
    ) as limiter:
        yield limiter


@pytest.fixture(autouse=True)
async def clean_redis(redis_client):
    await redis_client.flushdb()
