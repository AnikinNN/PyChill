import asyncio
import pytest

from tests.limited_server import ServerRateLimitError
from tests.utils import reraise_gather


async def test_redis_empty(redis_limiter, clean_redis):
    await asyncio.sleep(5)




async def test_redis_itself(redis_client):
    res_1 = await redis_client.xread(
        streams={"foo": '1000-0'}
    )
