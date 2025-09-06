import asyncio
from logging import getLogger

import pytest

from pychill import RedisPyChillConfig, RedisPyChillLimiter


async def test_redis_empty(redis_limiter):
    await asyncio.sleep(1)


async def test_two_limiters(redis_client, good_rl, limited_server):
    async def request():
        await limited_server.run()

    async def task(logger_name):
        async with RedisPyChillLimiter(
                rate=good_rl.rate,
                window=good_rl.window,
                redis=redis_client,
                logger=getLogger(logger_name),
        ) as limiter:
            await limiter.gather(*(
                request() for _ in range(good_rl.rate * 1)
            ))

    one = asyncio.create_task(task('pychill.one'))
    await asyncio.sleep(RedisPyChillConfig().leader_heartbeat_timeout.total_seconds())
    two = asyncio.create_task(task('pychill.two'))

    await one
    await two

@pytest.mark.skip()
async def test_client(redis_client):
    stream_name = 'stream'
    end_event_key = 'end_event_key'
    sub_num = 1000
    event_num = 100
    pub_num = 100

    async def subscribe(i):
        cursor = '$'
        while True:
            res = await redis_client.xread(
                streams={
                    stream_name: cursor,
                },
                block=0,
                count=1,
            )
            event = res[0][1][0][1]
            cursor = res[0][1][0][0]
            if i == 0:
                print(res)
            if end_event_key.encode() in event:
                break

    async def sub_many():
        await asyncio.gather(*(
            subscribe(i)
            for i in range(sub_num)
        ))

    async def publish():
        for i in range(event_num):
            await asyncio.gather(*(
                redis_client.xadd(
                    name=stream_name,
                    fields={'i': i, 'j': j},
                )
                for j in range(pub_num)
            ))

            await asyncio.sleep(0.1)

        await redis_client.xadd(
            name=stream_name,
            fields={end_event_key: '1'},
        )

    sub = asyncio.create_task(sub_many())
    await asyncio.sleep(1)
    await publish()
    await sub
