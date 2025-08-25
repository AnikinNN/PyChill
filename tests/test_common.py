import pytest

from tests.limited_server import ServerRateLimitError
from tests.utils import reraise_gather, with_timeout


@pytest.fixture
def unwrap_limiter(request):
    assert isinstance(request.param, str)
    return request.getfixturevalue(request.param)


many_limiters = pytest.mark.parametrize(
    'unwrap_limiter',
    [
        'in_memory_limiter',
        'redis_limiter',
    ],
    indirect=True,
)

many_bad_limiters = pytest.mark.parametrize(
    'unwrap_limiter',
    [
        'bad_in_memory_limiter',
        'bad_redis_limiter',
    ],
    indirect=True,
)


@many_limiters
@with_timeout(timeout=15)
async def test_normal(limited_server, unwrap_limiter):
    @unwrap_limiter.decorator
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


@many_bad_limiters
@with_timeout(timeout=15)
async def test_brake_the_limit(limited_server, unwrap_limiter):
    @unwrap_limiter.decorator
    async def should_be_limited(*args, **kwargs):
        return await limited_server.run(*args, **kwargs)

    with pytest.raises(ExceptionGroup) as e:
        await reraise_gather(*(
            should_be_limited(i)
            for i in range(30)
        ))

    assert all(map(lambda x: isinstance(x, ServerRateLimitError), e.value.exceptions))


@many_limiters
@with_timeout(timeout=15)
async def test_gather(limited_server, unwrap_limiter):
    async def should_be_limited(*args, **kwargs):
        return await limited_server.run(*args, **kwargs)

    res = []
    for _ in range(2):
        r = await unwrap_limiter.gather(*(
            should_be_limited(i)
            for i in range(30)
        ))
        res.extend(r)

    assert all(map(lambda x: isinstance(x, str), res))


@many_bad_limiters
@with_timeout(timeout=15)
async def test_gather_brake_the_limit(limited_server, unwrap_limiter):
    async def should_be_limited(*args, **kwargs):
        return await limited_server.run(*args, **kwargs)

    with pytest.raises(ServerRateLimitError):
        await unwrap_limiter.gather(*(
            should_be_limited(i)
            for i in range(30)
        ))


@many_bad_limiters
@with_timeout(timeout=15)
async def test_gather_brake_the_limit_return_exceptions(limited_server, unwrap_limiter):
    async def should_be_limited(*args, **kwargs):
        return await limited_server.run(*args, **kwargs)

    res = []
    for _ in range(2):
        r = await unwrap_limiter.gather(
            *(
                should_be_limited(i)
                for i in range(30)
            ),
            return_exceptions=True,
        )
        res.extend(r)

    errors = list(filter(lambda x: isinstance(x, Exception), res))
    normal_results = list(filter(lambda x: not isinstance(x, Exception), res))

    assert all(map(lambda x: isinstance(x, ServerRateLimitError), errors))
    assert all(map(lambda x: isinstance(x, str), normal_results))


@many_limiters
@with_timeout(timeout=15)
async def test_one_item(limited_server, unwrap_limiter):
    @unwrap_limiter.decorator
    async def should_be_limited(*args, **kwargs):
        return await limited_server.run(*args, **kwargs)

    res = await should_be_limited(1)
    assert isinstance(res, str)
