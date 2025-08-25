import pytest

from pychill import InMemoryPyChillLimiter


@pytest.fixture
def in_memory_limiter(good_rl):
    return InMemoryPyChillLimiter(rate=good_rl.rate, window=good_rl.window)


@pytest.fixture
def bad_in_memory_limiter(bad_rl):
    return InMemoryPyChillLimiter(rate=bad_rl.rate, window=bad_rl.window)
