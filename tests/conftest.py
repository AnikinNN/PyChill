import logging.config
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import BaseModel

from tests.limited_server import LimitedServer
from tests.utils import RateLimit

logging.config.fileConfig(Path(__file__).parent / 'logging.ini')

pytest_plugins = ['tests.plugins.redis_plugins', 'tests.plugins.in_memory_plugins']

@pytest.fixture
def good_rl():
    return RateLimit(
        rate=10,
        window=timedelta(seconds=1),
    )

@pytest.fixture
def bad_rl():
    return RateLimit(
        rate=100,
        window=timedelta(seconds=1),
    )

@pytest.fixture(autouse=True)
def print_newline_after_start():
    print()


@pytest.fixture
def limited_server():
    return LimitedServer(10, timedelta(seconds=1))
