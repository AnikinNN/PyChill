import logging.config
from pathlib import Path

import pytest

logging.config.fileConfig(Path(__file__).parent / 'logging.ini')


@pytest.fixture(autouse=True)
def print_newline_after_start():
    print()
