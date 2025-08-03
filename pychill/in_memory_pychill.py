import asyncio
from collections import deque
from datetime import datetime
from logging import getLogger, Logger
from typing import Any
from pydantic import Field

from pychill.base import BasePyChillLimiter

logger = getLogger(__name__)

class InMemoryPyChillLimiter(BasePyChillLimiter):
    logger: Logger = Field(default=logger)
    in_queue: deque | None = Field(default=None)
    processing_deque: deque | None = Field(default=None)

    def model_post_init(self, context: Any, /) -> None:
        self.in_queue = deque()
        self.processing_deque = deque()

    async def run(self, awaitable, ):
        event = asyncio.Event()
        self.in_queue.appendleft(event)
        self.logger.debug(f'schedule {awaitable=}')

        # in case, there are no predecessors
        if self.in_queue[-1] is event:
            self.logger.debug(f'the awaitable is the first item: {awaitable=}')
            event.set()

        await event.wait()
        self.logger.debug(f'event happened for {awaitable=}')

        # huge while loop to be sure that we do not overflow limit
        while True:
            now = datetime.now()
            # remove expired timestamps
            while (len(self.processing_deque) > 0) and ((now - self.processing_deque[0]) > self.window):
                self.processing_deque.popleft()

            # check if there is room for new requests
            if len(self.processing_deque) >= self.effective_rate:
                # if no room, sleep a little bit
                time_to_sleep = self.window - (now - self.processing_deque[0])
                self.logger.debug(f'sleep for {time_to_sleep.total_seconds()} seconds before run an {awaitable=}')
                await asyncio.sleep(time_to_sleep.total_seconds())
                continue
            break

        now = datetime.now()
        assert self.in_queue[-1] is event
        self.in_queue.pop()
        self.processing_deque.append(now)

        self.logger.debug(f"run {awaitable=}")
        try:
            return await awaitable
        finally:
            # call successors if present
            if len(self.in_queue):
                self.in_queue[-1].set()
